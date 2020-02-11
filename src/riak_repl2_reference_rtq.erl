-module(riak_repl2_reference_rtq).
-behaviour(gen_server).

%% API
-export(
[
    start_link/1,
    push/2,
    register/3
]).

%% gen_server callbacks
-export(
[
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state,
{
    name = undefined,
    consumers = orddict:new(),
    consumer_queue = queue:new(),
    reference_tab = ets:new(?MODULE, [protected, ordered_set]),
    sent_tab = ets:new(?MODULE, [protected, ordered_set]),
    qtab = undefined,
    current_seq = 0,
    last_seq = 0

}).

% Consumers
-record(consumer,
{
    pid = undefined, %% rtsource_helper pid to push objects
    aseq = 0,
    cseq = 0,
    batch_sent = 0,
    batch_acked = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

push(Pid, Seq) ->
    gen_server:cast(Pid, {push, Seq}).

register(Pid, HelperPid, Ref)->
    gen_server:call(Pid, {register, HelperPid, Ref}, infinity).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name]) ->
    %% register to riak_repl2_rtq
    QTab = riak_repl2_rtq:register(Name),
    {ok, #state{name = Name, qtab = QTab}}.

handle_call({register, Ref}, Pid, State = #state{consumers = Consumers, consumer_queue = Queue}) ->
    case orddict:fetch(Ref, Consumers) of
        {ok, _} ->
            lager:error("Name: ~p, Ref: ~p, Pid: ~p. Already exisiting reference to consumer", [State#state.name, Ref, Pid]),
            {reply, {error, ref_exists}, State};
        error ->
            erlang:monitor(process, Pid),
            C = #consumer{pid = Pid},
            NewConsumers = orddict:store(Ref, C, Consumers),
            NewQueue = queue:in(Ref, Queue),
            erlang:send(self(), maybe_send),
            {reply, ok, State#state{consumer_queue = NewConsumers, consumer_queue = NewQueue}}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(maybe_send, State) ->
    {noreply, maybe_send(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_send(State) ->
    case maybe_get_object(State) of
        {ok, QEntry, NewState} ->
            maybe_deliver_object(QEntry, NewState);
        {error, State} ->
            State
    end.

maybe_get_object(State = #state{current_seq = CurrentSeq, last_seq = LastSeq}) ->
    ObjectAvailable = CurrentSeq >= LastSeq,
    maybe_get_object(ObjectAvailable, State).

maybe_get_object(false, State) ->
    {error, State};
maybe_get_object(true, #state{reference_tab = RefTab, qtab = QTab, current_seq = CurrentSeq} = State) ->
    case ets:lookup(CurrentSeq, RefTab) of
        [] ->
            %% entry removed from reference table? This would not happen
            lager:error("something removed reference queue entry!"),
            maybe_get_object(State#state{current_seq = CurrentSeq+1});
        [{CurrentSeq, QSeq}] ->
            case ets:lookup(QSeq, QTab) of
                [] ->
                    %% entry removed, this can happen if the queue was trimmed!
                    maybe_get_object(State#state{current_seq = CurrentSeq+1});
                [QEntry] ->
                    {ok, QEntry, State}
            end
    end.

maybe_deliver_object(QEntry, State = #state{consumer_queue = ConsumerQ}) ->
    case queue:out(ConsumerQ) of
        {empty, NewQ} ->
            State#state{consumer_queue = NewQ};
        {{value, ConsumerRef}, NewQ} ->
            maybe_deliver_object(ConsumerRef, QEntry, State#state{consumer_queue = NewQ})
    end.

maybe_deliver_object(ConsumerRef, QEntry, #state{consumers = Consumers} = State) ->
    case orddict:fetch(ConsumerRef, Consumers) of
        error ->
            %% consumer must have died and was removed from consumers
            %% however its ref was still in the queue!
            maybe_deliver_object(QEntry, State);
        {ok, Consumer} ->
            deliver_object(ConsumerRef, Consumer, QEntry, State)
    end.

deliver_object(ConsumerRef, Consumer, QEntry, State) ->
    #state
    {
        reference_tab = RefTab, sent_tab = SentTab,
        current_seq = CurrentSeq, consumers = Consumers,
        consumer_queue = ConsumerQ
    } = State,
    #consumer
    {
        pid = Pid, cseq = CSeq, batch = Batch,
        batch_sent = BatchSent, batch_acked = BatchAcked
    } = Consumer,

    {QSeq, NumItems, Bin, Meta, _Completed} = QEntry,
    Entry = {CSeq, NumItems, Bin, Meta},

    OkError =
        try
            %% send to rtsource helper
            ok = riak_repl2_rtsource_helper:send_object(Pid, Entry),
            ok
        catch Type:Error ->
            {error, Type, Error}
        end,

    case OkError of
        ok ->
            %% insert into sent table
            ets:insert(SentTab, {{ConsumerRef, CSeq}, QSeq}),

            %% delete from reference table
            ets:delete(RefTab, CurrentSeq),

            %% update consumer
            UpdatedConsumer = Consumer#consumer{cseq = CSeq +1, batch_sent = BatchSent +1},

            %% update consumers orddict
            UpdatedConsumers = orddict:store(ConsumerRef, UpdatedConsumer, Consumers),

            %% update consumer queue
            UpdatedConsumerQ =
                case UpdatedConsumer#consumer.batch_sent == Batch of
                    true ->
                        ConsumerQ;
                    false ->
                        queue:in(ConsumerRef, ConsumerQ)
                end,

            %% update state
            State#state
            {
                current_seq = CurrentSeq +1, consumers = UpdatedConsumers, consumer_queue = UpdatedConsumerQ
            };
        {error, Type, Error} ->
            lager:warning("Failed to send object to rtsource helper pid: ~p", [Pid]),
            lager:error("Reference Queue Error: Type: ~p, Error: ~p", [Type, Error]),
            maybe_deliver_object(QEntry, State)
    end.


-module(riak_repl2_reference_rtq).
-behaviour(gen_server).

%% API
-export(
[
    start_link/1,
    push/2,
    ack/3,
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
    qtab = undefined,
    consumers = orddict:new(),
    consumer_queue = queue:new(),
    reference_tab = ets:new(?MODULE, [protected, ordered_set]),
    rseq = 0, %% the last sequence number in the reference queue
    seq = 0, %% the next sequence number to send
    total_sent = 0,
    total_acked = 0
}).

% Consumers
-record(consumer,
{
    pid = undefined, %% rtsource_helper pid to push objects
    seq = undefined, %% if undefined this means that it has nothing in flight
    qseq = undefined  %% if undefined this means that it has nothing in flight
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

push(Pid, QEntry) ->
    gen_server:cast(Pid, {push, QEntry}).

ack(Pid, Ref, Seq) ->
    gen_server:cast(Pid, {ack, Ref, Seq}).

register(Pid, HelperPid, Ref)->
    gen_server:call(Pid, {register, HelperPid, Ref}, infinity).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name]) ->
    %% register to riak_repl2_rtq
    {Seq, QSeq, QTab} = riak_repl2_rtq:register(Name),

    State = #state{name = Name, qtab = QTab},
    %% decide if we need to populate
    NewState = populate_reference_table(Seq, QSeq, State),

    {ok, NewState}.

populate_reference_table(Seq, Seq, State) ->
    State;
populate_reference_table('$end_of_table', _Seq, State) ->
    State;
populate_reference_table(Seq, QSeq, State = #state{reference_tab = RefTab, qtab = QTab, rseq = RSeq}) ->
    case ets:lookup(QTab, Seq) of
        [] ->
            %% been trimmed forget it
            populate_reference_table(Seq+1, QSeq, State);
        [{Seq, _, _, _, Completed}] ->
            case lists:member(Name, Compelted) of
                true ->
                    %% object is not for us
                    populate_reference_table(Seq+1, QSeq, State);
                false ->
                    %% object is for us
                    ets:insert(RefTab, {})
                    populate_reference_table(Seq+1, QSeq, blag)
            end
    end.

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
            erlang:send(self(), maybe_pull),
            {reply, ok, State#state{consumer_queue = NewConsumers, consumer_queue = NewQueue}}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({push, QEntry}, State) ->
    {noreply, do_push(QEntry, State)};

handle_cast({ack, Ref, Seq}, State) ->
    {noreply, ack_seq(Ref, Seq, State)};

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(maybe_pull, State) ->
    {noreply, maybe_pull(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

ack_seq(Ref, Seq, State = #state{consumers = Consumers, name = Name, reference_tab = RefTab}) ->
    case orddict:fetch(Ref, Consumers) of
        {ok, #consumer{seq = Seq, qseq = QSeq} = Consumer} ->
            %% Should this be a call?
            riak_repl2_rtq:ack(Name, Seq),

            %% remove from reference table
            ets:delete(RefTab, QSeq),

            %% update consumer
            UpdatedConsumer = Consumer#consumer{seq = undefined, qseq = undefined},

            %% store in state and return
            State#state{consumers = orddict:store(Ref, UpdatedConsumer, Consumers)};
        _ ->
            %% this consumer is suppsoed to be dead, or sequence number did not match!
            State
    end.


do_push(QEntry, State = #state{seq = Seq, rseq = RSeq, reference_tab = RefTab}) ->
    RSeq2 = RSeq +1,
    {QSeq, _NumItem, _Bin, _Meta, _Completed} = QEntry,

    %% insert into reference table
    ets:insert(RefTab, {RSeq2, QSeq}),

    %% update reference sequence number
    NewState = State#state{rseq = RSeq2},

    %% maybe_deliver_object, only if we are at the head of the queue!
    case RSeq == Seq of
        true ->
            maybe_deliver_object(RSeq2, QEntry, NewState);
        false ->
            NewState
    end.


maybe_pull(State) ->
    case maybe_get_object(State) of
        {ok, Seq2, QEntry, NewState} ->
            maybe_deliver_object(Seq2, QEntry, NewState);
        {error, State} ->
            State
    end.

maybe_get_object(State = #state{seq = Seq, rseq = RSeq}) ->
    Seq2 = Seq +1,
    case Seq2 =< RSeq of
        true ->
            maybe_get_object(Seq2, State);
        false ->
            {error, State}
    end.

maybe_get_object(Seq2, #state{reference_tab = RefTab, qtab = QTab} = State) ->
    case ets:lookup(Seq2, RefTab) of
        [] ->
            %% entry removed from reference table? This would not happen
            lager:error("something removed reference queue entry!"),
            maybe_get_object(State#state{seq = Seq2});
        [{Seq2, QSeq}] ->
            case ets:lookup(QSeq, QTab) of
                [] ->
                    %% entry removed, this can happen if the queue was trimmed!
                    maybe_get_object(State#state{seq = Seq2});
                [QEntry] ->
                    {ok, Seq2, QEntry, State}
            end
    end.

maybe_deliver_object(Seq2, QEntry, State = #state{consumer_queue = ConsumerQ}) ->
    case queue:out(ConsumerQ) of
        {empty, NewQ} ->
            State#state{consumer_queue = NewQ};
        {{value, ConsumerRef}, NewQ} ->
            maybe_deliver_object(ConsumerRef, Seq2, QEntry, State#state{consumer_queue = NewQ})
    end.

maybe_deliver_object(ConsumerRef, Seq2, QEntry, #state{consumers = Consumers} = State) ->
    case orddict:fetch(ConsumerRef, Consumers) of
        error ->
            %% consumer must have died and was removed from consumers
            %% however its ref was still in the queue!
            maybe_deliver_object(Seq2, QEntry, State);
        {ok, Consumer} ->
            deliver_object(ConsumerRef, Consumer, Seq2, QEntry, State)
    end.

deliver_object(ConsumerRef, Consumer, Seq2, QEntry, State) ->
    #state{seq = Seq, consumers = Consumers} = State,
    #consumer{pid = Pid} = Consumer,
    {QSeq, NumItems, Bin, Meta, _Completed} = QEntry,
    Entry = {Seq2, NumItems, Bin, Meta},

    OkError =
        try
            Seq2 = Seq +1, %% sanity check
            %% send to rtsource helper
            ok = riak_repl2_rtsource_helper:send_object(Pid, Entry),
            ok
        catch Type:Error ->
            {error, Type, Error}
        end,

    case OkError of
        ok ->

            %% update consumer
            UpdatedConsumer = Consumer#consumer{qseq = QSeq, seq = Seq},

            %% update consumers orddict
            UpdatedConsumers = orddict:store(ConsumerRef, UpdatedConsumer, Consumers),

            %% update state
            State#state{seq = Seq2, consumers = UpdatedConsumers};
        {error, Type, Error} ->
            lager:warning("Failed to send object to rtsource helper pid: ~p", [Pid]),
            lager:error("Reference Queue Error: Type: ~p, Error: ~p", [Type, Error]),
            maybe_deliver_object(QEntry, State)
    end.


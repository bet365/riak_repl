-module(riak_repl2_reference_rtq).
-behaviour(gen_server).

%% API
-export(
[
    start_link/1,
    name/1,
    push/2,
    ack/3,
    register/2,
    status/1
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

-define(SERVER(RemoteName), name(RemoteName)).

-record(state,
{
    name = undefined,
    qtab = undefined,
    consumers = orddict:new(),
    consumer_queue = queue:new(),
    reference_tab = ets:new(?MODULE, [protected, ordered_set]),
    rseq = 0, %% the last sequence number in the reference queue
    seq = 0, %% the next sequence number to send
    ack_counter = 0 %% number of objects acked by sink
}).

% Consumers
-record(consumer,
{
    pid = undefined, %% rtsource_helper pid to push objects
    seq = undefined, %% sequence number for reference queue
    cseq = undefined, %% sequence number for consumer
    qseq = undefined  %% sequence number for realtime queue
}).

%%%===================================================================
%%% API
%%%===================================================================

name(RemoteName) ->
    IdBin = list_to_binary(RemoteName),
    binary_to_atom(<<"riak_repl2_reference_rtq_", IdBin/binary>>, latin1).

start_link(RemoteName) ->
    gen_server:start_link({local, name(RemoteName)}, ?MODULE, [RemoteName], []).

%% should we try catch this so we don't kill the rtq?
push(RemoteName, QEntry) ->
    gen_server:cast(?SERVER(RemoteName), {push, QEntry}).

ack(RemoteName, Ref, CSeq) ->
    gen_server:cast(?SERVER(RemoteName), {ack, Ref, CSeq}).

register(RemoteName, Ref)->
    gen_server:call(?SERVER(RemoteName), {register, Ref}, infinity).

status(RemoteName) ->
    try
        gen_server:call(?SERVER(RemoteName), status)

    catch
        _:_  ->
            []
    end.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([RemoteName]) ->
    gen_server:cast(?SERVER(RemoteName), initialise),
    {ok, #state{name = RemoteName}}.

handle_call({register, Ref}, Pid, State = #state{name = Name, consumers = Consumers, consumer_queue = Queue}) ->
    case orddict:fetch(Ref, Consumers) of
        {ok, _} ->
            lager:error("Name: ~p, Ref: ~p, Pid: ~p. Already exisiting reference to consumer", [State#state.name, Ref, Pid]),
            {reply, {error, ref_exists}, State};
        error ->
            erlang:monitor(process, Pid),
            C = #consumer{pid = Pid},
            NewConsumers = orddict:store(Ref, C, Consumers),
            NewQueue = queue:in(Ref, Queue),
            gen_server:cast(?SERVER(Name), maybe_pull),
            {reply, ok, State#state{consumers = NewConsumers, consumer_queue = NewQueue}}
    end;

handle_call(status, _From, State) ->
    #state{ack_counter = Acked, rseq = RSeq, seq = Seq} = State,
    Stats = [{pending, RSeq - Seq}, {unacked, Seq - Acked}, {acked, Acked}],
    {reply, Stats, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(maybe_pull, State) ->
    {noreply, maybe_pull(State)};

handle_cast(initialise, State = #state{name = Name}) ->
    {QSeq, QTab} = riak_repl2_rtq:register(Name),
    NewState = State#state{qtab = QTab},
    {noreply, populate_reference_table(ets:first(QTab), QSeq, NewState)};

handle_cast({push, QEntry}, State) ->
    {noreply, do_push(QEntry, State)};

handle_cast({ack, Ref, CSeq}, State) ->
    {noreply, ack_seq(Ref, CSeq, State)};

handle_cast(_Request, State) ->
    {noreply, State}.

%% TODO: 'DOWN' messages!
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{reference_tab = RefTab}) ->
    lager:info("Reference Queue shutting down; Reason: ~p", [Reason]),
    ets:delete(RefTab),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

populate_reference_table(Seq, Seq, State) ->
    State;
populate_reference_table('$end_of_table', _Seq, State) ->
    State;
populate_reference_table(Seq, QSeq, State = #state{reference_tab = RefTab, qtab = QTab, rseq = RSeq, name = Name}) ->
    RSeq2 = RSeq +1,
    case ets:lookup(QTab, Seq) of
        [] ->
            %% been trimmed forget it
            populate_reference_table(Seq+1, QSeq, State);
        [{Seq, _, _, _, Completed}] ->
            case lists:member(Name, Completed) of
                true ->
                    %% object is not for us
                    populate_reference_table(Seq+1, QSeq, State);
                false ->
                    %% object is for us
                    ets:insert(RefTab, {RSeq2, Seq}),
                    populate_reference_table(Seq+1, QSeq, State#state{rseq = RSeq2})
            end
    end.

ack_seq(Ref, CSeq, State = #state{consumers = Consumers, name = Name, reference_tab = RefTab, ack_counter = AC}) ->
    case orddict:fetch(Ref, Consumers) of
        {ok, #consumer{seq = Seq, cseq = CSeq, qseq = QSeq} = Consumer} ->
            %% Should this be a call?
            riak_repl2_rtq:ack(Name, QSeq),

            %% remove from reference table
            ets:delete(RefTab, Seq),

            %% update consumer
            UpdatedConsumer = Consumer#consumer{seq = undefined, qseq = undefined},

            %% store in state and return
            State#state{consumers = orddict:store(Ref, UpdatedConsumer, Consumers), ack_counter = AC +1};

        {ok, #consumer{}} ->
            lager:error("Sequence number from consumer did not match that stored in reference queue"),
            %%TODO kill consumer? (maybe)
            State;
        _ ->
            %% this consumer is supposed to be dead, or sequence number did not match!
            %% what should we do? kill the process? (A retry to another process has already been sent!)
            lager:error("reference queue got ack from helper: Ref, there is a lingering process", [Ref]),
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
            maybe_deliver_object(ConsumerRef, NewQ, Seq2, QEntry, State)
    end.

maybe_deliver_object(ConsumerRef, NewQ, Seq2, QEntry, #state{consumers = Consumers} = State) ->
    case orddict:fetch(ConsumerRef, Consumers) of
        error ->
            %% consumer must have died and was removed from consumers
            %% however its ref was still in the queue!
            maybe_deliver_object(Seq2, QEntry, State#state{consumer_queue = NewQ});
        {ok, Consumer} ->
            deliver_object(ConsumerRef, NewQ, Consumer, Seq2, QEntry, State)
    end.

deliver_object(ConsumerRef, NewQ, Consumer, Seq2, QEntry, State) ->
    #state{seq = Seq, consumers = Consumers, reference_tab = RefTab, name = Name} = State,
    #consumer{pid = Pid} = Consumer,
    {QSeq, NumItems, Bin, Meta, _Completed} = QEntry,
    Entry = {Seq2, NumItems, Bin, Meta},

    OkError =
        try
            Seq2 = Seq +1, %% sanity check
            %% send to rtsource helper
            case riak_repl2_rtsource_helper:send_object(Pid, Entry) of
                bucket_type_not_supported_by_remote ->
                    bucket_type_not_supported_by_remote;
                {ok, CSeq0} ->
                    {ok, CSeq0}
            end
        catch T:E ->
            {error, T, E}
        end,

    case OkError of
        {ok, CSeq} ->

            %% update consumer
            UpdatedConsumer = Consumer#consumer{qseq = QSeq, seq = Seq2, cseq = CSeq},

            %% update consumers orddict
            UpdatedConsumers = orddict:store(ConsumerRef, UpdatedConsumer, Consumers),

            %% update state
            State#state{seq = Seq2, consumers = UpdatedConsumers, consumer_queue = NewQ};
        bucket_type_not_supported_by_remote ->
            %%TODO: do we want to increase a dropped counter somewhere?
            %% we want to delete this from the reference table, and ack the realtime queue
            %% keep the consumer in the queue
            %% maybe_pull?

            %% delete from reference table
            ets:delete(RefTab, Seq2),

            %% ack realtime queue
            riak_repl2_rtq:ack(Name, QSeq),

            %% maybe pull (to continue if there is anything to pull)
            maybe_pull(State#state{seq = Seq2});
        {error, Type, Error} ->
            %%TODO: do we kill the consumer
            %%TODO: do we remove the consumer from the consumer list?
            lager:warning("Failed to send object to rtsource helper pid: ~p", [Pid]),
            lager:error("Reference Queue Error: Type: ~p, Error: ~p", [Type, Error]),
            maybe_deliver_object(Seq2, QEntry, State#state{consumer_queue = NewQ})
    end.


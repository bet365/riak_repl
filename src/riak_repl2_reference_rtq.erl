-module(riak_repl2_reference_rtq).
-behaviour(gen_server).
-include("riak_repl.hrl").

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
-define(DEFAULT_RETRY_LIMIT, 3).

-record(state,
{
    name = undefined,
    status = active, %% can be active | retry (this tells us which table to get info from)
    qtab = undefined,
    reference_queue = #queue{},
    retry_queue = #queue{},
    consumers = orddict:new(),
    consumer_monitors = orddict:new(),
    consumer_fifo = queue:new(),
    drops = 0,
    ack_counter = 0 %% number of objects acked by sink
}).

%% Queues
-record(queue,
{
    table = ets:new(?MODULE, [protected, ordered_set]),
    start_seq = 0, %% the next sequence number in queue
    end_seq = 0    %% the last sequence number in the queue

}).

%% Consumers
-record(consumer,
{
    status = active,   %% can be active | inactive | retry (this tells us which table to look at)
    retry_counter = 0, %% based on this and config we may drop an object after N number of retries
    pid = undefined,   %% rtsource_helper pid to push objects
    seq = undefined,   %% sequence number for reference queue
    qseq = undefined,  %% sequence number for realtime queue
    cseq = undefined   %% sequence number for given by consumer
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

handle_call({register, Ref}, Pid, State) ->
    #state{name = Name, consumers = Consumers, consumer_fifo = ConsumerFIFO, consumer_monitors = ConsumerMonitors} = State,
    case orddict:fetch(Ref, Consumers) of
        {ok, _} ->
            lager:error("Name: ~p, Ref: ~p, Pid: ~p. Already exisiting reference to consumer", [State#state.name, Ref, Pid]),
            {reply, {error, ref_exists}, State};
        error ->
            MonitorRef = erlang:monitor(process, Pid),
            C = #consumer{pid = Pid},
            NewConsumers = orddict:store(Ref, C, Consumers),
            NewConsumerMonitors = orddict:store(MonitorRef, Ref, ConsumerMonitors),
            NewConsumerFIFO = queue:in(Ref, ConsumerFIFO),
            NewState = State#state
                {
                    consumers = NewConsumers,
                    consumer_fifo = NewConsumerFIFO,
                    consumer_monitors = NewConsumerMonitors
                },
            gen_server:cast(?SERVER(Name), maybe_pull),
            {reply, ok, NewState}
    end;

handle_call(status, _From, State) ->
    #state{ack_counter = Acked, reference_queue = RefQ, retry_queue = RetryQ} = State,
    #queue{start_seq = RefStart, end_seq = RefEnd} = RefQ,
    #queue{start_seq = RetryStart, end_seq = RetryEnd} = RetryQ,
    Pending = (RefEnd - RefStart) + (RetryEnd - RetryStart),
    Unacked = RefStart - Acked,
    Stats = [{pending, Pending}, {unacked, Unacked}, {acked, Acked}],
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


handle_info({'DOWN', MonitorRef, process, Pid, _Reason}, State) ->
    {noreply, maybe_retry(MonitorRef, Pid, State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{reference_queue = RefQ, retry_queue = RetryQ}) ->
    lager:info("Reference Queue shutting down; Reason: ~p", [Reason]),
    #queue{table = RefTab} = RefQ,
    #queue{table = RetryTab} = RetryQ,
    ets:delete(RefTab),
    ets:delete(RetryTab),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_retry(MonitorRef, Pid, State) ->
    #state
    {
        consumer_monitors = ConsumerMonitors, consumers = Consumers,
        reference_queue = RefQ, retry_queue = RetryQ,
        name = Name, status = Status, drops = Drops
    } = State,
    case orddict:fetch(MonitorRef, ConsumerMonitors) of
        {ok, Ref} ->
            case orddict:fetch(Ref, Consumers) of
                {ok, Consumer = #consumer{pid = Pid}} ->
                    #consumer{seq = Seq, qseq = QSeq, retry_counter = RetryCounter} = Consumer,
                    NewRetryCounter = RetryCounter +1,
                    NewConsumers = orddict:erase(Ref, Consumers),
                    NewConsumerMonitors = orddict:erase(MonitorRef, ConsumerMonitors),
                    delete_object_from_queue(Seq, RefQ, RetryQ, Status),
                    case  NewRetryCounter > get_retry_limit(State) of
                        true ->
                            NewState =
                                State#state
                                {
                                    consumers = NewConsumers,
                                    consumer_monitors = NewConsumerMonitors,
                                    drops = Drops +1
                                },
                            NewState;
                        false ->
                            NewRetryQ = insert_object_to_queue({QSeq, NewRetryCounter}, RetryQ),
                            NewState =
                                State#state
                                {
                                    status = retry,
                                    retry_queue = NewRetryQ,
                                    consumers = NewConsumers,
                                    consumer_monitors = NewConsumerMonitors
                                },
                            gen_server:cast(?SERVER(Name), maybe_pull),
                            NewState
                    end;
                error ->
                    %% we got a down message with a monitor reference but the consumer does not exist
                    lager:error("'DOWN' message recieved with a monitor ref that matches no consumer"),
                    State
            end;
        error ->
            %% we got a down message with a monitor reference that we are not aware of
            lager:error("'DOWN' message recieved with a monitor ref we are not aware of"),
            State
    end.


insert_object_to_queue(QSeq, Queue) ->
    #queue{end_seq = EndSeq, table = Tab} = Queue,
    EndSeq2 = EndSeq +1,
    ets:insert(Tab, {EndSeq2, QSeq}),
    Queue#queue{end_seq = EndSeq2}.

delete_object_from_queue(Seq, RefQ, _RetryQ, active) ->
    delete_object_from_queue(Seq, RefQ);
delete_object_from_queue(Seq, _RefQ, RetryQ, retry) ->
    delete_object_from_queue(Seq, RetryQ).

delete_object_from_queue(Seq, Queue) ->
    #queue{table = Tab} = Queue,
    ets:delete(Tab, Seq).

populate_reference_table(Seq, Seq, State) ->
    State;
populate_reference_table('$end_of_table', _Seq, State) ->
    State;
populate_reference_table(Seq, QSeq, State = #state{qtab = QTab, name = Name, reference_queue = RefQ}) ->
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
                    NewRefQ = insert_object_to_queue(Seq, RefQ),
                    populate_reference_table(Seq+1, QSeq, State#state{reference_queue = NewRefQ})
            end
    end.


ack_seq(Ref, CSeq, State) ->
    #state
    {
        consumers = Consumers, name = Name, ack_counter = AC,
        reference_queue = RefQ, retry_queue = RetryQ
    } = State,
    case orddict:fetch(Ref, Consumers) of
        {ok, #consumer{status = Status, seq = Seq, qseq = QSeq, pid = Pid} = C} when C#consumer.cseq == CSeq ->
            riak_repl2_rtq:ack(Name, QSeq),
            delete_object_from_queue(Seq, RefQ, RetryQ, Status),
            UpdatedConsumer = #consumer{pid = Pid},
            State#state{consumers = orddict:store(Ref, UpdatedConsumer, Consumers), ack_counter = AC +1};

        {ok, #consumer{pid = Pid}} ->
            lager:error("Sequence number from consumer did not match that stored sequence number in reference queue"),
            exit(Pid, shutdown),
            State;
        error ->
            %% this consumer is supposed to be dead, or sequence number did not match!
            %% what should we do? kill the process? (A retry to another process has already been sent!)
            lager:error("reference queue got ack from helper: Ref, there is a lingering process", [Ref]),
            State
    end.


do_push(QEntry, State = #state{status = Status, reference_queue = RefQ}) ->
    {QSeq, _NumItem, _Bin, _Meta, _Completed} = QEntry,
    NewRefQ = insert_object_to_queue(QSeq, RefQ),
    NewState = State#state{reference_queue = RefQ},
    #queue{start_seq = RefStartSeq, end_seq = RefEndSeq} = NewRefQ,
    ShouldSend = (RefStartSeq +1) == RefEndSeq,
    RetryCounter = 0,

    %% maybe_deliver_object, only if we are at the head of the queue!
    case {Status, ShouldSend} of
        {active, true} ->
            maybe_deliver_object(RefEndSeq, RetryCounter, QEntry, NewState);
        {active, false} ->
            NewState;
        {retry, _} ->
            maybe_pull(NewState)
    end.


maybe_pull(State) ->
    case maybe_get_object(State) of
        {ok, Seq2, RetryCounter, QEntry, NewState} ->
            maybe_deliver_object(Seq2, RetryCounter, QEntry, NewState);
        {error, State} ->
            State
    end.

maybe_get_object(State = #state{status = active, reference_queue = RefQ}) ->
    #queue{start_seq = StartSeq, end_seq = EndSeq} = RefQ,
    StartSeq2 = StartSeq +1,
    case StartSeq2 =< EndSeq of
        true ->
            maybe_get_object(StartSeq2, State);
        false ->
            {error, State}
    end;
maybe_get_object(State = #state{status = retry, retry_queue = RetryQ}) ->
    #queue{start_seq = StartSeq, end_seq = EndSeq} = RetryQ,
    StartSeq2 = StartSeq +1,
    case StartSeq2 =< EndSeq of
        true ->
            maybe_get_object(StartSeq2, State);
        false ->
            maybe_get_object(State#state{status = active})
    end.

maybe_get_object(StartSeq2, #state{status = active, qtab = QTab, reference_queue = RefQ} = State) ->
    #queue{table = RefTab} = RefQ,
    case ets:lookup(StartSeq2, RefTab) of
        [] ->
            %% entry removed from reference table? This would not happen
            lager:error("something removed reference queue entry!"),
            maybe_get_object(State#state{reference_queue = RefQ#queue{start_seq = StartSeq2}});
        [{StartSeq2, QSeq}] ->
            case ets:lookup(QSeq, QTab) of
                [] ->
                    %% entry removed, this can happen if the queue was trimmed!
                    maybe_get_object(State#state{reference_queue = RefQ#queue{start_seq = StartSeq2}});
                [QEntry] ->
                    RetryCounter = 0,
                    {ok, StartSeq2, RetryCounter, QEntry, State}
            end
    end;
maybe_get_object(StartSeq2, #state{status = retry, qtab = QTab, retry_queue = RetryQ} = State) ->
    #queue{table = RetryTab} = RetryQ,
    case ets:lookup(StartSeq2, RetryTab) of
        [] ->
            %% entry removed from reference table? This would not happen
            lager:error("something removed retry queue entry!"),
            maybe_get_object(State#state{retry_queue = RetryQ#queue{start_seq = StartSeq2}});
        [{StartSeq2, {QSeq, RetryCounter}}] ->
            case ets:lookup(QSeq, QTab) of
                [] ->
                    %% entry removed, this can happen if the queue was trimmed!
                    maybe_get_object(State#state{retry_queue = RetryQ#queue{start_seq = StartSeq2}});
                [QEntry] ->
                    {ok, StartSeq2, RetryCounter, QEntry, State}
            end
    end.

maybe_deliver_object(Seq2, RetryCounter, QEntry, State = #state{consumer_fifo = ConsumerFIFO}) ->
    case queue:out(ConsumerFIFO) of
        {empty, NewFIFO} ->
            State#state{consumer_fifo = NewFIFO};
        {{value, ConsumerRef}, NewQ} ->
            maybe_deliver_object(ConsumerRef, NewQ, Seq2, RetryCounter, QEntry, State)
    end.

maybe_deliver_object(ConsumerRef, NewFIFO, Seq2, RetryCounter, QEntry, #state{consumers = Consumers} = State) ->
    case orddict:fetch(ConsumerRef, Consumers) of
        error ->
            %% consumer must have died and was removed from consumers
            %% however its ref was still in the queue!
            maybe_deliver_object(Seq2, RetryCounter, QEntry, State#state{consumer_fifo = NewFIFO});
        {ok, Consumer} ->
            deliver_object(ConsumerRef, NewFIFO, Consumer, Seq2, RetryCounter, QEntry, State)
    end.

deliver_object(ConsumerRef, NewFIFO, Consumer, Seq2, RetryCounter, QEntry, State = #state{status = active, drops = Drops}) ->
    #state{consumers = Consumers, reference_queue = RefQ, name = Name} = State,
    #queue{start_seq = Seq} = RefQ,
    #consumer{pid = Pid} = Consumer,
    {QSeq, NumItems, Bin, Meta, _Completed} = QEntry,
    Entry = {Seq2, NumItems, Bin, Meta},
    OkError = send_object(Seq2, Seq, Pid, Entry),

    case OkError of
        {ok, CSeq} ->
            UpdatedConsumer = Consumer#consumer{qseq = QSeq, seq = Seq2, cseq = CSeq, retry_counter = RetryCounter},
            UpdatedConsumers = orddict:store(ConsumerRef, UpdatedConsumer, Consumers),
            NewRefQ = RefQ#queue{start_seq = Seq2},
            State#state{consumers = UpdatedConsumers, consumer_fifo = NewFIFO, reference_queue = NewRefQ};

        bucket_type_not_supported_by_remote ->
            delete_object_from_queue(Seq2, RefQ),
            riak_repl2_rtq:ack(Name, QSeq),
            NewRefQ = RefQ#queue{start_seq = Seq2},
            maybe_pull(State#state{reference_queue = NewRefQ, drops = Drops +1});

        {error, Type, Error} ->
            lager:warning("Failed to send object to rtsource helper pid: ~p", [Pid]),
            lager:error("Reference Queue Error: Type: ~p, Error: ~p", [Type, Error]),
            exit(Pid, shutdown),
            maybe_deliver_object(Seq2, RetryCounter, QEntry, State#state{consumer_fifo = NewFIFO})
    end;

deliver_object(ConsumerRef, NewFIFO, Consumer, Seq2, RetryCounter, QEntry, State = #state{status = retry, drops = Drops}) ->
    #state{consumers = Consumers, retry_queue = RetryQ, name = Name} = State,
    #queue{start_seq = Seq, end_seq = EndSeq} = RetryQ,
    #consumer{pid = Pid} = Consumer,
    {QSeq, NumItems, Bin, Meta, _Completed} = QEntry,
    Entry = {Seq2, NumItems, Bin, Meta},
    OkError = send_object(Seq2, Seq, Pid, Entry),

    NewState = case Seq2 == EndSeq of
                   true ->
                       State#state{status = active};
                   false ->
                       State#state{status = retry}
               end,

    case OkError of
        {ok, CSeq} ->
            UpdatedConsumer = Consumer#consumer{qseq = QSeq, seq = Seq2, cseq = CSeq, retry_counter = RetryCounter},
            UpdatedConsumers = orddict:store(ConsumerRef, UpdatedConsumer, Consumers),
            NewRetryQ = RetryQ#queue{start_seq = Seq2},
            NewState#state{consumers = UpdatedConsumers, consumer_fifo = NewFIFO, retry_queue = NewRetryQ};

        bucket_type_not_supported_by_remote ->
            delete_object_from_queue(Seq2, RetryQ),
            riak_repl2_rtq:ack(Name, QSeq),
            NewRetryQ = RetryQ#queue{start_seq = Seq2},
            maybe_pull(NewState#state{retry_queue = NewRetryQ, drops = Drops +1});

        {error, Type, Error} ->
            lager:warning("Failed to send object to rtsource helper pid: ~p", [Pid]),
            lager:error("Reference Queue Error: Type: ~p, Error: ~p", [Type, Error]),
            exit(Pid, shutdown),
            maybe_deliver_object(Seq2, RetryCounter, QEntry, NewState#state{consumer_fifo = NewFIFO})
    end.

send_object(Seq2, Seq, Pid, Entry) ->
    try
        Seq2 = Seq +1, %% sanity check
        %% send to rtsource helper
        case riak_repl2_rtsource_helper:send_object(Pid, Entry) of
            bucket_type_not_supported_by_remote ->
                bucket_type_not_supported_by_remote;
            {ok, CSeq} ->
                {ok, CSeq}
        end
    catch T:E ->
        {error, T, E}
    end.


-ifdef(TEST).

get_retry_limit(_) ->
    app_helper:get_env(riak_repl, retry_limit, ?DEFAULT_RETRY_LIMIT).

-else.
get_retry_limit(#state{name = Name}) ->
    case riak_core_metadata:get(?RIAK_REPL2_RTQ_CONFIG_KEY, {retry_limit, Name}) of
        undefined -> get_retry_limit(Name);
        RetryLimit -> RetryLimit
    end.
-endif.
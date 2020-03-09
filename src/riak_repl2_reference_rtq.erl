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
    shutdown/1,
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
-define(DEFAULT_RETRY_LIMIT, unlimited).

%% Queues
-record(queue,
{
    table = ets:new(?MODULE, [protected, ordered_set]),
    start_seq = 0, %% the next sequence number in queue
    end_seq = 0    %% the last sequence number in the queue

}).

-record(state,
{
    name = undefined,
    shuting_down = false,
    shutting_down_ref = undefined,
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

%% Consumers
-record(consumer,
{
    status = active,   %% can be active | retry (this tells us which table to look at)
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

shutdown(RemoteName) ->
    gen_server:call(?SERVER(RemoteName), shutting_down, infinity).

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
    {QSeq, QTab} = riak_repl2_rtq:register(RemoteName),
    NewState = #state{name = RemoteName, qtab = QTab},
    {ok, populate_reference_table(ets:first(QTab), QSeq, NewState)}.

handle_call({register, Ref}, {Pid, _Tag}, State) ->
    #state{name = Name, consumers = Consumers, consumer_fifo = ConsumerFIFO, consumer_monitors = ConsumerMonitors} = State,
    case orddict:find(Ref, Consumers) of
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

handle_call(shutting_down, _From, State) ->
    {reply, ok, State#state{shuting_down = true}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(maybe_pull, State) ->
    {noreply, maybe_pull(State)};

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

maybe_retry(MonitorRef, Pid, State = #state{shuting_down = false}) ->
    #state
    {
        consumer_monitors = ConsumerMonitors,
        consumers = Consumers,
        name = Name,
        drops = Drops
    } = State,
    case orddict:find(MonitorRef, ConsumerMonitors) of
        {ok, Ref} ->
            case orddict:find(Ref, Consumers) of
                %% consumer does not have an object (we can just remove it, nothing to retry)
                {ok, #consumer{seq = undefined, qseq = undefined, cseq = undefined}} ->
                    NewConsumerMonitors = orddict:erase(MonitorRef, ConsumerMonitors),
                    NewConsumers = orddict:erase(Ref, Consumers),
                    State#state{consumer_monitors = NewConsumerMonitors, consumers = NewConsumers};

                %% consumer has an object, we need to drop it or retry the object with another consumer
                {ok, Consumer = #consumer{pid = Pid, qseq = QSeq}} ->
                    RetryCounter = get_retry_counter(Consumer, State) +1,
                    delete_object_from_queue(Consumer, State),
                    NewConsumers = orddict:erase(Ref, Consumers),
                    NewConsumerMonitors = orddict:erase(MonitorRef, ConsumerMonitors),
                    State1 = State#state{consumers = NewConsumers, consumer_monitors = NewConsumerMonitors},
                    case should_retry(RetryCounter, State) of
                        true ->
                            %% insert the object into the retry queue, and maybe_pull
                            State2 = insert_object_to_queue(QSeq, RetryCounter, State1),
                            maybe_pull(State2#state{status = retry});
                        false ->
                            %% ack the rtq
                            %% TODO: make a drop function to report the drop in the logs (log out bucket, key)
                            riak_repl2_rtq:ack(Name, QSeq),
                            State1#state{drops = Drops +1}
                    end;

                %% we could not find the consumer in the consumer orddict
                error ->
                    %% we got a down message with a monitor reference but the consumer does not exist
                    lager:error("'DOWN' message recieved with a monitor ref that matches no consumer"),
                    State
            end;

        %% we could not find the monitor in the consumer monitors orddict
        error ->
            %% we got a down message with a monitor reference that we are not aware of
            lager:error("'DOWN' message recieved with a monitor ref we are not aware of"),
            State
    end;
maybe_retry(_MonitorRef, _Pid, State = #state{shuting_down = true}) ->
    %% we don't care about this, as we are shutting down, we can let the queue migrate the object
    State.


insert_object_to_queue(Seq, RetryCounter, State = #state{retry_queue = RetryQ}) ->
    #queue{end_seq = EndSeq, table = Tab} = RetryQ,
    EndSeq2 = EndSeq +1,
    ets:insert(Tab, {EndSeq2, {Seq, RetryCounter}}),
    State#state{retry_queue = RetryQ#queue{end_seq = EndSeq2}}.

insert_object_to_queue(QSeq, State = #state{reference_queue = RefQ}) ->
    #queue{end_seq = EndSeq, table = Tab} = RefQ,
    EndSeq2 = EndSeq +1,
    ets:insert(Tab, {EndSeq2, QSeq}),
    State#state{reference_queue = RefQ#queue{end_seq = EndSeq2}}.


get_retry_counter(#consumer{seq = Seq}, State) ->
    #state{retry_queue = RetryQ, status = Status} = State,
    case Status of
        active ->
            0;
        retry ->
            #queue{table = Tab} = RetryQ,
            case ets:lookup(Tab, Seq) of
                [] ->
                    0;
                [{Seq, {_QSeq, RetryCounter}}] ->
                    RetryCounter
            end
    end.

should_retry(RetryCounter, State) ->
    case get_retry_limit(State) of
        unlimited ->
            true;
        N ->
            RetryCounter > N
    end.


delete_object_from_queue(#consumer{seq = Seq}, State) ->
    #state{reference_queue = RefQ, retry_queue = RetryQ, status = Status} = State,
    case Status of
        active ->
            #queue{table = Tab} = RefQ,
            ets:delete(Tab, Seq);
        retry ->
            #queue{table = Tab} = RetryQ,
            ets:delete(Tab, Seq)
    end.

populate_reference_table(Seq, Seq, State) ->
    State;
populate_reference_table('$end_of_table', _Seq, State) ->
    State;
populate_reference_table(Seq, QSeq, State = #state{qtab = QTab, name = Name}) ->
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
                    NewState = insert_object_to_queue(Seq, State),
                    populate_reference_table(Seq+1, QSeq, NewState)
            end
    end.


ack_seq(Ref, CSeq, State) ->
    #state{consumers = Consumers, name = Name, ack_counter = AC, consumer_fifo = ConsumerFIFO} = State,
    case orddict:find(Ref, Consumers) of
        {ok, #consumer{qseq = QSeq, pid = Pid} = C} when C#consumer.cseq == CSeq ->
            riak_repl2_rtq:ack(Name, QSeq),
            delete_object_from_queue(C, State),
            UpdatedConsumer = #consumer{pid = Pid},
            UpdatedConsumers = orddict:store(Ref, UpdatedConsumer, Consumers),
            UpdatedConsumerFIFO = queue:in(Ref, ConsumerFIFO),
            NewState =
                State#state{consumers =UpdatedConsumers, consumer_fifo = UpdatedConsumerFIFO, ack_counter = AC +1},
            maybe_pull(NewState);

        {ok, #consumer{pid = Pid} = C} ->
            lager:error("Sequence number from consumer did not match that stored sequence number in reference queue"),
            lager:error("CSeq: ~p, Consumer: ~p", [CSeq, C]),
            exit(Pid, shutdown),
            State;
        error ->
            %% this consumer is supposed to be dead, or sequence number did not match!
            %% what should we do? kill the process? (A retry to another process has already been sent!)
            lager:error("reference queue got ack from helper: Ref, there is a lingering process", [Ref]),
            State
    end.


do_push(QEntry, State = #state{status = Status, shuting_down = false}) ->
    {QSeq, _NumItem, _Bin, _Meta, _Completed} = QEntry,
     NewState = insert_object_to_queue(QSeq, State),
    #queue{start_seq = RefStartSeq, end_seq = RefEndSeq} = NewState#state.reference_queue,
    ShouldSend = (RefStartSeq +1) == RefEndSeq,

    %% maybe_deliver_object, only if we are at the head of the queue!
    case {Status, ShouldSend} of
        {active, true} ->
            maybe_deliver_object(RefEndSeq, QEntry, NewState);
        {active, false} ->
            NewState;
        {retry, _} ->
            maybe_pull(NewState)
    end;
do_push(_QEntry, State = #state{shuting_down = true}) ->
    State.


maybe_pull(State = #state{shuting_down = false}) ->
    case maybe_get_object(State) of
        {refq, Seq2, QEntry, NewState} ->
            maybe_deliver_object(Seq2, QEntry, NewState);
        {retryq, Seq2, _RetryCounter, QEntry, NewState} ->
            maybe_deliver_object(Seq2, QEntry, NewState);
        {error, State} ->
            State
    end;
maybe_pull(State = #state{shuting_down = true}) ->
    State.

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
    case ets:lookup(RefTab, StartSeq2) of
        [] ->
            %% entry removed from reference table? This would not happen
            lager:error("something removed reference queue entry!"),
            maybe_get_object(State#state{reference_queue = RefQ#queue{start_seq = StartSeq2}});
        [{StartSeq2, QSeq}] ->
            case ets:lookup(QTab, QSeq) of
                [] ->
                    %% entry removed, this can happen if the queue was trimmed!
                    maybe_get_object(State#state{reference_queue = RefQ#queue{start_seq = StartSeq2}});
                [QEntry] ->
                    {refq, StartSeq2, QEntry, State}
            end
    end;
maybe_get_object(StartSeq2, #state{status = retry, qtab = QTab, retry_queue = RetryQ} = State) ->
    #queue{table = RetryTab} = RetryQ,
    case ets:lookup(RetryTab, StartSeq2) of
        [] ->
            %% entry removed from reference table? This would not happen
            lager:error("something removed retry queue entry!"),
            maybe_get_object(State#state{retry_queue = RetryQ#queue{start_seq = StartSeq2}});
        [{StartSeq2, {QSeq, RetryCounter}}] ->
            %% TODO: consider checking retry counter here and throw the object away
            case ets:lookup(QTab, QSeq) of
                [] ->
                    %% entry removed, this can happen if the queue was trimmed!
                    maybe_get_object(State#state{retry_queue = RetryQ#queue{start_seq = StartSeq2}});
                [QEntry] ->
                    {retryq, StartSeq2, RetryCounter, QEntry, State}
            end
    end.

maybe_deliver_object(Seq2, QEntry, State = #state{consumer_fifo = ConsumerFIFO}) ->
    case queue:out(ConsumerFIFO) of
        {empty, NewFIFO} ->
            State#state{consumer_fifo = NewFIFO};
        {{value, ConsumerRef}, NewFIFO} ->
            maybe_deliver_object(ConsumerRef, NewFIFO, Seq2, QEntry, State)
    end.

maybe_deliver_object(ConsumerRef, NewFIFO, Seq2, QEntry, #state{consumers = Consumers} = State) ->
    case orddict:find(ConsumerRef, Consumers) of
        error ->
            %% consumer must have died and was removed from consumers
            %% however its ref was still in the queue!
            maybe_deliver_object(Seq2, QEntry, State#state{consumer_fifo = NewFIFO});
        {ok, Consumer} ->
            deliver_object(ConsumerRef, NewFIFO, Consumer, Seq2, QEntry, State)
    end.

deliver_object(ConsumerRef, NewFIFO, Consumer, Seq2, QEntry, State = #state{status = active, drops = Drops}) ->
    #state{consumers = Consumers, reference_queue = RefQ, name = Name} = State,
    #queue{start_seq = Seq} = RefQ,
    #consumer{pid = Pid} = Consumer,
    {QSeq, NumItems, Bin, Meta, _Completed} = QEntry,
    Entry = {Seq2, NumItems, Bin, Meta},
    OkError = send_object(Seq2, Seq, Pid, Entry),

    case OkError of
        {ok, CSeq} ->
            UpdatedConsumer = Consumer#consumer{qseq = QSeq, seq = Seq2, cseq = CSeq},
            UpdatedConsumers = orddict:store(ConsumerRef, UpdatedConsumer, Consumers),
            NewRefQ = RefQ#queue{start_seq = Seq2},
            State#state{consumers = UpdatedConsumers, consumer_fifo = NewFIFO, reference_queue = NewRefQ};

        bucket_type_not_supported_by_remote ->
            delete_object_from_queue(Seq2, RefQ),
            riak_repl2_rtq:ack(Name, QSeq),
            NewRefQ = RefQ#queue{start_seq = Seq2},
            maybe_pull(State#state{reference_queue = NewRefQ, drops = Drops +1});

        shutting_down ->
            maybe_deliver_object(Seq2, QEntry, State#state{consumer_fifo = NewFIFO});

        {error, badmatch, Error} ->
            lager:error("REFERENCE! error badmatch on sequence number: ~p", [Error]),
            lager:error("State:~p", [State]),
            State;

        {error, Type, Error} ->
            lager:warning("Failed to send object to rtsource helper pid: ~p", [Pid]),
            lager:error("Reference Queue Error: Type: ~p, Error: ~p", [Type, Error]),
            exit(Pid, shutdown),
            maybe_deliver_object(Seq2, QEntry, State#state{consumer_fifo = NewFIFO})
    end;

deliver_object(ConsumerRef, NewFIFO, Consumer, Seq2, QEntry, State = #state{status = retry, drops = Drops}) ->
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
            UpdatedConsumer = Consumer#consumer{qseq = QSeq, seq = Seq2, cseq = CSeq},
            UpdatedConsumers = orddict:store(ConsumerRef, UpdatedConsumer, Consumers),
            NewRetryQ = RetryQ#queue{start_seq = Seq2},
            NewState#state{consumers = UpdatedConsumers, consumer_fifo = NewFIFO, retry_queue = NewRetryQ};

        bucket_type_not_supported_by_remote ->
            delete_object_from_queue(Seq2, RetryQ),
            riak_repl2_rtq:ack(Name, QSeq),
            NewRetryQ = RetryQ#queue{start_seq = Seq2},
            maybe_pull(NewState#state{retry_queue = NewRetryQ, drops = Drops +1});

        shutting_down ->
            maybe_deliver_object(Seq2, QEntry, NewState#state{consumer_fifo = NewFIFO});

        {error, badmatch, Error} ->
            lager:error("RETRY! error badmatch on sequence number: ~p", [Error]),
            lager:error("State:~p", [State]),
            NewState;

        {error, Type, Error} ->
            lager:warning("Failed to send object to rtsource helper pid: ~p", [Pid]),
            lager:error("Reference Queue Error: Type: ~p, Error: ~p", [Type, Error]),
            exit(Pid, shutdown),
            maybe_deliver_object(Seq2, QEntry, State#state{consumer_fifo = NewFIFO})
    end.

send_object(Seq2, Seq, Pid, Entry) ->
    try
        Seq2 = Seq +1, %% sanity check
        %% send to rtsource helper
        case riak_repl2_rtsource_helper:send_object(Pid, Entry) of
            bucket_type_not_supported_by_remote ->
                bucket_type_not_supported_by_remote;
            shutting_down ->
                shutting_down;
            {ok, CSeq} ->
                {ok, CSeq}
        end
    catch T:E ->
        {error, T, E}
    end.



get_retry_limit() ->
    case app_helper:get_env(riak_repl, default_retry_limit) of
        N when is_integer(N) ->
            N;
        _ ->
            ?DEFAULT_RETRY_LIMIT
    end.

-ifdef(TEST).

get_retry_limit(_) ->
    get_retry_limit().

-else.
get_retry_limit(#state{name = Name}) ->
    case riak_core_metadata:get(?RIAK_REPL2_CONFIG_KEY, {retry_limit, Name}) of
        RetryLimit when is_integer(RetryLimit) ->
            RetryLimit;
        _ ->
            get_retry_limit()
    end.
-endif.
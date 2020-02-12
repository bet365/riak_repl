%% @doc Queue module for realtime replication.
-module(riak_repl2_rtq).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export(
[
    start_link/1,
    start_link/2,
    register_consumer/2,
    unregister/1,
    push/4,
    push/3,
    ack/2,
    status/1,
    is_empty/1,
    all_queues_empty/0,
    shutdown/0,
    stop/0,
    is_running/0
]).

% private api
-export([report_drops/1]).
-export([start_test/0]).
-export([name/1]).


-define(SERVER, ?MODULE).
-define(DEFAULT_OVERLOAD, 2000).
-define(DEFAULT_RECOVER, 1000).
-define(DEFAULT_RTQ_LATENCY_SLIDING_WINDOW, 300).
-define(DEFAULT_MAX_BYTES, 104857600). %% 100 MB

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state,
{
    name = undefined,
    qtab = ets:new(?MODULE, [protected, ordered_set, {read_concurrency, true}]), % ETS table
    overload = ?DEFAULT_OVERLOAD :: pos_integer(), % if the message q exceeds this, the rtq is overloaded
    recover = ?DEFAULT_RECOVER :: pos_integer(), % if the rtq is in overload mode, it does not recover until =<
    overloaded = false :: boolean(),
    overload_drops = 0 :: non_neg_integer(),
    shutting_down=false,
    qsize_bytes = 0,
    word_size=erlang:system_info(wordsize),

    total_filtered = 0,
    total_sent = 0,
    total_acked = 0,
    total_trimmed_drops = 0,

    cseq = 0, %% last sequence number sent to any consumer
    qseq = 0, %% Last sequence pushed onto the queue
    consumers = orddict:new(), %% orddict of all consumers (rtsource_helpers) stored using their ref
    consumer_queue = queue:new(), %% FIFO queue of refs, to be used to pop a consumer ref to send the next object too
    sent_tab = ets:new(?MODULE, [protected, ordered_set]) %% ets table to store objects sent to consumers
}).


% Consumers
-record(consumer,
{
    pid = undefined, %% rtsource_helper pid to push objects
    consumer_seq = 0,
    acked_seq = 0,
    sent_count = 0,
    acked_count = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

overload_ets_name(Name) ->
    IdBin = list_to_binary(Name),
    binary_to_atom(<<"riak_repl2_rtq_", IdBin/binary>>, latin1).
name(RemoteName) ->
    IdBin = list_to_binary(RemoteName),
    binary_to_atom(<<"riak_repl2_rtq_", IdBin/binary>>, latin1).

start_link(Name) ->
    Overload = app_helper:get_env(riak_repl, rtq_overload_threshold, ?DEFAULT_OVERLOAD),
    Recover = app_helper:get_env(riak_repl, rtq_overload_recover, ?DEFAULT_RECOVER),
    Opts = [{overload_threshold, Overload}, {overload_recover, Recover}],
    start_link(Name, Opts).
start_link(Name, Options) ->
    OverloadETS = overload_ets_name(Name),
    case ets:info(OverloadETS) of
        undefined ->
            OverloadETS = ets:new(OverloadETS, [named_table, public, {read_concurrency, true}]),
            ets:insert(OverloadETS, {overloaded, false});
        _ ->
            ok
    end,
    gen_server:start_link({local, name(Name)}, ?MODULE, Options, []).

start_test() ->
    gen_server:start(?MODULE, [], []).


register_consumer(RemoteName, Ref) ->
    gen_server:call(name(RemoteName), {register, Ref}, infinity).

status(RemoteName) ->
    Status = gen_server:call(name(RemoteName), status, infinity),
    % I'm having the calling process do derived stats because
    % I don't want to block the rtq from processing objects.
    MaxBytes = proplists:get_value(max_bytes, Status),
    CurrentBytes = proplists:get_value(bytes, Status),
    PercentBytes = round( (CurrentBytes / MaxBytes) * 100000 ) / 1000,
    [{percent_bytes_used, PercentBytes} | Status].




%% no longer used here
%%unregister(Name, Ref) ->
%%    gen_server:call(?SERVER, {unregister, Name}, infinity).

%%TODO re-write the below to work with the new method!





shutdown() ->
    gen_server:call(?SERVER, shutting_down, infinity).

stop() ->
    gen_server:call(?SERVER, stop, infinity).

is_running() ->
    gen_server:call(?SERVER, is_running, infinity).

%% TODO, replace with drain_queue
is_empty(Name) ->
    gen_server:call(?SERVER, {is_empty, Name}, infinity).

all_queues_empty() ->
    gen_server:call(?SERVER, all_queues_empty, infinity).
%%%=====================================================================================================================
%%% Casts
%%%=====================================================================================================================
push(Name, Seq, NumItems, Bin) ->
    push(Name, Seq, NumItems, Bin, []).
push(Name, Seq, NumItems, Bin, Meta) ->
    OverloadETS = overload_ets_name(Name),
    case ets:lookup(OverloadETS, overloaded) of
        [{overloaded, true}] ->
            lager:debug("rtq overloaded"),
            riak_repl2_rtq_overload_counter:drop();
        [{overloaded, false}] ->
            gen_server:cast(name(Name), {push, Seq, NumItems, Bin, Meta})
    end.





ack(Name, Seqs) ->
    gen_server:cast(?SERVER, {ack, Name, Seqs}).

report_drops(N) ->
    gen_server:cast(?SERVER, {report_drops, N}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Options) ->
    Overloaded = proplists:get_value(overload_threshold, Options, ?DEFAULT_OVERLOAD),
    Recover = proplists:get_value(overload_recover, Options, ?DEFAULT_RECOVER),
    {ok, #state{overload = Overloaded, recover = Recover}}. % lots of initialization done by defaults


handle_call({register, Ref}, Pid, State) ->
    {OkError, NewState} = register_consumer(Ref, Pid, State),
    {reply, OkError, NewState};


%%handle_call({unregister, Name}, _From, State) ->
%%    {Reply, NewState} =  unregister_q(Name, State),
%%    {reply, Reply, NewState};

%% TODO decide if want some information from reference rtq
handle_call(status, _From, State) ->
    Status = make_status(State),
    {reply, Status, State};

%% this is okay
handle_call(shutting_down, _From, State = #state{shutting_down=false}) ->
    %% this will allow the realtime repl hook to determine if it should send
    %% to another host
    _ = riak_repl2_rtq_proxy:start(),
    {reply, ok, State#state{shutting_down = true}};

%% this is okay
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

%% this is okay
handle_call(is_running, _From, State = #state{shutting_down = ShuttingDown}) ->
    {reply, not ShuttingDown, State};


%%%=====================================================================================================================
%% TODO, replace with drain_queue functionality for repl_migration
handle_call({is_empty, Name}, _From, State = #state{remotes = Remotes}) ->
    Result = is_queue_empty(Name, Remotes),
    {reply, Result, State};

handle_call(all_queues_empty, _From, State = #state{remotes = Remotes}) ->
    Result = lists:all(fun (#remote{name = Name}) -> is_queue_empty(Name, Remotes) end, Remotes),
    {reply, Result, State}.


%%%=====================================================================================================================
%%% PUSH Functionality -> move completly too riak_repl2_rtq_router!
%%%=====================================================================================================================

%% TODO decide if this code stays (it is legacy) [they are needed for backward compatibility]
% either old code or old node has sent us a old push, upvert it.
%%handle_call({push, NumItems, Bin}, From, State) ->
%%    handle_call({push, NumItems, Bin, [], []}, From, State);
%%handle_call({push, NumItems, Bin, Meta, []}, _From, State) ->
%%    State2 = maybe_flip_overload(State),
%%    {reply, ok, maybe_push(NumItems, Bin, Meta, State2)}.
%%%%%=====================================================================================================================
%%
%%% have to have backward compatability for cluster upgrades
%%handle_cast({push, NumItems, Bin}, State) ->
%%    handle_cast({push, NumItems, Bin, []}, State);
%%handle_cast({push, NumItems, Bin, Meta}, State) ->
%%    State2 = maybe_flip_overload(State),
%%    {noreply, maybe_push(NumItems, Bin, Meta, State2)};


handle_cast({push, Seq, NumItems, Bin, Meta}, State) ->
    State2 = maybe_flip_overload(State),
    {noreply, do_push(Seq, NumItems, Bin, Meta, State2)};




handle_cast({ack, Name, Seqs}, State) ->
       {noreply, ack_seqs(Name, Seqs, State)};

%%TODO: should we include remote drops?
handle_cast({report_drops, N}, State) ->
    QSeq = State#state.qseq + N,
    Drops = State#state.overload_drops + N,
    State2 = State#state{qseq = QSeq, overload_drops = Drops},
    State3 = maybe_flip_overload(State2),
    {noreply, State3}.


handle_info(_Msg, State) ->
    {noreply, State}.

%%TODO: this needs fixing
terminate(Reason, State) ->
  lager:info("rtq terminating due to: ~p State: ~p", [Reason, State]),
    %% when started from tests, we may not be registered
    catch(erlang:unregister(?SERVER)),
    flush_pending_pushes(),
%%    _ = [deliver_error(DeliverFun, {terminate, Reason}) || DeliverFun <- lists:flatten(DList)],
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%% ================================================================================================================== %%

%% Internal Functions For Gen Server Calls

%% ================================================================================================================== %%
%% Register
%% ================================================================================================================== %%
%%register_remote(Name, Pid, State = #state{remotes = Remotes, all_remote_names = AllRemoteNames}) ->
%%    UpdatedRemotes =
%%        case lists:keytake(Name, #remote.name, Remotes) of
%%            {value, R = #remote{name = Name, pid = Pid}, Remotes2} ->
%%                %% rtsource_rtq has re-registered (under the same pid)
%%                Remotes;
%%            {value, R = #remote{name = Name, pid = Pid2}, Remotes2} ->
%%                %% rtosurce_rtq hash re-registered (new pid, it must have died)
%%                [R#remote{pid = Pid2} | Remotes2];
%%            false ->
%%                %% New registration, start from the beginning
%%                [#remote{name = Name, pid = Pid} | Remotes]
%%        end,
%%    UpdatedAllRemoteNames =
%%        case lists:member(Name, AllRemoteNames) of
%%            true -> AllRemoteNames;
%%            false -> [Name | AllRemoteNames]
%%        end,
%%    State#state{remotes = UpdatedRemotes, all_remote_names = UpdatedAllRemoteNames}.

register_consumer(Ref, Pid, State = #state{consumer_queue = Queue, consumers = Consumers}) ->
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
    end.

%% ================================================================================================================== %%
%% Unregister
%% ================================================================================================================== %%
%%unregister_q(Name, State = #state{remotes = Remotes, all_remote_names = AllRemoteNames, qtab = QTab}) ->
%%    NewAllRemoteNames = AllRemoteNames -- [Name],
%%    case lists:keytake(Name, #remote.name, Remotes) of
%%        {value, Remote, Remotes2} ->
%%            MinSeq = ets:first(QTab),
%%            NewState = unregister_cleanup(Remote, MinSeq, State#state{remotes = Remotes2, all_remote_names = NewAllRemoteNames}),
%%            {ok, NewState};
%%        false ->
%%            {{error, not_registered}, State}
%%    end.

%% we have to iterate the entire queue and check if we need to delete any objects
%%unregister_cleanup('$end_of_table', _Remote, State) ->
%%    State;
%%unregister_cleanup(Seq, Remote, State = #state{qtab = QTab, all_remote_names = AllRemoteNames}) ->
%%    case ets:lookup(QTab, Seq) of
%%        [{_, _, Bin, _, Completed}] ->
%%            ShrinkSize = ets_obj_size(Bin, State),
%%            case AllRemoteNames -- Completed of
%%                [] ->
%%                    ets:delete(QTab, Seq),
%%                    NewState = update_queue_size(State, -ShrinkSize),
%%                    unregister_cleanup(ets:next(QTab, Seq), Remote, NewState);
%%                _ ->
%%                    unregister_cleanup(ets:next(QTab, Seq), Remote, State)
%%            end;
%%        _ ->
%%            unregister_cleanup(ets:next(QTab, Seq), Remote, State)
%%    end.


%% ================================================================================================================== %%
%% Status
%% TODO: consumer latency needs to be added back in some how (maybe hand this off to rtsource_conn/helper?)
%% ================================================================================================================== %%
make_status(State) ->
    #state
    {
        name = Name,
        qtab = QTab,
        qseq = QSeq,
        cseq = CSeq,
        total_filtered = Filtered,
        total_trimmed_drops = Drops,
        total_acked = Acked,
        total_sent = Sent,
        overload_drops = OverloadedDrops
    } = State,
    [
        {bytes, qbytes(QTab, State)},
        {max_bytes, get_queue_max_bytes(Name)},
        {pending, (QSeq - Filtered - Drops) - CSeq},
        {unacked, Sent - Acked},
        {filtered, Filtered},
        {acked, Acked},
        {trimmed_drops, Drops},
        {overload_drops, OverloadedDrops}
    ].

%% ================================================================================================================== %%

%% Internal Functions For Gen Server Casts

%% ================================================================================================================== %%
%% Push
%% ================================================================================================================== %%
%%maybe_push(NumItems, Bin, Meta, PreCompleted, State) ->
%%    #state
%%    {
%%        qtab = QTab,
%%        last_seq = LastSeq,
%%        current_seq = CurrentSeq,
%%        shutting_down = false
%%    } = State,
%%    case filter_remotes(AllRemoteNames, PreCompleted, Meta) of
%%        {[], _} ->
%%            %% We have no remotes to send too, drop the object (do not insert)
%%            State;
%%        {Send, Completed} ->
%%            % create object to place into queue
%%            QSeq2 = QSeq + 1,
%%            Meta1 = set_local_forwards_meta(Send, Meta),
%%            Meta2 = set_skip_meta(Meta1),
%%            QEntry = {QSeq2, NumItems, Bin, Meta2, Completed},
%%            State1 = State#state{qseq = QSeq2},
%%
%%            %% insert object into queue
%%            ets:insert(QTab, QEntry),
%%
%%            %% update queue and remote sizes
%%            Size = ets_obj_size(Bin, State1),
%%            State2 = update_queue_size(State1, Size),
%%
%%            %% push to reference queues
%%            State3 = push_to_remotes(Send, QSeq2, State2),
%%
%%            %% (trim queue) find out if queue reached maximum capacity
%%            maybe_trim_queue(State4)
%%    end;
%%push(NumItems, Bin, Meta, PreCompleted, State = #state{shutting_down = true}) ->
%%    riak_repl2_rtq_proxy:push(NumItems, Bin, Meta, PreCompleted),
%%    State.

do_push(Seq, NumItems, Bin, Meta, State) ->
    #state
    {
        qtab = QTab,
        qseq = QSeq,
        cseq = CSeq,
        shutting_down = false
    } = State,
    % update state
    QEntry = {Seq, NumItems, Bin, Meta},
    State1 = State#state{qseq = Seq},

    %% insert object into queue
    ets:insert(QTab, QEntry),

    %% update queue size
    Size = ets_obj_size(Bin, State1),
    State2 = update_queue_size(State1, Size),

    %% push to out to consumer iff we are at the head of the queue
    State3 = case QSeq == CSeq of
                 true ->
                     maybe_deliver_object(QEntry, State2);
                 false ->
                     State
             end,

    %% (trim queue) find out if queue reached maximum capacity
    maybe_trim_queue(State3);

do_push(NumItems, Bin, Meta, PreCompleted, State = #state{shutting_down = true}) ->
    riak_repl2_rtq_proxy:push(NumItems, Bin, Meta, PreCompleted),
    State.



%% ================================================================================================================== %%
%% Push Helper Functions
%% ================================================================================================================== %%


%% ==================================================== %%
%% Maybe Send
%% ==================================================== %%
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

%% ==================================================== %%
%% Trimming The Queue
%% ==================================================== %%
maybe_trim_queue(State) ->
    maybe_trim_queue(State, 0).
maybe_trim_queue(State = #state{qtab = QTab}, Counter) ->
    case queue_needs_trim(State) of
        true ->
            NewState = trim_single_queue_entry(ets:first(QTab), State),
            maybe_trim_queue(NewState, Counter+1);
        false ->
            case Counter of
                0 -> ok;
                _ ->
                    lager:error("Dropped ~p objects in ~p entries due to reaching maximum queue size of ~p bytes",
                        [Counter, get_queue_max_bytes()])
            end,
            State
    end.

queue_needs_trim(#state{qsize_bytes = QBytes}) ->
    QBytes > get_queue_max_bytes().

trim_single_queue_entry(Seq, State = #state{qtab = QTab}) ->
    case ets:lookup(QTab, Seq) of
        [{_, _, Bin, _}] ->
            ets:delete(QTab, Seq),
            ShrinkSize = ets_obj_size(Bin, State),
            update_queue_size(State, -ShrinkSize);
        _ ->
            State

    end.

%% ================================================================================================================== %%
%% Acking the queue. Either adds to a remote to the 'Completed' list, or deletes the object.
%% TODO: this needs altered
%% ================================================================================================================== %%
ack_seqs(Name, Seqs, State = #state{remotes = Remotes}) ->
    case lists:keytake(Name, #remote.name, Remotes) of
        false ->
            State;
        {value, Remote, Remotes2} ->
            {UpdatedRemote, UpdatedState} =
                lists:foldl(
                    fun(Seq, {AccRemote, AccState}) ->
                        ack_seq(Seq, AccRemote, AccState)
                    end, {Remote, State}, Seqs),
            UpdatedState#state{remotes = [UpdatedRemote |Remotes2]}
    end.

ack_seq(Seq, Remote = #remote{max_ack = MaxAck}, State) ->
    #state{qtab = QTab, qsize_bytes = QSize, all_remote_names = AllRemoteNames} = State,
    NewRemote1 =
        case Seq > MaxAck of
            true ->
                Remote#remote{max_ack = Seq};
            false ->
                Remote
        end,
    case ets:lookup(QTab, Seq) of
        [] ->
            %% TODO:
            %% the queue has been trimmed due to reaching its maximum size
            %% but this has been sent and acked! - so we can reduce the dropped counter
            {NewRemote1, State};
        [{_, _, Bin, _, Completed}] ->
            NewCompleted = [NewRemote1#remote.name | Completed],
            ShrinkSize = ets_obj_size(Bin, State),
            NewRemote2 = update_remote_queue_size(NewRemote1, -ShrinkSize),
            case AllRemoteNames -- NewCompleted of
                [] ->
                    ets:delete(QTab, Seq),
                    NewState = update_queue_size(State, -ShrinkSize),
                    {NewRemote2, NewState};
                _ ->
                    ets:update_element(QTab, Seq, {5, NewCompleted}),
                    {NewRemote2, State}
            end;
        UnExpectedObj ->
            lager:warning("Unexpected object in RTQ, ~p", [UnExpectedObj]),
            {NewRemote1, QSize}
    end.
%% ================================================================================================================== %%

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% ================================================================================================================== %%
%% Maybe flip overload used to ensure that the rtq mailbox does not exceed a configured size.
%% ================================================================================================================== %%
maybe_flip_overload(State) ->
    #state{overloaded = Overloaded, overload = Overload, recover = Recover, name = Name} = State,
    OverloadETS = overload_ets_name(Name),
    {message_queue_len, MsgQLen} = erlang:process_info(self(), message_queue_len),
    if
        Overloaded andalso MsgQLen =< Recover ->
            lager:info("Recovered from overloaded condition"),
            ets:insert(OverloadETS, {overloaded, false}),
            State#state{overloaded = false};
        (not Overloaded) andalso MsgQLen > Overload ->
            lager:warning("Realtime queue mailbox size of ~p is greater than ~p indicating overload; objects will be dropped until size is less than or equal to ~p", [MsgQLen, Overload, Recover]),
            % flip the rt_dirty flag on
            riak_repl_stats:rt_source_errors(),
            ets:insert(OverloadETS, {overloaded, true}),
            State#state{overloaded = true};
        true ->
            State
    end.


flush_pending_pushes() ->
    receive
        {'$gen_cast', {push, NumItems, Bin, Meta, Completed}} ->
            riak_repl2_rtq_proxy:push(NumItems, Bin, Meta, Completed),
            flush_pending_pushes();
        {'$gen_cast', {push, NumItems, Bin, Meta}} ->
            riak_repl2_rtq_proxy:push(NumItems, Bin, Meta),
            flush_pending_pushes();
        {'$gen_cast', {push, NumItems, Bin}} ->
            riak_repl2_rtq_proxy:push(NumItems, Bin),
            flush_pending_pushes()
    after
        1000 ->
            ok
    end.



ets_obj_size(Obj, _State=#state{word_size = WordSize}) when is_binary(Obj) ->
  ets_obj_size(Obj, WordSize);
ets_obj_size(Obj, WordSize) when is_binary(Obj) ->
  BSize = erlang:byte_size(Obj),
  case BSize > 64 of
        true -> BSize - (6 * WordSize);
        false -> BSize
  end;
ets_obj_size(Obj, _) ->
  erlang:size(Obj).


update_queue_size(State = #state{qsize_bytes = CurrentQSize}, Diff) ->
    State#state{qsize_bytes = CurrentQSize + Diff}.

update_total_drops(Remote = #remote{total_drops = Drops}, Diff) ->
    Remote#remote{total_drops = Drops + Diff}.

is_queue_empty(Name, Remotes) ->
    case lists:keytake(Name, #remote.name, Remotes) of
        {value,  #remote{rsize_bytes = RBytes}, _Remotes2} ->
            case RBytes == 0 of
                true -> false;
                false -> true
            end;
        false -> lager:error("Unknown queue")
    end.



-ifdef(TEST).
qbytes(_QTab, #state{qsize_bytes = QSizeBytes}) ->
    %% when EQC testing, don't account for ETS overhead
    QSizeBytes.

get_queue_max_bytes() ->
    %% Default maximum realtime queue size to 100Mb
    app_helper:get_env(riak_repl, rtq_max_bytes, ?DEFAULT_MAX_BYTES).

-else.
qbytes(QTab, #state{qsize_bytes = QSizeBytes, word_size=WordSize}) ->
    Words = ets:info(QTab, memory),
    (Words * WordSize) + QSizeBytes.

get_queue_max_bytes(#state{name = Name}) ->
    case riak_core_metadata:get(?RIAK_REPL2_RTQ_CONFIG_KEY, {rtq_max_bytes, Name}) of
        undefined -> ?DEFAULT_MAX_BYTES;
        MaxBytes -> MaxBytes
    end.
    -endif.

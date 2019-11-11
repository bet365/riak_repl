%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.

%% @doc Queue module for realtime replication.
%%
%% The queue strives to reliably pass on realtime replication, with the
%% aim of reducing the need to fullsync.  Every item in the queue is
%% given a sequence number when pushed.  Consumers register with the
%% queue, then pull passing in a function to receive items (executed
%% on the queue process - it can cast/! as it desires).
%%
%% Once the consumer has delievered the item, it must ack the queue
%% with the sequence number.  If multiple deliveries have taken
%% place an ack of the highest seq number acknowledge all previous.
%%
%% The queue is currently stored in a private ETS table.  Once
%% all consumers are done with an item it is removed from the table.
-module(riak_repl2_rtq).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export([start_link/0,
         start_link/1,
         start_test/0,
         register_remote/1,
         unregister_remote/1,
         register_consumer/4,
         unregister_consumer/2,
         push/3,
         push/2,
         pull/2,
         pull_sync/2,
         ack/3,
         ack_sync/3,
         status/0,
         dumpq/0,
         summarize/0,
         evict/1,
         evict/2,
         is_empty/1,
         all_queues_empty/0,
         shutdown/0,
         stop/0,
         is_running/0]).
% private api
-export([report_drops/1]).

-define(overload_ets, rtq_overload_ets).
-define(SERVER, ?MODULE).
-define(DEFAULT_OVERLOAD, 2000).
-define(DEFAULT_RECOVER, 1000).
-define(DEFAULT_RTQ_LATENCY_SLIDING_WINDOW, 300).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    qtab = ets:new(?MODULE, [protected, ordered_set]), % ETS table
    qseq = 0,  % Last sequence number handed out
    default_max_bytes = undefined, % maximum ETS table memory usage in bytes

    % if the message q exceeds this, the rtq is overloaded
    overload = ?DEFAULT_OVERLOAD :: pos_integer(),

    % if the rtq is in overload mode, it does not recover until =<
    recover = ?DEFAULT_RECOVER :: pos_integer(),

    overloaded = false :: boolean(),
    overload_drops = 0 :: non_neg_integer(),

    remotes = [],
    shutting_down=false,
    qsize_bytes = 0,
    word_size=erlang:system_info(wordsize)
}).

% Consumers
-record(remote, {
    name,      % remote name
    mode = normal, %% normal | recover_consumer
    rseq = 0,  % last sequence sent
    aseq = 0,  % last sequence acked
    q_drops = 0, % number of dropped queue entries (not items)
    r_drops = 0,
    drops = 0,
    errs = 0,  % delivery errors
    active_consumers = [],
    inactive_consumers = [],
    recover_unregistered_consumers = [],
    last_seen,  % a timestamp of when we received the last ack to measure latency
    remote_qbytes = 0
}).

-record(consumer, {
    version = 1,
    ctab,               % ets table per consumer/ per remote (mapping cseq -> rseq -> qseq) [DO WE NEED THIS?]
    ref,
    deliver_fun,
    cseq = 0,
    caseq = 0
}).

-type name() :: term().
-type seq() :: non_neg_integer().

%% API
%% @doc Start linked, registered to module name.
-spec start_link() -> {ok, pid()}.
start_link() ->
    Overload = app_helper:get_env(riak_repl, rtq_overload_threshold, ?DEFAULT_OVERLOAD),
    Recover = app_helper:get_env(riak_repl, rtq_overload_recover, ?DEFAULT_RECOVER),
    Opts = [{overload_threshold, Overload}, {overload_recover, Recover}],
    start_link(Opts).

-type overload_threshold_option() :: {'overload_threshold', pos_integer()}.
-type overload_recover_option() :: {'overload_recover', pos_integer()}.
-type start_option() :: overload_threshold_option() | overload_recover_option().
-type start_options() :: [start_option()].
%% @doc Start linked, registers to module name, with given options. This makes
%% testing some options a bit easier as it removes a dependance on app_helper.
-spec start_link(Options :: start_options()) -> {'ok', pid()}.
start_link(Options) ->
    case ets:info(?overload_ets) of
        undefined ->
            ?overload_ets = ets:new(?overload_ets, [named_table, public, {read_concurrency, true}]),
            ets:insert(?overload_ets, {overloaded, false});
        _ ->
            ok
    end,
    gen_server:start_link({local, ?SERVER}, ?MODULE, Options, []).

%% @doc Test helper, starts unregistered and unlinked.
-spec start_test() -> {ok, pid()}.
start_test() ->
    gen_server:start(?MODULE, [], []).

%% @doc Register a remote with the given name. The Name of the remote is
%% the name of the remote cluster by convention. Returns the oldest unack'ed
%% sequence number.
-spec register_remote(Name :: name()) -> {'ok', number()}.
register_remote(Name) ->
    gen_server:call(?SERVER, {register_remote, Name}, infinity).

%% @doc Removes a remote.
-spec unregister_remote(Name :: name()) -> 'ok' | {'error', 'not_registered'}.
unregister_remote(Name) ->
    gen_server:call(?SERVER, {unregister_remote, Name}, infinity).


register_consumer(Name, Fun, Ref, Version) ->
    gen_server:call(?SERVER, {register_consumer, Name, Fun, Ref, Version}, infinity).
unregister_consumer(Name, Ref) ->
    gen_server:call(?SERVER, {unregister_consumer, Name, Ref}, infinity).

%% @doc True if the given remote has no items to consume.
-spec is_empty(Name :: name()) -> boolean().
is_empty(Name) ->
    gen_server:call(?SERVER, {is_empty, Name}, infinity).

%% @doc True if no remote has items to consume.
-spec all_queues_empty() -> boolean().
all_queues_empty() ->
    gen_server:call(?SERVER, all_queues_empty, infinity).

%% @doc Push an item onto the queue. Bin should be the list of objects to push
%% run through term_to_binary, while NumItems is the length of that list
%% before being turned to a binary. Meta is an orddict of data about the
%% queued item. The key `routed_clusters' is a list of the clusters the item
%% has received and ack for. The key `local_forwards' is added automatically.
%% It is a list of the remotes this cluster forwards to. It is intended to be
%% used by remotes to alter the `routed_clusters' key before being sent to
%% the sink.
-spec push(NumItems :: pos_integer(), Bin :: binary(), Meta :: orddict:orddict()) -> 'ok'.
push(NumItems, Bin, Meta) ->
    case should_drop() of
        true ->
            lager:debug("rtq overloaded"),
            riak_repl2_rtq_overload_counter:drop();
        false ->
             gen_server:cast(?SERVER, {push, NumItems, Bin, Meta})
    end.

should_drop() ->
    [{overloaded, Val}] = ets:lookup(?overload_ets, overloaded),
    Val.

%% @doc Like `push/3', only Meta is `orddict:new/0'.
-spec push(NumItems :: pos_integer(), Bin :: binary()) -> 'ok'.
push(NumItems, Bin) ->
    push(NumItems, Bin, []).

%% @doc Using the given DeliverFun, send an item to the remote Name
%% asynchonously.
-type queue_entry() :: {pos_integer(), pos_integer(), binary(), orddict:orddict()}.
-type not_reg_error() :: {'error', 'not_registered'}.
-type deliver_fun() :: fun((queue_entry() | not_reg_error()) -> 'ok').
-spec pull(Name :: name(), DeliverFun :: deliver_fun()) -> 'ok'.
pull(Name, DeliverFun) ->
    gen_server:cast(?SERVER, {pull, Name, DeliverFun}).

%% @doc Block the caller while the pull is done.
-spec pull_sync(Name :: name(), DeliverFun :: deliver_fun()) -> 'ok'.
pull_sync(Name, Ref) ->
    gen_server:call(?SERVER, {pull_with_ack, Name, Ref}, infinity).

%% @doc Asynchronously acknowldge delivery of all objects with a sequence
%% equal or lower to Seq for the remote.
-spec ack(Name :: name(), Seq :: pos_integer(), Ref :: reference()) -> 'ok'.
ack(Name, Seq, Ref) ->
    gen_server:cast(?SERVER, {ack, Name, Ref, Seq, os:timestamp()}).

%% @doc Same as `ack/2', but blocks the caller.
-spec ack_sync(Name :: name(), Seq :: pos_integer(), Ref :: reference()) ->'ok'.
ack_sync(Name, Seq, Ref) ->
    gen_server:call(?SERVER, {ack_sync, Name, Ref, Seq, os:timestamp()}, infinity).

%% @doc The status of the queue.
%% <dl>
%% <dt>`percent_bytes_used'</dt><dd>How full the queue is in percentage to 3 significant digits</dd>
%% <dt>`bytes'</dt><dd>Size of the data store backend</dd>
%% <dt>`max_bytes'</dt><dd>Maximum size of the data store backend</dd>
%% <dt>`consumers'</dt><dd>Key - Value pair of the consumer stats, key is the
%% consumer name.</dd>
%% </dl>
%%
%% The consumers have the following data:
%% <dl>
%% <dt>`pending'</dt><dd>Number of queue items left to send.</dd>
%% <dt>`unacked'</dt><dd>Number of queue items that are sent, but not yet acked</dd>
%% <dt>`drops'</dt><dd>Dropped entries due to `max_bytes'</dd>
%% <dt>`errs'</dt><dd>Number of non-ok returns from deliver fun</dd>
%% </dl>
-spec status() -> [any()].
status() ->
    Status = gen_server:call(?SERVER, status, infinity),
    % I'm having the calling process do derived stats because
    % I don't want to block the rtq from processing objects.
    MaxBytes = proplists:get_value(max_bytes, Status),
    CurrentBytes = proplists:get_value(bytes, Status),
    PercentBytes = round( (CurrentBytes / MaxBytes) * 100000 ) / 1000,
    [{percent_bytes_used, PercentBytes} | Status].

%% @doc return the data store as a list.
-spec dumpq() -> [any()].
dumpq() ->
    gen_server:call(?SERVER, dumpq, infinity).

%% @doc Return summary data for the objects currently in the queue.
%% The return value is a list of tuples of the form {SequenceNum, Key, Size}.
-spec summarize() -> [{seq(), riak_object:key(), non_neg_integer()}].
summarize() ->
    gen_server:call(?SERVER, summarize, infinity).

%% @doc If an object with the given Seq number is currently in the queue,
%% evict it and return ok.
-spec evict(Seq :: seq()) -> 'ok'.
evict(Seq) ->
    gen_server:call(?SERVER, {evict, Seq}, infinity).

%% @doc If an object with the given Seq number is currently in the queue and it
%% also matches the given Key, then evict it and return ok. This is a safer
%% alternative to evict/1 since `Seq' numbers can potentially be recycled.
%% It also provides a more meaningful return value in the case that the object
%% was not present. Specifically, if there is no object in the queue with the
%% given `Seq' number, then {not_found, Seq} is returned, whereas if the
%% object with the given `Seq' number is present but does not match the
%% provided `Key', then {wrong_key, Seq, Key} is returned.
-spec evict(Seq :: seq(), Key :: riak_object:key()) ->
    'ok' | {'not_found', integer()} | {'wrong_key', integer(), riak_object:key()}.
evict(Seq, Key) ->
    gen_server:call(?SERVER, {evict, Seq, Key}, infinity).

%% @doc Signal that this node is doing down, and so a proxy process needs to
%% start to avoid dropping, or aborting unacked results.
-spec shutdown() -> 'ok'.
shutdown() ->
    gen_server:call(?SERVER, shutting_down, infinity).

stop() ->
    gen_server:call(?SERVER, stop, infinity).

%% @doc Will explode if the server is not started, but will tell you if it's
%% in shutdown.
-spec is_running() -> boolean().
is_running() ->
    gen_server:call(?SERVER, is_running, infinity).

%% @private
report_drops(N) ->
    gen_server:cast(?SERVER, {report_drops, N}).

%% Internals
%% @private
init(Options) ->
    %% Default maximum realtime queue size to 100Mb
    DefaultMaxBytes = app_helper:get_env(riak_repl, rtq_max_bytes, 100*1024*1024),
    Overloaded = proplists:get_value(overload_threshold, Options, ?DEFAULT_OVERLOAD),
    Recover = proplists:get_value(overload_recover, Options, ?DEFAULT_RECOVER),
    {ok, #state{default_max_bytes = DefaultMaxBytes, overload = Overloaded, recover = Recover}}. % lots of initialization done by defaults

%% @private
handle_call(status, _From, State = #state{qtab = QTab, qseq = QSeq, remotes = Rs}) ->
    
    %% TODO: calculate unacked from sent and acked
    MaxBytes = get_queue_max_bytes(State),
    Remotes =
        [{Name, [{remote_qbytes, RBytes},
                 {remote_max_qbytes, get_remote_max_bytes(R)},
                 {pending, QSeq - RSeq},  % items to be send
%%                 {unacked, RSeq - ASeq - Skips},  % sent items requiring ack
                 {r_drops, RDrops},
                 {q_drops, QDrops},
                 {drops, Drops},          % number of dropped entries due to max bytes
                 {errs, Errs}]}           % number of non-ok returns from deliver fun


         || #remote{name = Name, aseq = _ASeq, rseq = RSeq, q_drops = QDrops, r_drops = RDrops, drops = Drops,
            errs = Errs, remote_qbytes = RBytes} = R <- Rs],

    GetTrimmedFolsomMetrics =
        fun(MetricName) ->
            lists:foldl(fun
                            ({min, Value}, Acc) -> [{latency_min, Value} | Acc];
                            ({max, Value}, Acc) -> [{latency_max, Value} | Acc];
                            ({percentile, Value}, Acc) -> [{latency_percentile, Value} | Acc];
                            (_, Acc) -> Acc
                        end, [], folsom_metrics:get_histogram_statistics(MetricName))
        end,


    UpdatedRemoteStats = lists:foldl(fun(RemoteName, Acc) ->
                                    MetricName = {latency, RemoteName},
                                    case folsom_metrics:metric_exists(MetricName) of
                                        true ->
                                            case lists:keytake(RemoteName, 1, Acc) of
                                                {value, {RemoteName, RemoteStats}, Rest} ->
                                                    [{RemoteName, RemoteStats ++ GetTrimmedFolsomMetrics(MetricName)} | Rest];
                                                _ ->
                                                    Acc
                                            end;
                                        false ->
                                            Acc
                                    end
                                end, Remotes, [ R#remote.name || R <- Rs ]),

    Status =
        [{bytes, qbytes(QTab, State)},
         {max_bytes, MaxBytes},
         {remotes, UpdatedRemoteStats},
         {overload_drops, State#state.overload_drops}],
    {reply, Status, State};

handle_call(shutting_down, _From, State = #state{shutting_down=false}) ->
    %% this will allow the realtime repl hook to determine if it should send
    %% to another host
    _ = riak_repl2_rtq_proxy:start(),
    {reply, ok, State#state{shutting_down = true}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(is_running, _From,
            State = #state{shutting_down = ShuttingDown}) ->
    {reply, not ShuttingDown, State};

handle_call({is_empty, Name}, _From, State = #state{remotes = Rs}) ->
    Result = is_queue_empty(Name, Rs),
    {reply, Result, State};

handle_call(all_queues_empty, _From, State = #state{remotes = Rs}) ->
    Result = lists:all(fun (#remote{name = Name}) -> is_queue_empty(Name, Rs) end, Rs),
    {reply, Result, State};


handle_call({register_remote, Name}, _From, State = #state{qtab = QTab, qseq = QSeq, remotes = Rs}) ->
    MinSeq = minseq(QTab, QSeq),
    UpdatedRemotes =
        case lists:keytake(Name, #remote.name, Rs) of
            {value, R = #remote{aseq = PrevASeq, drops = PrevDrops, q_drops = QPrevDrops}, Rs2} ->
                %% Work out if anything should be considered dropped if
                %% unacknowledged.
                Drops = max(0, MinSeq - PrevASeq - 1),

                %% Re-registering, send from the last acked sequence
                RSeq =
                    case R#remote.aseq < MinSeq of
                        true -> MinSeq;
                        false -> R#remote.aseq
                    end,

                [R#remote{rseq = RSeq, drops = PrevDrops + Drops, q_drops = QPrevDrops + Drops} | Rs2];

            false ->
                %% New registration, start from the beginning
                %% loop the rtq, update remote q size, and add self to filter list if required
                RSeq = MinSeq,
                RegisteredRemote = register_new_remote(State, #remote{name = Name, aseq = RSeq, rseq = RSeq}, ets:first(QTab)),
                [RegisteredRemote | Rs]
        end,

    RTQSlidingWindow = app_helper:get_env(riak_repl, rtq_latency_window, ?DEFAULT_RTQ_LATENCY_SLIDING_WINDOW),
    case folsom_metrics:metric_exists({latency, Name}) of
        true -> skip;
        false ->
            folsom_metrics:new_histogram({latency, Name}, slide, RTQSlidingWindow)
    end,
    {reply, {ok, RSeq}, State#state{remotes =  UpdatedRemotes}};
handle_call({unregister_remote, Name}, _From, State) ->
    case unregister_q(Name, State) of
        {ok, NewState} ->
            catch folsom_metrics:delete_metric({latency, Name}),
            {reply, ok, NewState};
        {{error, not_registered}, State} ->
            {reply, {error, not_registered}, State}
  end;

handle_call({register_consumer, Name, Fun, Ref, Version}, _From, #state{remotes = Rs} = State) ->
    case lists:keytake(Name, #remote.name, Rs) of
        {value, R, Rest} ->
            AllCs = R#remote.active_consumers ++ R#remote.inactive_consumers ++ R#remote.recover_unregistered_consumers,
            case lists:keyfind(Ref, #consumer.ref, AllCs) of
                true ->
                    {reply, ref_exists, State};
                false ->
                    NewEts = ets:new(Ref, [protected, ordered_set]),
                    NewC = #consumer{version = Version, ctab = NewEts, ref = Ref, deliver_fun = Fun},
                    NewR = R#remote{active_consumers = R#remote.active_consumers ++ [NewC]},
                    {reply, ok, State#state{remotes = [NewR| Rest]}}
            end;
        false ->
            {reply, remote_not_registered, State}
    end;

handle_call({unregister_consumer, Name, Ref}, _From, #state{remotes = Rs} = State) ->
    case lists:keytake(Name, #remote.name, Rs) of
        {value, R, RestR} ->
            case lists:keytake(Ref, #consumer.ref, R#remote.active_consumers) of
                {value, C, RestC} ->
                    NewR = R#remote
                    {
                        active_consumers = RestC,
                        recover_unregistered_consumers = R#remote.recover_unregistered_consumers ++ [C]
                    },
                    {reply, ok, State#state{remotes = [NewR | RestR]}};
                false ->
                    case lists:keytake(Ref, #consumer.ref, R#remote.inactive_consumers) of
                        {value, C, RestC} ->
                            NewR = R#remote
                            {
                                inactive_consumers = RestC,
                                recover_unregistered_consumers = R#remote.recover_unregistered_consumers ++ [C]
                            },
                            {reply, ok, State#state{remotes = [NewR | RestR]}};
                        false ->
                            {reply, error_no_ref, State}
                    end
            end;
        false ->
            {reply, remote_not_registered, State}
    end;

handle_call(dumpq, _From, State = #state{qtab = QTab}) ->
    {reply, ets:tab2list(QTab), State};

handle_call(summarize, _From, State = #state{qtab = QTab}) ->
    Fun = fun({Seq, _NumItems, Bin, _Meta}, Acc) ->
        Obj = riak_repl_util:from_wire(Bin),
        {Key, Size} = summarize_object(Obj),
        Acc ++ [{Seq, Key, Size}]
    end,
    {reply, ets:foldl(Fun, [], QTab), State};

handle_call({evict, Seq}, _From, State = #state{qtab = QTab}) ->
    ets:delete(QTab, Seq),
    {reply, ok, State};
handle_call({evict, Seq, Key}, _From, State = #state{qtab = QTab}) ->
    case ets:lookup(QTab, Seq) of
        [{Seq, _, Bin, _}] ->
            Obj = riak_repl_util:from_wire(Bin),
            case Key =:= riak_object:key(Obj) of
                true ->
                    ets:delete(QTab, Seq),
                    {reply, ok, State};
                false ->
                    {reply, {wrong_key, Seq, Key}, State}
            end;
        _ ->
            {reply, {not_found, Seq}, State}
    end;

handle_call({pull_with_ack, Name, Ref}, _From, State) ->
    {reply, ok, pull(Name, Ref, State)};

% either old code or old node has sent us a old push, upvert it.
handle_call({push, NumItems, Bin}, From, State) ->
    handle_call({push, NumItems, Bin, []}, From, State);

% TODO what is this code for? there's no external interface for it...
handle_call({push, NumItems, Bin, Meta}, _From, State) ->
    State2 = maybe_flip_overload(State),
    {reply, ok, push(NumItems, Bin, Meta, State2)};

handle_call({ack_sync, Name, Seq, Ts}, _From, State) ->
    {reply, ok, ack_seq(Name, Seq, Ts, State)}.

% ye previous cast. rtq_proxy may send us an old pattern.
handle_cast({push, NumItems, Bin}, State) ->
    handle_cast({push, NumItems, Bin, []}, State);

handle_cast({push, _NumItems, _Bin, _Meta}, State=#state{remotes=[]}) ->
    {noreply, State};
handle_cast({push, NumItems, Bin, Meta}, State) ->
    State2 = maybe_flip_overload(State),
    {noreply, push(NumItems, Bin, Meta, State2)};

%% @private
handle_cast({report_drops, N}, State) ->
    QSeq = State#state.qseq + N,
    Drops = State#state.overload_drops + N,
    State2 = State#state{qseq = QSeq, overload_drops = Drops},
    State3 = maybe_flip_overload(State2),
    {noreply, State3};

handle_cast({pull, Name, Ref}, State) ->
     {noreply, pull(Name, Ref, State)};

handle_cast({ack, Name, Seq, Ts}, State) ->
       {noreply, ack_seq(Name, Seq, Ts, State)}.

record_remote_latency(Name, OldLastSeen, SeqNumber, NewTimestamp) ->
    case OldLastSeen of
        {SeqNumber, OldTimestamp} ->
            folsom_metrics:notify({{latency, Name}, abs(timer:now_diff(NewTimestamp, OldTimestamp))});
        _ ->
            % Don't log for a non-matching seq number
            skip
    end.

ack_seq(Name, Seq, NewTs, State = #state{qtab = QTab, remotes =  Rs}) ->
    case lists:keytake(Name, #remote.name, Rs) of
        false ->
            State;
        {value, R, Rest} ->
            %% record remote latency
            record_remote_latency(Name, R#remote.last_seen, Seq, NewTs),

            case Seq > R#remote.aseq of
                true ->
                    {NewState, NewR} = cleanup(R, QTab, Seq, R#remote.aseq, State),
                    NewState#state{remotes =  Rest ++ [NewR#remote{aseq = Seq}]};
                false ->
                    State
            end
    end.

%% @private
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(Reason, State=#state{remotes =  Rs}) ->
  lager:info("rtq terminating due to: ~p
  State: ~p", [Reason, State]),
    %% when started from tests, we may not be registered
    catch(erlang:unregister(?SERVER)),
    flush_pending_pushes(),
    lists:foreach(
        fun(R) ->
            AllC = R#remote.active_consumers ++ R#remote.inactive_consumers ++ R#remote.recover_unregistered_consumers,
            lists:foreach(fun(C) -> deliver_error(C#consumer.deliver_fun, {terminate, Reason}) end, AllC)
        end, Rs),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_flip_overload(State) ->
    #state{overloaded = Overloaded, overload = Overload, recover = Recover} = State,
    {message_queue_len, MsgQLen} = erlang:process_info(self(), message_queue_len),
    if
        Overloaded andalso MsgQLen =< Recover ->
            lager:info("Recovered from overloaded condition"),
            ets:insert(?overload_ets, {overloaded, false}),
            State#state{overloaded = false};
        (not Overloaded) andalso MsgQLen > Overload ->
            lager:warning("Realtime queue mailbox size of ~p is greater than ~p indicating overload; objects will be dropped until size is less than or equal to ~p", [MsgQLen, Overload, Recover]),
            % flip the rt_dirty flag on
            riak_repl_stats:rt_source_errors(),
            ets:insert(?overload_ets, {overloaded, true}),
            State#state{overloaded = true};
        true ->
            State
    end.

flush_pending_pushes() ->
    receive
        {'$gen_cast', {push, NumItems, Bin}} ->
            riak_repl2_rtq_proxy:push(NumItems, Bin),
            flush_pending_pushes()
    after
        1000 ->
            ok
    end.


unregister_q(Name, State = #state{qtab = QTab, remotes =  Rs}) ->
     case lists:keytake(Name, #remote.name, Rs) of
        {value, R, Rs2} ->
            %% Remove R from Rs, let any pending process know
            %% and clean up the queue
            AllC = R#remote.active_consumers ++ R#remote.inactive_consumers ++ R#remote.recover_unregistered_consumers,
            lists:foreach(fun(C) -> deliver_error(C#consumer.deliver_fun, {error, unregistered}) end, AllC),
            {NewState, _NewR} = cleanup(R, QTab, State#state.qseq, R#remote.aseq, State),
            {ok, NewState#state{remotes =  Rs2}};
        false ->
            {{error, not_registered}, State}
    end.


register_new_remote(_State, R, '$end_of_table') ->
    R;
register_new_remote(#state{qtab = QTab} = State, #remote{name = Name} = R, Seq) ->
    case ets:lookup(QTab, Seq) of
        [] ->
            %% entry removed
            register_new_remote(QTab, R, ets:next(QTab, Seq));
        [QEntry] ->
            {Seq, NumItems, Bin, Meta} = QEntry,
            Size = ets_obj_size(Bin, State),
            RoutedList = meta_get(routed_clusters, [], Meta),
            FilteredList = meta_get(filtered_clusters, [], Meta),
            AckedList = meta_get(acked_clusters, [], Meta),
            ShouldSkip = lists:member(Name, RoutedList),
            ShouldFilter =  riak_repl2_object_filter:realtime_filter(Name, Meta),
            AlreadyAcked = lists:member(Name, AckedList),
            {ShouldSend, {UpdatedMeta, NewMeta}} =
                register_new_remote_update_meta(Name, Meta, ShouldSkip, ShouldFilter, AlreadyAcked, FilteredList),
            case UpdatedMeta of
                true ->
                    ets:insert(QTab, {Seq, NumItems, Bin, NewMeta});
                false ->
                    ok
            end,
            NewR = case ShouldSend of
                       true ->
                           update_rq_size(R, Size);
                       false ->
                           R
                   end,
            register_new_remote(State, NewR, ets:next(QTab, Seq))
    end.

%% should skip, true
register_new_remote_update_meta(_Name, Meta, true, _, _, _FilteredList) ->
    {false, {false, Meta}};
%% should filter, true
register_new_remote_update_meta(Name, Meta, _, true, _, FilteredList) ->
    {false, {true, orddict:store(filtered_clusters, [Name | FilteredList], Meta)}};
%% already acked, true
register_new_remote_update_meta(_Name, Meta, _, _, true, _FilteredList) ->
    {false, {false, Meta}};
%% should send, true
register_new_remote_update_meta(_Name, Meta, false, false, false, _FilteredList) ->
    {true, {false, Meta}}.


allowed_filtered_remotes(Rs, Meta) ->
    AllRemotes = [Remote#remote.name || Remote <- Rs],
    Filtered = [RName || RName <- AllRemotes, riak_repl2_object_filter:realtime_filter(RName, Meta)],
    Allowed = AllRemotes -- Filtered,
    {Allowed, Filtered}.

meta_get(Key, Default, Meta) ->
    case orddict:find(Key, Meta) of
        error -> Default;
        {ok, Value} -> Value
    end.


push(NumItems, Bin, Meta,
    State = #state{qtab = QTab, qseq = QSeq, remotes =  Rs, shutting_down = false}) ->

    QSeq2 = QSeq + 1,
    QEntry = {QSeq2, NumItems, Bin, Meta},

    {AllowedRemoteNames, FilteredRemoteNames} = allowed_filtered_remotes(Rs, Meta),
    QEntry2 = set_local_forwards_meta(AllowedRemoteNames, QEntry),
    QEntry3 = set_filtered_clusters_meta(FilteredRemoteNames, QEntry2),


    %% we have to send to all remotes to update there state!
    DeliverAndRs2 = [maybe_deliver_item(R, QEntry3) || R <- Rs],
    {DeliverResults, Rs3} = lists:unzip(DeliverAndRs2),

    %% This has changed for 'filtered' to mimic the behaviour of 'skipped'.
    %% We do not want to add an object that all remotes will filter or skip to the queue
    AllSkippedFilteredAcked = lists:all(fun
                                         (skipped) -> true;
                                         (_) -> false
                                     end, DeliverResults),


    Routed = meta_get(routed_clusters, [], Meta),
    Acked = meta_get(acked_clusters, [], Meta),
    RemotesToUpdate = AllowedRemoteNames -- (Routed ++ Acked),
    State2 = State#state{remotes =  Rs3, qseq = QSeq2},
    State3 =
        case AllSkippedFilteredAcked of
            true ->
                State2;
            false ->
                ets:insert(QTab, QEntry3),
                Size = ets_obj_size(Bin, State2),
                NewState = update_q_size(State2, Size),
                update_remote_q_sizes(NewState, Size, RemotesToUpdate)
        end,
    trim_q(State3);
push(NumItems, Bin, Meta, State = #state{shutting_down = true}) ->
    riak_repl2_rtq_proxy:push(NumItems, Bin, Meta),
    State.

pull(Name, Ref, State = #state{qtab = QTab, qseq = QSeq, remotes =  Rs}) ->
     RsNames = [R#remote.name || R <- Rs],
     UpdRs = case lists:keytake(Name, #remote.name, Rs) of
                {value, R = #remote{active_consumers = AC}, Rs2} ->
                    case lists:keytake(Ref, #consumer.ref, R#remote.inactive_consumers) of
                        {value, C, Cs2} ->
                            NewR = R#remote{active_consumers = AC ++ [C], inactive_consumers = Cs2},
                            [maybe_pull(QTab, QSeq, NewR, RsNames) | Rs2];
                        false ->
                            lager:error("Consumer ~p pulled from RTQ, but was not registered", [Ref]),
                            _ = deliver_error(Ref, consumer_not_registered),
                            Rs
                    end;
                false ->
                    lager:error("Remote ~p pulled from RTQ, but was not registered", [Name]),
                    _ = deliver_error(Ref, remote_not_registered),
                    Rs
            end,
    State#state{remotes =  UpdRs}.


maybe_pull(QTab, QSeq, R = #remote{mode = normal, rseq = RSeq}, RsNames) ->
    RSeq2 = RSeq + 1,
    case RSeq2 =< QSeq of
        true -> % something reday
            case ets:lookup(QTab, RSeq2) of
                [] -> % entry removed, due to previously being unroutable
                    maybe_pull(QTab, QSeq, R#remote{rseq = RSeq2}, RsNames);
                [QEntry] ->
                    {RSeq2, _NumItems, _Bin, _Meta} = QEntry,
                    % if the item can't be delivered due to cascading rt, or filtering,
                    % just keep trying.
                    {Res, R2} = maybe_deliver_item(R, QEntry),
                    case Res == skipped of
                        true ->
                            maybe_pull(QTab, QSeq, R2, RsNames);
                        false ->
                            R2
                    end
            end;
        false ->
            R
    end.



maybe_deliver_item(R=#remote{mode = recover_consumer}, _QEntry) ->
    #remote{recover_unregistered_consumers = [DC | _], active_consumers = [C | Rest]} = R,
    {no_fun, NewR};



maybe_deliver_item(#remote{mode = normal, name = Name} = R, {_Seq, _NumItems, _Bin, Meta} = QEntry) ->
    Routed = meta_get(routed_clusters, [], Meta),
    Filtered = meta_get(filtered_clusters, [], Meta),
    Acked = meta_get(acked_clusters, [], Meta),
    
    IsRouted = lists:member(Name, Routed),
    IsFiltered = lists:member(Name, Filtered),
    IsAcked = lists:member(Name, Acked),
    ShouldSkip = IsRouted or IsFiltered or IsAcked,
    maybe_deliver_item(R, QEntry, ShouldSkip).

%% Should Skip = true
maybe_deliver_item(R = #remote{rseq = RSeq}, _QEntry, true) ->
    {skipped, R#remote{rseq = RSeq + 1}};

%% NotAcked, NotFiltered, NotRouted (Send)
maybe_deliver_item(#remote{active_consumers = []} = R, _QEntry, _) ->
    {no_fun, R};
maybe_deliver_item(R = #remote{active_consumers = [C|Rest]}, QEntry, _) ->
    {delivered, deliver_item(R#remote{active_consumers = Rest}, C, QEntry)}.


deliver_item(R, C, {Seq, NumItems, Bin, Meta}) ->

    #remote
    {
        inactive_consumers = INAC,
        recover_unregistered_consumers = RUC
    } = R,

    try
        Seq = R#remote.rseq + 1, % bit of paranoia, remove after EQC
        CSeq = C#consumer.cseq + 1,
        C2 = C#consumer{cseq = CSeq},
        QEntry2 = {CSeq, NumItems, Bin, Meta},
        QEntry3 = set_skip_meta(QEntry2),
        ok = C#consumer.deliver_fun(QEntry3),
        %% add sequence number too consumer ets table
        ets:insert(C#consumer.ctab, {CSeq, Seq}),
        case Seq rem app_helper:get_env(riak_repl, rtq_latency_interval, 1000) of
            0 ->
                R#remote{rseq = Seq, inactive_consumers = [C2|INAC], last_seen = {Seq, os:timestamp()}};
            _ ->
                R#remote{rseq = Seq, inactive_consumers = [C2|INAC]}
        end
    catch
        Type:Error ->
            lager:warning("did not deliver object back to rtsource_helper, Reason: {~p,~p}", [Type, Error]),
            lager:info("Seq: ~p   -> RSeq: ~p", [Seq, R#remote.rseq]),
            lager:info("remote: ~p" ,[R]),
            riak_repl_stats:rt_source_errors(),
            %% do not advance head so it will be delivered again
            %% TODO make function for dead consumers (find ets:first, set cseq to this! change the mode)
            R#remote{mode = recover_consumer, errs = R#remote.errs + 1, recover_unregistered_consumers = RUC ++ [C]}
    end.

%% Deliver an error if a delivery function is registered.
deliver_error(DeliverFun, Reason) when is_function(DeliverFun)->
    catch DeliverFun({error, Reason}),
    ok;
deliver_error(_NotAFun, _Reason) ->
    ok.

% if nothing has been delivered, the sink assumes nothing was skipped
% fulfill that expectation.
set_skip_meta(QEntry) ->
    set_meta(QEntry, skip_count, 0).

set_local_forwards_meta(LocalForwards, QEntry) ->
    set_meta(QEntry, local_forwards, LocalForwards).

set_filtered_clusters_meta(FilteredClusters, QEntry) ->
    set_meta(QEntry, filtered_clusters, FilteredClusters).

set_acked_clusters_meta(AckedClusters, QEntry) ->
    set_meta(QEntry, acked_clusters, AckedClusters).

set_meta({Seq, NumItems, Bin, Meta}, Key, Value) ->
    Meta2 = orddict:store(Key, Value, Meta),
    {Seq, NumItems, Bin, Meta2}.


cleanup(R, QTab, NewAck, OldAck, State) ->
    AllRemoteNames = [R1#remote.name || R1 <- State#state.remotes],
    queue_cleanup(R, AllRemoteNames, QTab, NewAck, OldAck, State).

queue_cleanup(R, _AllClusters, _QTab, '$end_of_table', _OldSeq, State) ->
    {State, R};
queue_cleanup(#remote{name = Name} = R, AllClusters, QTab, NewAck, OldAck, State) when NewAck > OldAck ->
    case ets:lookup(QTab, NewAck) of
        [] ->
            queue_cleanup(R, AllClusters, QTab, ets:prev(QTab, NewAck), OldAck, State);
        [{_, _, Bin, Meta} = QEntry] ->
            Routed = meta_get(routed_clusters, [], Meta),
            Filtered = meta_get(filtered_clusters, [], Meta),
            Acked = meta_get(acked_clusters, [], Meta),
            NewAcked = Acked ++ [Name],
            QEntry2 = set_acked_clusters_meta(NewAcked, QEntry),
            RoutedFilteredAcked = Routed ++ Filtered ++ NewAcked,
            ShrinkSize = ets_obj_size(Bin, State),
            NewR = update_rq_size(R, -ShrinkSize),
            State2 = case AllClusters -- RoutedFilteredAcked of
                         [] ->
                             ets:delete(QTab, NewAck),
                             update_q_size(State, -ShrinkSize);
                         _ ->
                             ets:insert(QTab, QEntry2),
                             State
                     end,
            queue_cleanup(NewR, AllClusters, QTab, ets:prev(QTab, NewAck), OldAck, State2);
        UnExpectedObj ->
            lager:warning("Unexpected object in RTQ, ~p", [UnExpectedObj]),
            queue_cleanup(R, AllClusters, QTab, ets:prev(QTab, NewAck), OldAck, State)
    end;
queue_cleanup(R, _AllClusters, _QTab, _NewAck, _OldAck, State) ->
    {State, R}.

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

update_q_size(State = #state{qsize_bytes = CurrentQSize}, Diff) ->
  State#state{qsize_bytes = CurrentQSize + Diff}.

update_remote_q_sizes(State = #state{remotes =  Rs}, Diff, AllowedNames) ->
    Rs2 = lists:map(
        fun(R) ->
            case lists:member(R#remote.name, AllowedNames) of
                true ->
                    update_rq_size(R, Diff);
                false ->
                    R
            end
        end, Rs),
    State#state{remotes =  Rs2}.


update_rq_size(R = #remote{remote_qbytes = QBytes}, Diff) ->
    R#remote{remote_qbytes = QBytes + Diff}.


%% Trim the queue if necessary
trim_q(State = #state{qtab = QTab}) ->
    State1 = trim_remotes_q(State, ets:first(QTab)),
    trim_global_q(get_queue_max_bytes(State1), State1).


remote_needs_trim(undefined, _) ->
    false;
remote_needs_trim(MaxBytes, #remote{remote_qbytes = RBytes}) ->
    RBytes > MaxBytes.

trim_remotes_q(State = #state{remotes =  Rs}, Seq) ->
    AllClusters = [R#remote.name || R <- Rs],
    TrimmedRs = [R || R <- Rs, not remote_needs_trim(get_remote_max_bytes(R), R)],
    case Rs -- TrimmedRs of
        [] ->
            State;
        NotTrimmedRs ->
            trim_remotes_q_entries(State, AllClusters, TrimmedRs, NotTrimmedRs, Seq)
    end.

trim_remotes_q_entries(State, _AllClusters, TrimmedRs, [], '$end_of_table') ->
    State#state{remotes =  TrimmedRs};
trim_remotes_q_entries(State, _AllClusters, TrimmedRs, [], _Seq) ->
    State#state{remotes =  TrimmedRs};
trim_remotes_q_entries(State, _AllClusters, TrimmedRs, NotTrimmedRs, '$end_of_table') ->
    lager:warning("rtq trim remote q, end of table with remotes needing trimming ~p", [NotTrimmedRs]),
    State#state{remotes =  TrimmedRs ++ NotTrimmedRs};
trim_remotes_q_entries(State = #state{qtab = QTab}, AllClusters, TrimmedRs, NotTrimmedRs, Seq) ->
    [{_, _, Bin, Meta} = QEntry] = ets:lookup(QTab, Seq),
    ShrinkSize = ets_obj_size(Bin, State),

    Routed = meta_get(routed_clusters, [], Meta),
    Filtered = meta_get(filtered_clusters, [], Meta),
    Acked = meta_get(acked_clusters, [], Meta),
    TooAckClusters = AllClusters -- (Routed ++ Filtered ++ Acked),

    NotTrimmedRNames = [R#remote.name || R <- NotTrimmedRs],
    RemotesToBeTrimmed = [R || R <- NotTrimmedRs, lists:member(R#remote.name, TooAckClusters)],
    RemotesNotToBeTrimmed = NotTrimmedRs -- RemotesToBeTrimmed,
    {NewState, NewRemotes} =
        case TooAckClusters -- NotTrimmedRNames of
            %% The remotes to be trimmed will cause the object to no longer be used by any remote
            %% delete the object in this case
            [] ->
                State1 = update_q_size(State, -ShrinkSize),
                ets:delete(QTab, Seq),
                {State1, [trim_single_remote_q_entry(R, ShrinkSize, Seq) || R <- RemotesToBeTrimmed]};

            %% The object is only relevant to a subset of all remotes
            %% only remove and update the correct remotes
            _ ->
                %% we need to update the object in ets to add these remotes to the ack'd list!
                QEntry2 = set_acked_clusters_meta(Acked ++ [R#remote.name || R <- RemotesToBeTrimmed], QEntry),
                ets:insert(QTab, QEntry2),
                {State, [trim_single_remote_q_entry(R, ShrinkSize, Seq) || R <- RemotesToBeTrimmed] ++ RemotesNotToBeTrimmed}
        end,
    trim_remotes_q(NewState#state{remotes =  TrimmedRs ++ NewRemotes}, ets:next(QTab, Seq)).


trim_single_remote_q_entry(R = #remote{rseq = RSeq}, ShrinkSize, TrimSeq) ->
    R1 = update_rq_size(R, -ShrinkSize),
    case RSeq < TrimSeq of
        true ->
            %% count the drop and increase the rseq and aseq to the new trimseq value
            R1#remote{drops = R#remote.drops + 1, r_drops = R#remote.r_drops + 1, rseq = TrimSeq, aseq = TrimSeq};
        _ ->
            R1
    end.



trim_global_q(undefined, State) ->
    State;
trim_global_q(MaxBytes, State = #state{qtab = QTab, qseq = QSeq}) ->
    case qbytes(QTab, State) > MaxBytes of
        true ->
            {Rs2, NewState} = trim_global_q_entries(QTab, MaxBytes, State#state.remotes,
                                             State),

            %% Adjust the last sequence handed out number
            %% so that the next pull will retrieve the new minseq
            %% number.  If that increases a remotes rseq,
            %% reset the aseq too.  The drops have already been
            %% accounted for.
            NewRSeq = case ets:first(QTab) of
                          '$end_of_table' ->
                              QSeq; % if empty, make sure pull waits
                          MinSeq ->
                              MinSeq - 1
                      end,
            Rs3 = [case RSeq < NewRSeq of
                       true ->
                           R#remote{rseq = NewRSeq, aseq = NewRSeq};
                       _ ->
                           R
                   end || R = #remote{rseq = RSeq} <- Rs2],
            NewState#state{remotes = Rs3};
        false -> % Q size is less than MaxBytes words
            State
    end.

trim_global_q_entries(QTab, MaxBytes, Rs, State) ->
    {Rs2, State2, Entries, Objects} = trim_global_q_entries(QTab, MaxBytes, Rs, State, 0, 0),
    if
        Entries + Objects > 0 ->
            lager:debug("Dropped ~p objects in ~p entries due to reaching maximum queue size of ~p bytes", [Objects, Entries, MaxBytes]);
        true ->
            ok
    end,
    {Rs2, State2}.

trim_global_q_entries(QTab, MaxBytes, Rs, State, Entries, Objects) ->
    case ets:first(QTab) of
        '$end_of_table' ->
            {Rs, State, Entries, Objects};
        TrimSeq ->
            [{_, NumObjects, Bin, Meta}] = ets:lookup(QTab, TrimSeq),
            ShrinkSize = ets_obj_size(Bin, State),
            NewState1 = update_q_size(State, -ShrinkSize),

            Routed = meta_get(routed_clusters, [], Meta),
            Filtered = meta_get(filtered_clusters, [], Meta),
            Acked = meta_get(acked_clusters, [], Meta),
            TooAckClusters = [R#remote.name || R <- Rs] -- (Routed ++ Filtered ++ Acked),
            NewState2 = update_remote_q_sizes(NewState1, -ShrinkSize, TooAckClusters),
            ets:delete(QTab, TrimSeq),
            Rs2 = [case RSeq < TrimSeq of
                       true ->
                           %% If the last sent qentry is before the trimseq
                           %% it will never be sent, so count it as a drop.
                           R#remote{drops = R#remote.drops + 1, q_drops = R#remote.q_drops +1};
                       _ ->
                           R
                   end || R = #remote{rseq = RSeq} <- Rs],
            %% Rinse and repeat until meet the target or the queue is empty
            case qbytes(QTab, NewState2) > MaxBytes of
                true ->
                    trim_global_q_entries(QTab, MaxBytes, Rs2, NewState2, Entries + 1, Objects + NumObjects);
                _ ->
                    {Rs2, NewState2, Entries + 1, Objects + NumObjects}
            end
    end.


-ifdef(TEST).
qbytes(_QTab, #state{qsize_bytes = QSizeBytes}) ->
    %% when EQC testing, don't account for ETS overhead
    QSizeBytes.

get_queue_max_bytes(#state{default_max_bytes = Default}) ->
    Default.

get_remote_max_bytes(_) ->
    undefined.

-else.
qbytes(QTab, #state{qsize_bytes = QSizeBytes, word_size=WordSize}) ->
    Words = ets:info(QTab, memory),
    (Words * WordSize) + QSizeBytes.

get_queue_max_bytes(#state{default_max_bytes = Default}) ->
    case riak_core_metadata:get(?RIAK_REPL2_RTQ_CONFIG_KEY, queue_max_bytes) of
        undefined -> Default;
        MaxBytes -> MaxBytes
    end.

get_remote_max_bytes(#remote{name = Name}) ->
    riak_core_metadata:get(?RIAK_REPL2_RTQ_CONFIG_KEY, {remote_max_bytes, Name}).
-endif.

is_queue_empty(Name, Rs) ->
    case lists:keytake(Name, #remote.name, Rs) of
        {value,  #remote{remote_qbytes = RBytes}, _Rs2} ->
            case RBytes == 0 of
                true -> false;
                false -> true
            end;
        false -> lager:error("Unknown queue")
    end.


%% Find the first sequence number
minseq(QTab, QSeq) ->
    case ets:first(QTab) of
        '$end_of_table' ->
            QSeq;
        MinSeq ->
            MinSeq - 1
    end.

summarize_object(Obj) ->
  ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
  {riak_object:key(Obj), riak_object:approximate_size(ObjFmt, Obj)}.
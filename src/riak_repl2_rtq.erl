%% @doc Queue module for realtime replication.
-module(riak_repl2_rtq).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export(
[
    name/1,
    start_link/1,
    start_link/2,
    register/2,
    unregister/2,
    push/5,
    push/4,
    push/3,
    ack/3,
    status/1,
    shutdown/1,
    stop/1,
    is_running/1,


    drain_queue/1,
    is_empty/1,

    summarize/1,
    dumpq/1,
    evict/2,
    evict/3,
    ack_sync/3
]).

% private api
-export([report_drops/2]).
-export([start_test/0]).


-define(DEFAULT_OVERLOAD, 2000).
-define(DEFAULT_RECOVER, 1000).
-define(DEFAULT_RTQ_LATENCY_SLIDING_WINDOW, 300).
-define(DEFAULT_MAX_BYTES, 104857600). %% 100 MB

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state,
{
    id,
    qtab = ets:new(?MODULE, [protected, ordered_set, {read_concurrency, true}]), % ETS table
    qseq = 0,  % Last sequence number handed out
    overload = ?DEFAULT_OVERLOAD :: pos_integer(), % if the message q exceeds this, the rtq is overloaded
    recover = ?DEFAULT_RECOVER :: pos_integer(), % if the rtq is in overload mode, it does not recover until =<
    overloaded = false :: boolean(),
    overload_drops = 0 :: non_neg_integer(),
    shutting_down=false,
    qsize_bytes = 0,
    word_size=erlang:system_info(wordsize),
    remotes = [],
    all_remote_names = []
}).

-record(remote,
{
    name,
    total_drops = 0,
    rsize_bytes = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

name(Id) ->
    IdBin = integer_to_binary(Id),
    binary_to_atom(<<"riak_repl2_rtq_", IdBin/binary>>, latin1).

overload_ets_name(Id) ->
    IdBin = integer_to_binary(Id),
    binary_to_atom(<<"rtq_overload_ets_", IdBin/binary>>, latin1).


start_link(Id) ->
    Overload = app_helper:get_env(riak_repl, rtq_overload_threshold, ?DEFAULT_OVERLOAD),
    Recover = app_helper:get_env(riak_repl, rtq_overload_recover, ?DEFAULT_RECOVER),
    Opts = [{overload_threshold, Overload}, {overload_recover, Recover}],
    start_link(Id, Opts).
start_link(Id, Options) ->
    ETS = overload_ets_name(Id),
    case ets:info(ETS) of
        undefined ->
            ETS = ets:new(ETS, [named_table, public, {read_concurrency, true}]),
            ets:insert(ETS, {overloaded, false});
        _ ->
            ok
    end,
    gen_server:start_link({local, name(Id)}, ?MODULE, [Id, Options], []).

start_test() ->
    gen_server:start(?MODULE, [], []).

register(Id, Name) ->
    gen_server:call(name(Id), {register, Name}, infinity).

unregister(Id, Name) ->
    gen_server:call(name(Id), {unregister, Name}, infinity).

status(Id) ->
    gen_server:call(name(Id), status, infinity).

shutdown(Id) ->
    gen_server:call(name(Id), shutting_down, infinity).

stop(Id) ->
    gen_server:call(name(Id), stop, infinity).

is_running(Id) ->
    gen_server:call(name(Id), is_running, infinity).

%% TODO: deal with repl migration stuff
push(Id, NumItems, Bin) ->
    push(Id, NumItems, Bin, []).
push(Id, NumItems, Bin, Meta) ->
    push(Id, NumItems, Bin, Meta, []).
push(Id, NumItems, Bin, Meta, PreCompleted) ->
    ETS = overload_ets_name(Id),
    case ets:lookup(ETS, overloaded) of
        [{overloaded, true}] ->
            lager:debug("rtq overloaded"),
            riak_repl2_rtq_overload_counter:drop(Id);
        [{overloaded, false}] ->
            gen_server:cast(name(Id), {push, NumItems, Bin, Meta, PreCompleted})
    end.

ack(Id, Name, Seq) ->
    gen_server:cast(name(Id), {ack, Name, Seq}).

report_drops(Id, N) ->
    gen_server:cast(name(Id), {report_drops, N}).

drain_queue(Id) ->
    gen_server:call(name(Id), drain_queue).

is_empty(Id) ->
    gen_server:call(name(Id), is_empty, infinity).

%%%========================================================================
%%% Backward Compatability Functions (will eventually be removed/ changed)
%%%========================================================================
summarize(Id) ->
    gen_server:call(name(Id), summarize, infinity).

dumpq(Id) ->
    gen_server:call(name(Id), dumpq, infinity).

evict(Id, Seq) ->
    gen_server:call(name(Id), {evict, Seq}, infinity).

evict(Id, Seq, Key) ->
    gen_server:call(name(Id), {evict, Seq, Key}, infinity).

ack_sync(Id, Name, Seq) ->
    gen_server:call(name(Id), {ack_sync, Name, Seq}, infinity).


%%%===================================================================
%%% gen_server calls
%%%===================================================================
init([Id, Options]) ->
    Overloaded = proplists:get_value(overload_threshold, Options, ?DEFAULT_OVERLOAD),
    Recover = proplists:get_value(overload_recover, Options, ?DEFAULT_RECOVER),
    {ok, #state{id = Id, overload = Overloaded, recover = Recover}}. % lots of initialization done by defaults


handle_call({register, Name}, _From, State = #state{qtab = QTab, qseq = QSeq}) ->
    NewState = register_remote(Name, State),
    {reply, {QSeq, QTab}, NewState};

handle_call({unregister, Name}, _From, State) ->
    {Reply, NewState} =  unregister_q(Name, State),
    {reply, Reply, NewState};

handle_call(status, _From, State) ->
    Status = make_status(State),
    {reply, Status, State};

handle_call(shutting_down, _From, State = #state{shutting_down=false}) ->
    %% this will allow the realtime repl hook to determine if it should send
    %% to another host
    _ = riak_repl2_rtq_proxy:start(),
    {reply, ok, State#state{shutting_down = true}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(is_running, _From, State = #state{shutting_down = ShuttingDown}) ->
    {reply, not ShuttingDown, State};

handle_call({is_empty, Name}, _From, State = #state{remotes = Remotes}) ->
    Result = is_queue_empty(Name, Remotes),
    {reply, Result, State};

handle_call(all_queues_empty, _From, State = #state{remotes = Remotes}) ->
    Result = lists:all(fun (#remote{name = Name}) -> is_queue_empty(Name, Remotes) end, Remotes),
    {reply, Result, State};


%%%=====================================================================================================================
%% Calls: Backward Compatibility
%%%=====================================================================================================================
handle_call({push, NumItems, Bin}, From, State) ->
    handle_call({push, NumItems, Bin, [], []}, From, State);
handle_call({push, NumItems, Bin, Meta, []}, From, State) ->
    handle_call({push, NumItems, Bin, Meta, []}, From, State);
handle_call({push, NumItems, Bin, Meta, Completed}, _From, State) ->
    State2 = maybe_flip_overload(State),
    {reply, ok, do_push(NumItems, Bin, Meta, Completed, State2)};

handle_call(summarize, _From, State = #state{qtab = QTab}) ->
    Fun = fun({Seq, _NumItems, Bin, _Meta, _Completed}, Acc) ->
        Obj = riak_repl_util:from_wire(Bin),
        {Key, Size} = summarize_object(Obj),
        Acc ++ [{Seq, Key, Size}]
          end,
    {reply, ets:foldl(Fun, [], QTab), State};

handle_call(dumpq, _From, State = #state{qtab = QTab}) ->
    {reply, ets:tab2list(QTab), State};

handle_call({evict, Seq}, _From, State = #state{qtab = QTab}) ->
    ets:delete(QTab, Seq),
    {reply, ok, State};
handle_call({evict, Seq, Key}, _From, State = #state{qtab = QTab}) ->
    case ets:lookup(QTab, Seq) of
        [{Seq, _, Bin, _, _}] ->
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

handle_call(drain_queue, _From, State = #state{shutting_down = true}) ->
    Reply = do_drain_queue(State),
    {reply, Reply, State};
handle_call(drain_queue, _From, State = #state{shutting_down = false}) ->
    {reply, {error, queue_not_shutting_down}, State};

handle_call({ack_sync, Name, Seq}, _From, State) ->
    {reply, ok, ack_seq(Name, Seq, State)};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.
%%%=====================================================================================================================

%%%===================================================================
%%% gen_server casts
%%%===================================================================
handle_cast({push, _NumItems, _Bin, _Meta, _Completed}, State=#state{remotes=[]}) ->
    {noreply, State};
handle_cast({push, NumItems, Bin, Meta, PreCompleted}, State) ->
    State2 = maybe_flip_overload(State),
    {noreply, do_push(NumItems, Bin, Meta, PreCompleted, State2)};

handle_cast({ack, Name, Seq}, State) ->
       {noreply, ack_seq(Name, Seq, State)};

handle_cast({report_drops, N}, State) ->
    QSeq = State#state.qseq + N,
    Drops = State#state.overload_drops + N,
    State2 = State#state{qseq = QSeq, overload_drops = Drops},
    State3 = maybe_flip_overload(State2),
    {noreply, State3};

%%%=====================================================================================================================
%% Casts: Backward Compatibility
%%%=====================================================================================================================
% have to have backward compatability for cluster upgrades
handle_cast({push, NumItems, Bin}, State) ->
    handle_cast({push, NumItems, Bin, [], []}, State);
handle_cast({push, NumItems, Bin, Meta}, State) ->
    handle_cast({push, NumItems, Bin, Meta, []}, State);

handle_cast(_Msg, State) ->
    {noreply, State}.
%%%=====================================================================================================================
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(Reason, State = #state{id = N}) ->
    lager:info("rtq terminating due to: ~p State: ~p", [Reason, State]),
    catch(erlang:unregister(name(N))),
    flush_pending_pushes().

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%% ================================================================================================================== %%

%% Internal Functions For Gen Server Calls

%% ================================================================================================================== %%
%% Register
%% ================================================================================================================== %%
register_remote(Name, State = #state{remotes = Remotes, all_remote_names = AllRemoteNames}) ->
    UpdatedRemotes =
        case lists:keytake(Name, #remote.name, Remotes) of
            {value, #remote{name = Name}, _} ->
                %% rtsource_rtq has re-registered
                Remotes;
            false ->
                %% New registration, start from the beginning
                [#remote{name = Name} | Remotes]
        end,
    UpdatedAllRemoteNames =
        case lists:member(Name, AllRemoteNames) of
            true -> AllRemoteNames;
            false -> [Name | AllRemoteNames]
        end,
    State#state{remotes = UpdatedRemotes, all_remote_names = UpdatedAllRemoteNames}.


%% ================================================================================================================== %%
%% Unregister
%% ================================================================================================================== %%
unregister_q(Name, State = #state{remotes = Remotes, all_remote_names = AllRemoteNames, qtab = QTab}) ->
    NewAllRemoteNames = AllRemoteNames -- [Name],
    case lists:keytake(Name, #remote.name, Remotes) of
        {value, Remote, Remotes2} ->
            MinSeq = ets:first(QTab),
            NewState = unregister_cleanup(Remote, MinSeq, State#state{remotes = Remotes2, all_remote_names = NewAllRemoteNames}),
            {ok, NewState};
        false ->
            {{error, not_registered}, State}
    end.

%% we have to iterate the entire queue and check if we need to delete any objects
unregister_cleanup('$end_of_table', _Remote, State) ->
    State;
unregister_cleanup(Seq, Remote, State = #state{qtab = QTab, all_remote_names = AllRemoteNames}) ->
    case ets:lookup(QTab, Seq) of
        [{_, _, Bin, _, Completed}] ->
            ShrinkSize = ets_obj_size(Bin, State),
            case AllRemoteNames -- Completed of
                [] ->
                    ets:delete(QTab, Seq),
                    NewState = update_queue_size(State, -ShrinkSize),
                    unregister_cleanup(ets:next(QTab, Seq), Remote, NewState);
                _ ->
                    unregister_cleanup(ets:next(QTab, Seq), Remote, State)
            end;
        _ ->
            unregister_cleanup(ets:next(QTab, Seq), Remote, State)
    end.


%% ================================================================================================================== %%
%% Status
%% ================================================================================================================== %%
make_status(State = #state{qtab = QTab, remotes = Remotes, id = Id}) ->
    MaxBytes = get_queue_max_bytes(),
    RemoteStats =
        lists:foldl(
            fun(Remote, Acc) ->
                #remote{name = Name, total_drops = Drops, rsize_bytes = RSize} = Remote,
                Stats = [{bytes, RSize}, {max_bytes, get_remote_max_bytes(Remote)}, {drops, Drops}],
                RefStats = riak_repl2_reference_rtq:status(Id, Name),
                [{Name, Stats ++ RefStats} | Acc]
            end, [], Remotes),
    [{bytes, qbytes(QTab, State)},
    {max_bytes, MaxBytes},
    {remotes, RemoteStats},
    {overload_drops, State#state.overload_drops}].

%% ================================================================================================================== %%

%% Internal Functions For Gen Server Casts

%% ================================================================================================================== %%
%% Push
%% ================================================================================================================== %%
do_push(NumItems, Bin, Meta, PreCompleted, State = #state{shutting_down = false}) ->
    #state
    {
        id = Id,
        qtab = QTab,
        qseq = QSeq,
        all_remote_names = AllRemoteNames,
        shutting_down = false
    } = State,
    case filter_remotes(AllRemoteNames, PreCompleted, Meta) of
        {[], _} ->
            %% We have no remotes to send too, drop the object (do not insert)
            State;
        {Send, Completed} ->
            % create object to place into queue
            QSeq2 = QSeq + 1,
            Meta1 = set_local_forwards_meta(Send, Meta),
            Meta2 = set_skip_meta(Meta1),
            QEntry = {QSeq2, NumItems, Bin, Meta2, Completed},
            State1 = State#state{qseq = QSeq2},

            %% insert object into queue
            ets:insert(QTab, QEntry),

            %% update queue and remote sizes
            Size = ets_obj_size(Bin, State1),
            State2 = update_queue_size(State1, Size),
            State3 = update_remotes_queue_size(State2, Size, Send),

            %% push to reference queues
            push_to_remotes(Send, Id, QEntry),

            %% (trim consumers) find out if consumers have reach maximum capacity
            State4 = maybe_trim_remote_queues(State3),

            %% (trim queue) find out if queue reached maximum capacity
            maybe_trim_queue(State4)
    end;
do_push(NumItems, Bin, Meta, PreCompleted, State = #state{shutting_down = true}) ->
    riak_repl2_rtq_proxy:push(NumItems, Bin, Meta, PreCompleted),
    State.



%% ================================================================================================================== %%
%% Push Helper Functions
%% ================================================================================================================== %%
filter_remotes(AllRemoteNames, PreCompleted, Meta) ->
    Routed = meta_get(routed_clusters, [], Meta),
    Filtered = riak_repl2_object_filter:realtime_blacklist(Meta),
    Completed = lists:usort(PreCompleted ++ Routed ++ Filtered),
    Send = AllRemoteNames -- Completed,
    {Send, Completed}.

meta_get(Key, Default, Meta) ->
    case orddict:find(Key, Meta) of
        error -> Default;
        {ok, Value} -> Value
    end.

set_local_forwards_meta(LocalForwards, Meta) ->
    orddict:store(local_forwards, LocalForwards, Meta).

set_skip_meta(Meta) ->
    orddict:store(skip_count, 0, Meta).

push_to_remotes([], _Id, _QEntry) ->
    ok;
push_to_remotes([RemoteName | Rest], Id, QEntry) ->
    %% TODO try catch this? (its a cast)
    riak_repl2_reference_rtq:push(RemoteName, Id, QEntry),
    push_to_remotes(Rest, Id, QEntry).

%% ==================================================== %%
%% Trimming Remote Queues
%% ==================================================== %%
maybe_trim_remote_queues(State = #state{remotes = Remotes, qtab = QTab}) ->
    case remotes_needs_trim(Remotes, [], []) of
        ok ->
            State;
        {trim, RemotesToTrim, OkRemotes} ->
            trim_remote_queues(RemotesToTrim, ets:first(QTab), State#state{remotes = OkRemotes})
    end.

remotes_needs_trim([], [], _) ->
    ok;
remotes_needs_trim([], RemotesToTrim, OkRemotes) ->
    {trim, RemotesToTrim, OkRemotes};
remotes_needs_trim([Remote | Rest], RemotesToTrim, OkRemotes) ->
    case remote_need_trim(Remote) of
        true -> remotes_needs_trim(Rest, [Remote|RemotesToTrim], OkRemotes);
        false -> remotes_needs_trim(Rest, RemotesToTrim, [Remote | OkRemotes])
    end.

remote_need_trim(Remote = #remote{rsize_bytes = RBytes}) ->
    RBytes > get_remote_max_bytes(Remote).


trim_remote_queues([], '$end_of_table', State) ->
    State;
trim_remote_queues(Remotes, '$end_of_table', State = #state{remotes = OkRemotes}) ->
    lager:error("Remotes requires trimming but we reached the end of table ~p", [Remotes]),
    State#state{remotes = Remotes ++ OkRemotes};
trim_remote_queues([], _, State) ->
    State;
trim_remote_queues(Remotes, Seq, State = #state{remotes = OkRemotes, qtab = QTab, all_remote_names = AllRemoteNames}) ->
    case ets:lookup(QTab, Seq) of
        [] ->
            trim_remote_queues(Remotes, ets:next(QTab, Seq), State);
        [{_, _, Bin, _, PreCompleted}] ->
            ShrinkSize = ets_obj_size(Bin, State),
            {NewCompleted, NewRemotes, NewOkRemotes} = maybe_trim_remotes_single_entry(Remotes, PreCompleted, ShrinkSize),
            case AllRemoteNames -- NewCompleted of
                [] ->
                    %% delete queue entry
                    ets:delete(QTab, Seq);
                _ ->
                    %% update queue entry
                    %% TODO: this will not get rid of the sequence number in the reference queue!?? This might be okay
                    ets:update_element(QTab, Seq, {5, NewCompleted})

            end,
            %% continue trimming remotes
            trim_remote_queues(NewRemotes, ets:next(QTab, Seq), State#state{remotes = OkRemotes ++ NewOkRemotes})
    end.

maybe_trim_remotes_single_entry(Remotes, PreCompleted, ShrinkSize) ->
    lists:foldl(
        fun(Remote, {Completed, TrimRemotes, OkRemotes}) ->
            case maybe_trim_remote_single_entry(Completed, Remote, ShrinkSize) of
                {ok, NewCompleted, NewRemote} ->
                    {NewCompleted, TrimRemotes, [NewRemote | OkRemotes]};
                {trim, NewCompleted, NewRemote} ->
                    {NewCompleted, [NewRemote | TrimRemotes], OkRemotes}
            end
        end, {PreCompleted, [], []}, Remotes).

maybe_trim_remote_single_entry(Remote, Completed, ShrinkSize) ->
    case lists:member(Remote#remote.name, Completed) of
        true ->
            {trim, Completed, Remote};
        false ->
            trim_remote_single_entry(Completed, Remote, ShrinkSize)
    end.

trim_remote_single_entry(Completed, Remote, ShrinkSize) ->
    Remote1 = update_remote_queue_size(Remote, -ShrinkSize),
    Remote2 = update_remote_total_drops(Remote1, 1),
    Completed1 = Completed -- [Remote#remote.name],
    case remote_need_trim(Remote2) of
        true ->
            {trim, Completed1, Remote2};
        _ ->
            {ok, Completed1, Remote2}
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



trim_single_queue_entry(Seq, State = #state{qtab = QTab, all_remote_names = AllRemoteNames}) ->
    case ets:lookup(QTab, Seq) of
        [{_, _, Bin, _, Completed}] ->
            ShrinkSize = ets_obj_size(Bin, State),
            State1 = update_queue_size(State, -ShrinkSize),
            State2 = update_remotes_queue_size(State1, -ShrinkSize, AllRemoteNames -- Completed),
            State3 = update_remotes_total_drops(State2, 1, AllRemoteNames -- Completed),
            ets:delete(QTab, Seq),
            State3;
        _ ->
            State

    end.

%% ================================================================================================================== %%
%% Acking the queue. Either adds to a remote to the 'Completed' list, or deletes the object.
%% ================================================================================================================== %%
ack_seq(RemoteName, Seq, State) ->
    #state{qtab = QTab, all_remote_names = AllRemoteNames, remotes = Remotes} = State,
    case ets:lookup(QTab, Seq) of
        [] ->
            %% TODO:
            %% the queue has been trimmed due to reaching its maximum size
            %% but this has been sent and acked! - so we can reduce the dropped counter
            State;
        [{Seq, _, Bin, _, Completed}] ->
            case lists:keytake(RemoteName, #remote.name, Remotes) of
                {value, Remote, Remotes2}  ->
                    NewCompleted = [RemoteName | Completed],
                    ShrinkSize = ets_obj_size(Bin, State),
                    NewRemote = update_remote_queue_size(Remote, -ShrinkSize),
                    NewState = State#state{remotes = [NewRemote | Remotes2]},
                    case AllRemoteNames -- NewCompleted of
                        [] ->
                            ets:delete(QTab, Seq),
                            update_queue_size(NewState, -ShrinkSize);
                        _ ->
                            ets:update_element(QTab, Seq, {5, NewCompleted}),
                            NewState
                    end;
                false ->
                    lager:error("Ack received for a remote that is not registered"),
                    State
            end;
        UnExpectedObj ->
            lager:warning("Unexpected object in RTQ, ~p", [UnExpectedObj]),
            State
    end.

%% ================================================================================================================== %%
%% Drain Queue
%% ================================================================================================================== %%
do_drain_queue(#state{qtab = QTab}) ->
    case ets:last(QTab) of
        'end_of_table' ->
            empty;
        Seq ->
            [QEntry] = ets:lookup(QTab, Seq),
            ets:delete(QTab, Seq),
            QEntry
    end.


%% ================================================================================================================== %%

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% ================================================================================================================== %%
%% Maybe flip overload used to ensure that the rtq mailbox does not exceed a configured size.
%% ================================================================================================================== %%
maybe_flip_overload(State = #state{id = Id}) ->
    #state{overloaded = Overloaded, overload = Overload, recover = Recover} = State,
    {message_queue_len, MsgQLen} = erlang:process_info(self(), message_queue_len),
    ETS = overload_ets_name(Id),
    if
        Overloaded andalso MsgQLen =< Recover ->
            lager:info("Recovered from overloaded condition"),
            ets:insert(ETS, {overloaded, false}),
            State#state{overloaded = false};
        (not Overloaded) andalso MsgQLen > Overload ->
            lager:warning("Realtime queue mailbox size of ~p is greater than ~p indicating overload; objects will be dropped until size is less than or equal to ~p", [MsgQLen, Overload, Recover]),
            % flip the rt_dirty flag on
            riak_repl_stats:rt_source_errors(),
            ets:insert(ETS, {overloaded, true}),
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

update_remotes_queue_size(State = #state{remotes = Remotes}, Diff, RemoteNames) ->
    UpdatedRemotes = lists:map(
        fun(Remote) ->
            case lists:member(Remote#remote.name, RemoteNames) of
                true ->
                    update_remote_queue_size(Remote, Diff);
                false ->
                    Remote
            end
        end, Remotes),
    State#state{remotes = UpdatedRemotes}.
update_remote_queue_size(Remote = #remote{rsize_bytes = RBytes}, Diff) ->
    Remote#remote{rsize_bytes = RBytes + Diff}.



update_remotes_total_drops(State = #state{remotes = Remotes}, DropCounter, RemoteNames) ->
    UpdatedRemotes = lists:map(
        fun(Remote) ->
            case lists:member(Remote#remote.name, RemoteNames) of
                true ->
                    update_remote_total_drops(Remote, DropCounter);
                false ->
                    Remote
            end
        end, Remotes),
    State#state{remotes = UpdatedRemotes}.

update_remote_total_drops(Remote = #remote{total_drops = Drops}, DropCounter) ->
    Remote#remote{total_drops = Drops + DropCounter}.



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
    app_helper:get_env(riak_repl, default_queue_max_bytes, ?DEFAULT_MAX_BYTES).

get_remote_max_bytes(_) ->
    app_helper:get_env(riak_repl, default_consumer_max_bytes, ?DEFAULT_MAX_BYTES).


-else.
qbytes(QTab, #state{qsize_bytes = QSizeBytes, word_size=WordSize}) ->
    Words = ets:info(QTab, memory),
    (Words * WordSize) + QSizeBytes.

%% ------------------------------------------------------------------------------------------------------------------ %%
%%                                      Queue Max Bytes
%% ------------------------------------------------------------------------------------------------------------------ %%
get_queue_max_bytes() ->
    case get_queue_max_bytes_node() of
        undefined -> get_queue_max_bytes_cluster();
        N -> N
    end.

get_queue_max_bytes_node() ->
    case app_helper:get_env(riak_repl, default_queue_max_bytes) of
        N when is_integer(N) -> N;
        _ -> undefined
    end.

get_queue_max_bytes_cluster() ->
    case riak_core_metadata:get(?RIAK_REPL2_CONFIG_KEY, queue_max_bytes) of
        undefined -> ?DEFAULT_MAX_BYTES;
        MaxBytes -> MaxBytes
    end.

%% ------------------------------------------------------------------------------------------------------------------ %%
%%                                      Consumer Max Bytes
%% ------------------------------------------------------------------------------------------------------------------ %%
get_remote_max_bytes(Remote) ->
    case get_remote_max_bytes_node() of
        undefined -> get_remote_max_bytes_cluster(Remote);
        N -> N
    end.

get_remote_max_bytes_node() ->
    case app_helper:get_env(riak_repl, default_consumer_max_bytes) of
        N when is_integer(N) -> N;
        _ -> undefined
    end.

get_remote_max_bytes_cluster(#remote{name = Name}) ->
    case riak_core_metadata:get(?RIAK_REPL2_CONFIG_KEY, {consumer_max_bytes, Name}) of
        undefined -> ?DEFAULT_MAX_BYTES;
        MaxBytes -> MaxBytes
    end.

-endif.


%% ================================================================================================================== %%
%% Backward Compatibility Functions
%% ================================================================================================================== %%

summarize_object(Obj) ->
    ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
    {riak_object:key(Obj), riak_object:approximate_size(ObjFmt, Obj)}.

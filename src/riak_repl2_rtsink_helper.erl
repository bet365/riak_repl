%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.

%% @doc Realtime replication sink module
%%
%% High level responsibility...
%%  consider moving out socket responsibilities to another process
%%  to keep this one responsive (but it would pretty much just do status)
%%
-module(riak_repl2_rtsink_helper).
-include("riak_repl.hrl").

%% API
-export([start_link/1,
         stop/1,
         write_objects_v3/4,
         write_objects_v4/5]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-behavior(gen_server).

-record(state, {parent           %% Parent process
               }).

start_link(Parent) ->
    gen_server:start_link(?MODULE, [Parent], []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

write_objects_v3(Pid, BinObjs, DoneFun, WireVersion) ->
    gen_server:cast(Pid, {write_objects_v3, BinObjs, DoneFun, WireVersion}).

write_objects_v4(Pid, BinObjs, Meta, AckPid, WireVersion) ->
    gen_server:cast(Pid, {write_objects_v4, BinObjs, Meta, AckPid, WireVersion}).

%% Callbacks
init([Parent]) ->
    {ok, #state{parent = Parent}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({write_objects, BinObjs, DoneFun, Ver}, State) ->
    do_write_objects(BinObjs, DoneFun, Ver),
    {noreply, State};

handle_cast({write_objects_v4, BinObjs, RtSinkPid, Ref, Ver}, State) ->
    do_write_objects_v4(BinObjs, RtSinkPid, Ref, Ver),
    {noreply, State};

handle_cast({unmonitor, Ref}, State) ->
    demonitor(Ref),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, _Pid, Reason}, State)
  when Reason == normal; Reason == shutdown ->
    {noreply, State};

handle_info({'DOWN', _MRef, process, Pid, Reason}, State) ->
    {stop, {worker_died, {Pid, Reason}}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Receive TCP data - decode framing and dispatch
do_write_objects(BinObjs, DoneFun, WireVersion) ->
    Worker = poolboy:checkout(riak_repl2_rtsink_pool, true, infinity),
    MRef = monitor(process, Worker),
    Me = self(),
    WrapperFun = fun(ObjectFilteringRules) -> DoneFun(ObjectFilteringRules), gen_server:cast(Me, {unmonitor, MRef}) end,
    ok = riak_repl_fullsync_worker:do_binputs(Worker, BinObjs, WrapperFun, riak_repl2_rtsink_pool, WireVersion).



do_write_objects_v4(BinObjs, Meta, AckPid, WireVersion) ->
    Objects = riak_repl_util:from_wire(WireVersion, BinObjs),
    Retries = app_helper:get_env(riak_repl, rtsink_retry_limit, 3),
    do_repl_put(Objects, Meta, AckPid, WireVersion, Retries).

do_repl_put(_Objects, _Meta, _AckPid, 0) ->
    failed;
do_repl_put(Objects, Meta, AckPid, Counter) ->

    Results =
        lists:foldl(
            fun(Obj, Acc) ->
                PutResult = do_repl_put(Obj),
                Rules = riak_repl2_object_filter:get_realtime_blacklist(Obj)
            end, [], Objects),
    ok.


do_repl_put(Object) ->
    B = riak_object:bucket(Object),
    K = riak_object:key(Object),
    case repl_helper_recv(Object) of
        ok ->
            ReqId = erlang:phash2({self(), os:timestamp()}),
            Opts = [asis, disable_hooks, {update_last_modified, false}],
            {ok, PutPid} = riak_kv_put_fsm:start_link(ReqId, Object, all, all, ?REPL_FSM_TIMEOUT, self(), Opts),
            MRef = erlang:monitor(process, PutPid),

            %% block waiting for response
            case wait_for_response(ReqId, "put") of
                ok ->
                    wait_for_fsm(MRef),
                    maybe_reap(ReqId, Object, B, K),
                    ack;
                {error, _Reason} ->
                    wait_for_fsm(MRef),
                    retry
            end;
        cancel ->
            lager:debug("Skipping repl received object ~p/~p", [B, K]),
            ack
    end.

maybe_reap(ReqId, Object, B, K) ->
    %% This needs to grab the delete mode and check is backend_reap is enabled
    %% Then grab riak_object:has_expire_time(Object) and check what to do with it from there (no need to reap!)
    case riak_kv_util:is_x_deleted(Object) of
        true -> _ = reap(ReqId, B, K, Object);
        false -> lager:debug("Incoming obj ~p/~p", [B, K])
    end.

reap(ReqId, Bucket, Key, Object) ->
    case riak_kv_util:backend_reap_mode(Bucket) of
        {backend_reap, _BackendreapThreshold} ->
            case riak_object:has_expire_time(Object) of
                false ->
                    reap(ReqId, Bucket, Key);
                _ ->
                    ok
            end;
        _ ->
            reap(ReqId, Bucket, Key)
    end.

reap(ReqId, Bucket, Key) ->
    lager:debug("Incoming deleted obj ~p/~p", [Bucket, Key]),
    riak_kv_get_fsm:start(ReqId, Bucket, Key, 1, ?REPL_FSM_TIMEOUT, self()),
    %% block waiting for response
    wait_for_response(ReqId, "reap").

wait_for_response(ReqId, Verb) ->
    receive
        {ReqId, {error, Reason}} ->
            lager:debug("Failed to ~s replicated object: ~p", [Verb, Reason]),
            {error, Reason};
        {ReqId, _} ->
            ok
    after 60000 ->
        lager:warning("Timed out after 1 minute doing ~s on replicated object", [Verb]),
        {error, timeout}
    end.

wait_for_fsm(MRef) ->
    %% wait for put FSM to exit
    receive {'DOWN', MRef, _, _, _} ->
        ok
    after 60000 ->
        lager:warning("put fsm did not exit on schedule"),
        ok
    end.

repl_helper_recv(Object) ->
    case application:get_env(riak_core, repl_helper) of
        undefined ->
            ok;
        {ok, Mods} ->
            repl_helper_recv(Mods, Object)
    end.

repl_helper_recv([], _O) ->
    ok;
repl_helper_recv([{App, Mod}|T], Object) ->
    try Mod:recv(Object) of
        ok ->
            repl_helper_recv(T, Object);
        cancel ->
            cancel;
        Other ->
            lager:error("Unexpected result running repl recv helper ""~p from application ~p : ~p", [Mod, App, Other]),
            repl_helper_recv(T, Object)
    catch
        What:Why ->
            lager:error("Crash while running repl recv helper ""~p from application ~p : ~p:~p", [Mod, App, What, Why]),
            repl_helper_recv(T, Object)
    end.


maybe_push(Binary, Meta, ObjectFilteringRules) ->
    case app_helper:get_env(riak_repl, realtime_cascades, always) of
        never ->
            lager:debug("Skipping cascade due to app env setting"),
            ok;
        always ->
            lager:debug("app env either set to always, or in default; doing cascade"),
            List = riak_repl_util:from_wire(Binary),
            Meta2 = orddict:erase(skip_count, Meta),
            Meta3 = add_object_filtering_blacklist_to_meta(Meta2, ObjectFilteringRules),
            riak_repl2_rtq:push(length(List), Binary, Meta3)
    end.

add_object_filtering_blacklist_to_meta(Meta, []) ->
    lager:error("object filtering rules failed to return the default"),
    Meta;
add_object_filtering_blacklist_to_meta(Meta, [Rules]) ->
    orddict:store(?BT_META_BLACKLIST, Rules, Meta);
add_object_filtering_blacklist_to_meta(Meta, [_Rules | _Rest]) ->
    lager:error("object filtering, repl binary has more than one object!"),
    Meta.


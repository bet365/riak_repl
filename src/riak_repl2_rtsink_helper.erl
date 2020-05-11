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

handle_cast({write_objects_v3, BinObjs, DoneFun, Ver}, State) ->
    do_write_objects_v3(BinObjs, DoneFun, Ver),
    {noreply, State};

handle_cast({write_objects_v4, BinObjs, Meta, AckPid, Ver}, State) ->
    do_write_objects_v4(BinObjs, Meta, AckPid, Ver),
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
do_write_objects_v3(BinObjs, DoneFun, WireVersion) ->
    Worker = poolboy:checkout(riak_repl2_rtsink_pool, true, infinity),
    MRef = monitor(process, Worker),
    Me = self(),
    WrapperFun = fun(ObjectFilteringRules) -> DoneFun(ObjectFilteringRules), gen_server:cast(Me, {unmonitor, MRef}) end,
    ok = riak_repl_fullsync_worker:do_binputs(Worker, BinObjs, WrapperFun, riak_repl2_rtsink_pool, WireVersion).



do_write_objects_v4(BinObjs, Meta, AckPid, WireVersion) ->
    Objects = riak_repl_util:from_wire(WireVersion, BinObjs),
    NewMeta = orddict:erase(skip_count, Meta),
    {Objects2, _} =
        lists:foldl(
            fun(Obj, {Acc, OrginalMeta}) ->
                Rules = riak_repl2_object_filter:get_realtime_blacklist(Obj),
                UpdMeta = orddict:store(?BT_META_BLACKLIST, Rules, OrginalMeta),
                {[{Obj, UpdMeta} | Acc], OrginalMeta}
            end, {[], NewMeta}, Objects),
    Retries = app_helper:get_env(riak_repl, rtsink_retry_limit, 3),
    do_repl_put(Objects2, AckPid, Retries).

do_repl_put(_ObjectsMeta, _AckPid, 0) ->
    failed;
do_repl_put(ObjectsMeta, AckPid, Counter) ->

    Results =
        lists:foldl(
            fun(ObjMeta, Acc) ->
                case do_repl_put(ObjMeta) of
                    ack ->
                        Acc;
                    retry ->
                        [ObjMeta | Acc]
                end
            end, [], ObjectsMeta),

    case Results of
        [] ->
            gen_server:cast(AckPid, ack_v4);
        _ ->
            gen_server:cast(AckPid, retrying),
            do_repl_put(Results, AckPid, Counter -1)

    end.


do_repl_put({Object, Meta}) ->
    B = riak_object:bucket(Object),
    K = riak_object:key(Object),
    case repl_helper_recv(Object) of
        ok ->
            ReqId = erlang:phash2({self(), os:timestamp()}),
            Opts = [{timeout, ?REPL_FSM_TIMEOUT}, asis, disable_hooks, {update_last_modified, false}, {w, all}, {dw, all}],
            From = {raw, ReqId, self()},

            %% can be {ok, Pid} | {error, overload} (if its the error its sent back to the client anyway)
            _ = riak_kv_put_fsm:start(From, Object, Opts),

            %% block waiting for response
            case wait_for_response(ReqId, "put") of
                ok ->
                    maybe_reap(ReqId, Object, B, K),
                    maybe_push(Object, Meta),
                    ack;
                {error, Reason} ->
                    lager:info("Retrying repl put due to: ~p", [Reason]),
                    %% Do not push onto queue, until we have completed the PUT successfully
                    retry
            end;
        cancel ->
            %% Do not place onto the realtime queue, just like the riak_repl2_rt:postcommit hook
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
    %% reap needs to go to 'all' not '1'
    riak_kv_get_fsm:start(ReqId, Bucket, Key, all, ?REPL_FSM_TIMEOUT, self()),
    %% block waiting for response
    wait_for_response(ReqId, "reap").

wait_for_response(ReqId, Verb) ->
    receive
        {ReqId, {error, Reason}} ->
            lager:debug("Failed to ~s replicated object: ~p", [Verb, Reason]),
            {error, Reason};
        {ReqId, _} ->
            ok
    after ?REPL_FSM_TIMEOUT + 5000 ->
        lager:warning("Timed out after 20 seconds doing ~s on replicated object", [Verb]),
        {error, timeout}
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


maybe_push(Obj, Meta) ->
    case app_helper:get_env(riak_repl, realtime_cascades, always) of
        never ->
            ok;
        always ->
            %% taken from riak_repl2_rt:postcommit
            BinObjs =
                case orddict:fetch(?BT_META_TYPED_BUCKET, Meta) of
                    false ->
                        riak_repl_util:to_wire(w1, Obj);
                    true ->
                        riak_repl_util:to_wire(w2, Obj)
                end,
            %% try the proxy first, avoids race conditions with unregister()
            %% during shutdown
            case whereis(riak_repl2_rtq_proxy) of
                undefined ->
                    Concurrency = riak_repl_util:get_rtq_concurrency(),
                    Hash = erlang:phash2(BinObjs, Concurrency) +1,
                    riak_repl2_rtq:push(Hash, 1, BinObjs, Meta);
                _ ->
                    %% we're shutting down and repl is stopped or stopping...
                    riak_repl2_rtq_proxy:push(1, BinObjs, Meta)
            end
    end.


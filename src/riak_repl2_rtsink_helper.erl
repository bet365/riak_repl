%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.

%% @doc Realtime replication sink module
%%
%% High level responsibility...
%%  consider moving out socket responsibilities to another process
%%  to keep this one responsive (but it would pretty much just do status)
%%
-module(riak_repl2_rtsink_helper).

%% API
-export([start_link/1,
         stop/1,
         poolboy_status/1,
         write_objects/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-behavior(gen_server).

-record(state, {
    parent, %% Parent process
    poolboy_pid
}).

start_link(Parent) ->
    gen_server:start_link(?MODULE, [Parent], []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

poolboy_status(Pid) ->
    gen_server:call(Pid, poolboy_status, infinity).

write_objects(Pid, BinObjs, DoneFun, Ver) ->
    gen_server:cast(Pid, {write_objects, BinObjs, DoneFun, Ver}).

%% Callbacks
init([Parent]) ->
    MinPool = app_helper:get_env(riak_repl, rtsink_min_workers, 5),
    MaxPool = app_helper:get_env(riak_repl, rtsink_max_workers, 100),
    PoolArgs =
        [
            {worker_module, riak_repl_fullsync_worker},
            {worker_args, []},
            {size, MinPool}, {max_overflow, MaxPool}
        ],

    {ok, Pool} = poolboy:start_link(PoolArgs),
    {ok, #state{parent = Parent, poolboy_pid = Pool}}.

handle_call(poolboy_status, _From, #state{poolboy_pid = Pool} = State) ->
    {PoolboyState, PoolboyQueueLength, PoolboyOverflow, PoolboyMonitorsActive}
        = poolboy:status(Pool),
    Status =
        [{poolboy_state, PoolboyState},
        {poolboy_queue_length, PoolboyQueueLength},
        {poolboy_overflow, PoolboyOverflow},
        {poolboy_monitors_active, PoolboyMonitorsActive}],
    {reply, Status, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({write_objects, BinObjs, DoneFun, Ver}, #state{poolboy_pid = Pool} = State) ->
    do_write_objects(BinObjs, DoneFun, Ver, Pool),
    {noreply, State};
handle_cast({unmonitor, Ref}, State) ->
    demonitor(Ref),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, _Pid, Reason}, State)
  when Reason == normal; Reason == shutdown ->
    {noreply, State};
handle_info({'DOWN', _MRef, process, Pid, Reason}, State) ->
    %% TODO: Log worker failure
    %% TODO: Needs graceful way to let rtsink know so it can die
    {stop, {worker_died, {Pid, Reason}}, State}.

terminate(_Reason, _State) ->
    %% TODO: Consider trying to do something graceful with poolboy?
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Receive TCP data - decode framing and dispatch
do_write_objects(BinObjs, DoneFun, Ver, Pool) ->
    Worker = poolboy:checkout(Pool, true, infinity),
    MRef = monitor(process, Worker),
    Me = self(),
    WrapperFun = fun(ObjectFilteringRules) -> DoneFun(ObjectFilteringRules), gen_server:cast(Me, {unmonitor, MRef}) end,
    ok = riak_repl_fullsync_worker:do_binputs(Worker, BinObjs, WrapperFun, Pool, Ver).

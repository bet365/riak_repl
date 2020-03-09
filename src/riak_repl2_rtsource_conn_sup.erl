%% Riak EnterpriseDS
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_rtsource_conn_sup).
-behaviour(supervisor).
-export([
    start_link/0,
    enable/1,
    disable/1,
    enabled/0
]).

-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


enable(Remote) ->
    lager:info("Starting replication realtime source ~p", [Remote]),
    ChildSpec = make_remote(Remote),
    supervisor:start_child(?MODULE, ChildSpec).

disable(Remote) ->
    lager:info("Stopping replication realtime source ~p", [Remote]),
    _ = supervisor:terminate_child(?MODULE, Remote),
    _ = supervisor:delete_child(?MODULE, Remote).

enabled() ->
    [{Remote, ConnMgrSupPid}  || {Remote, ConnMgrSupPid, _, [riak_repl2_rtsource_conn_mgr_remote_sup]}
        <- supervisor:which_children(?MODULE), is_pid(ConnMgrSupPid)].

%% @private
init([]) ->
    %% once conn mgr is started by core.  Must be started/registered before sources are started
    riak_repl2_rt:register_remote_locator(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Remotes = riak_repl_ring:rt_started(Ring),
    Children = [make_remote(Remote) || Remote <- Remotes],
    {ok, {{one_for_one, 10, 10}, Children}}.

make_remote(Remote) ->
    {Remote, {riak_repl2_rtsource_conn_mgr_remote_sup, start_link, [Remote]},
        permanent, infinity, supervisor, [riak_repl2_rtsource_conn_mgr_remote_sup]}.

%% Riak EnterpriseDS
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_rtsource_conn_sup).
-behaviour(supervisor).
-export([
    start_link/0,
    enable/1,
    disable/1,
    enabled/0,

    set_leader/2,
    node_watcher_update/1
]).

-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%TODO: Rename enable/disable something better - start/stop is a bit overloaded
enable(Remote) ->
    lager:info("Starting replication realtime source ~p", [Remote]),
    ChildSpec = make_remote(Remote),
    supervisor:start_child(?MODULE, ChildSpec).

disable(Remote) ->
    lager:info("Stopping replication realtime source ~p", [Remote]),
    _ = supervisor:terminate_child(?MODULE, Remote),
    _ = supervisor:delete_child(?MODULE, Remote),
    riak_repl_util:delete_realtime_endpoints(Remote).

enabled() ->
    [{Remote, ConnMgrPid} || {Remote, ConnMgrPid, _, [riak_repl2_rtsource_conn_mgr]}
        <- supervisor:which_children(?MODULE), is_pid(ConnMgrPid)].

set_leader(LeaderNode, _LeaderPid) ->
    lists:foreach(
        fun({_, Pid, _, [riak_repl2_rtsource_conn_mgr]}) ->
            riak_repl2_rtsource_conn_mgr:set_leader(Pid, LeaderNode)
        end, supervisor:which_children(?MODULE)).

node_watcher_update(_Services) ->
    lists:foreach(
        fun({_, Pid, _, [riak_repl2_rtsource_conn_mgr]}) ->
            riak_repl2_rtsource_conn_mgr:node_watcher_update(Pid)
        end, supervisor:which_children(?MODULE)).

%% @private
init([]) ->
    %% TODO: Move before riak_repl2_rt_sup start
    %% once connmgr is started by core.  Must be started/registered
    %% before sources are started.
    riak_repl2_rt:register_remote_locator(),
    riak_core_node_watcher_events:add_sup_callback(fun ?MODULE:node_watcher_update/1),

    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Remotes = riak_repl_ring:rt_started(Ring),
    Children = [make_remote(Remote) || Remote <- Remotes],
    {ok, {{one_for_one, 10, 10}, Children}}.

make_remote(Remote) ->
    {Remote, {riak_repl2_rtsource_conn_mgr, start_link, [Remote]},
        permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn_mgr]}.

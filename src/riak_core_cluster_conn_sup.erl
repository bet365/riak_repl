%% Riak Core Cluster Manager Connection Supervisor
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
%%
%% Mission: ensure connections from a cluster manager to other clusters
%% in order to resolve ip addresses into known clusters or to refresh
%% the list of remote cluster members and observe their status.

-module(riak_core_cluster_conn_sup).
-behaviour(supervisor).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).
-else.
-define(TRACE(Stmt),ok).
-endif.

-export([start_link/0,
         add_remote_connection/1, remove_remote_connection/1,
         connections/0, is_connected/1
        ]).
-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give cluster_conn processes to shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

add_remote_connection(Remote) ->
    ?TRACE(?debugFmt("cluster_conn_sup: Connecting to remote cluster: ~p", [Remote])),
    lager:info("Connecting to remote cluster: ~p", [Remote]),
    ChildSpec = make_remote(Remote),
    supervisor:start_child(?MODULE, ChildSpec).

remove_remote_connection(Remote) ->
    ?TRACE(?debugFmt("cluster_conn_sup: Disconnecting from remote cluster: ~p", [Remote])),
    lager:info("Disconnecting from remote cluster: ~p", [Remote]),
    supervisor:terminate_child(?MODULE, Remote),
    supervisor:delete_child(?MODULE, Remote).

connections() ->
    [{Remote, Pid} || {Remote, Pid, _, _} <- supervisor:which_children(?MODULE), is_pid(Pid)].

is_connected(Remote) ->
    not ([] == lists:filter(fun({R,_Pid}) -> R == Remote end, connections())).

%% @private
init([]) ->
    %% %% TODO: Move before riak_core_cluster_mgr_sup start
    %% %% once connmgr is started by core. This must be registered
    %% %% before the connections start up. Uses an identity function
    %% %% to boostrap cluster connections by address.
    %% riak_core_cluster_mgr:register_cluster_locator(),

    %% %% TODO: remote list of test addresses.
    %% Remotes = initial clusters or ip addrs from ring
    %% Children = [make_remote(Remote) || Remote <- Remotes],
    Children = [],
    {ok, {{one_for_one, 10, 10}, Children}}.

make_remote(Remote) ->
    {Remote, {riak_core_cluster_conn, start_link, [Remote]},
        permanent, ?SHUTDOWN, worker, [riak_core_cluster_conn]}.

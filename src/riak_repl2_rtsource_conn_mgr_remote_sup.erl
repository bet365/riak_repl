-module(riak_repl2_rtsource_conn_mgr_remote_sup).
-behaviour(supervisor).
-export([
    start_link/1,
    enabled/2,
    maybe_rebalance/1,
    get_all_status/2
]).
-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

start_link(Remote) ->
    supervisor:start_link(?MODULE, [Remote]).

maybe_rebalance(Pid) ->
    lists:foreach(
        fun({_, ConnMgr, _, _}) ->
            riak_repl2_rtsource_conn_mgr:maybe_rebalance(ConnMgr)
        end, supervisor:which_children(Pid)).

get_all_status(Pid, Timeout) ->
    lists:foldl(
        fun({_, ConnMgr, _, _}, Acc) ->
            [riak_repl2_rtsource_conn_mgr:get_all_status(ConnMgr, Timeout) | Acc]
        end, [], supervisor:which_children(Pid)).


enabled(Pid, Remote) ->
    [{Remote, ConnMgrPid} || {_Id, ConnMgrPid, _, [riak_repl2_rtsource_conn_mgr]}
        <- supervisor:which_children(Pid), is_pid(ConnMgrPid)].


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Remote]) ->
    Concurrency = app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)),
    Children = [make_child(Remote, Id) || Id <- lists:seq(1, Concurrency)],
    {ok, {{one_for_one, 10, 10}, Children}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

make_child(Remote, Id) ->
    {Id, {riak_repl2_rtsource_conn_mgr, start_link, [Remote, Id]},
        permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn_mgr]}.
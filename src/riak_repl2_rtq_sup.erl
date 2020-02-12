%%%-------------------------------------------------------------------
%%% @author nordinesaabouni
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Feb 2020 12:39
%%%-------------------------------------------------------------------
-module(riak_repl2_rtq_sup).
-author("nordinesaabouni").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================


start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).


enable(Remote) ->
    lager:info("Starting realtime queue worker ~p", [Remote]),
    RealtimeQueueWorker = make_realtime_queue_woker(Remote),
    supervisor:start_child(?MODULE, RealtimeQueueWorker),
    lager:info("Starting overload counter worker for realtime queue ~p", [Remote]),
    OverLoadCounter = make_overload_counter_worker(Remote),
    supervisor:start_child(?MODULE, OverLoadCounter).

disable(Remote) ->
    lager:info("Stopping replication realtime source ~p", [Remote]),
    _ = supervisor:terminate_child(?MODULE, Remote),
    _ = supervisor:delete_child(?MODULE, Remote),
    riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, Remote).

enabled() ->
    [ {Remote, ConnMgrPid} || {Remote, ConnMgrPid, _, [riak_repl2_rtsource_conn_mgr]}
        <- supervisor:which_children(?MODULE), is_pid(ConnMgrPid)].



init([]) ->


    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Remotes = riak_repl_ring:rt_started(Ring),


    {ok, {one_for_one, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_realtime_queue_woker(RemoteName) ->
    {riak_repl2_rtq:name(RemoteName), {riak_repl2_rtq, start_link, [RemoteName]},
        transient, 50000, worker, [riak_repl2_rtq]}.

make_overload_counter_worker(RemoteName) ->
    {riak_repl2_rtq_overload_counter:name(RemoteName), {riak_repl2_rtq_overload_counter, start_link, [RemoteName]},
        transient, 50000, worker, [riak_repl2_rtq_overload_counter]}.



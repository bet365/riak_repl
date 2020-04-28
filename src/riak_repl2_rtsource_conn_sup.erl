%% Riak EnterpriseDS
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_rtsource_conn_sup).
-behaviour(supervisor).
-include("riak_repl.hrl").

-export([
    start_link/0,
    enable/1,
    disable/1,
    enabled/0,
    status/0,
    maybe_rebalance/0,
    maybe_rebalance/1
]).

-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


enable(Remote) ->
    ChildSpec = make_remote(Remote),
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, _} ->
            lager:info("Starting replication realtime source ~p", [Remote]);
        {ok, _, _} ->
            lager:info("Starting replication realtime source ~p", [Remote]);
        _ ->
            ok
    end.

disable(Remote) ->
    lager:info("Stopping replication realtime source ~p", [Remote]),
    _ = supervisor:terminate_child(?MODULE, Remote),
    _ = supervisor:delete_child(?MODULE, Remote).

enabled() ->
    [{Remote, ConnMgr}  || {Remote, ConnMgr, _, [riak_repl2_rtsource_conn_mgr]}
        <- supervisor:which_children(?MODULE), is_pid(ConnMgr)].

status() ->
    get_all_status().

maybe_rebalance() ->
    lists:foreach(fun({_, Pid}) -> riak_repl2_rtsource_conn_mgr:maybe_rebalance(Pid) end, enabled()).

maybe_rebalance(RemoteList) when is_list(RemoteList) ->
    lists:foreach(
        fun({Remote, Pid}) ->
            case lists:member(Remote, RemoteList) of
                true ->
                    riak_repl2_rtsource_conn_mgr:maybe_rebalance(Pid);
                _ ->
                    ok
            end
        end, enabled());
maybe_rebalance(Remote) ->
    case lists:keyfind(Remote, 1, enabled()) of
        false ->
            error;
        {Remote, Pid} ->
            riak_repl2_rtsource_conn_mgr:maybe_rebalance(Pid)
    end.

%% @private
init([]) ->
    %% once conn mgr is started by core.  Must be started/registered before sources are started
    riak_repl2_rt:register_remote_locator(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Remotes = riak_repl_ring:rt_started(Ring),
    Children = [make_remote(Remote) || Remote <- Remotes],
    {ok, {{one_for_one, 10, 10}, Children}}.

make_remote(Remote) ->
    {Remote, {riak_repl2_rtsource_conn_mgr, start_link, [Remote]},
        permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource_conn_mgr]}.



get_all_status() ->
    lists:foldl(
        fun({_, Pid}, Acc) ->
            case get_status(Pid) of
                busy ->
                    Acc;
                {Remote, Pids, Stats} ->
                    Latency = get_latency(Remote, Pids),
                    NewStats = {Remote, Stats ++ [{latency, Latency}]},
                    [NewStats | Acc]
            end
        end, [], enabled()).

get_status(Pid) ->
    try
        riak_repl2_rtsource_conn_mgr:status(Pid)
    catch
        _:_  ->
            busy
    end .



get_latency(Remote, Pids) ->
    %% Latency information
    Timeout = app_helper:get_env(riak_repl, status_timeout, 500),
    LatencyPerSink = lists:foldl(
        fun(Pid, Acc) ->
            case riak_repl2_rtsource_conn:get_latency(Pid, Timeout) of
                error -> Acc;
                %% note that rtsource_conn has alaredy formatted this "Addr" peername
                {Addr, LatencyDistribtion} -> merge_latency(Addr, LatencyDistribtion, Acc)
            end
        end, orddict:new(), Pids),
    generate_latency_from_distribtuion(LatencyPerSink, Remote).

merge_latency(Addr, LatencyDistribtion, Acc) ->
    case orddict:find(Addr, Acc) of
        error ->
            orddict:store(Addr, LatencyDistribtion, Acc);
        {ok, OGLatencyDistribtion} ->
            Latency3 = merge_latency(OGLatencyDistribtion, LatencyDistribtion),
            orddict:store(Addr, Latency3, Acc)
    end.

merge_latency(L1, L2) ->
    #distribution_collector
    {number_data_points = N1, aggregate_values = AV1, aggregate_values_sqrd = AVS1, max = Max1} = L1,
    #distribution_collector
    {number_data_points = N2, aggregate_values = AV2, aggregate_values_sqrd = AVS2, max = Max2} = L2,
    Max =  case Max1 >= Max2 of
               true ->
                   Max1;
               false ->
                   Max2
           end,
    #distribution_collector
    {number_data_points = N1 + N2, aggregate_values = AV1 + AV2, aggregate_values_sqrd = AVS1 + AVS2, max = Max}.


generate_latency_from_distribtuion(LatencyPerSink, Remote) ->
    RemoteLatencyDistribution =
        orddict:fold(
            fun(_, LatencyDistribution, Acc) ->
                merge_latency(Acc, LatencyDistribution)
            end, #distribution_collector{}, LatencyPerSink),
    AllLatencyDistributions = orddict:store(Remote, RemoteLatencyDistribution, LatencyPerSink),
    orddict:map(fun(_Key, Distribution) -> calculate_latency(Distribution) end, AllLatencyDistributions).

calculate_latency(#distribution_collector{number_data_points = 0}) ->
    [{mean, 0}, {percentile_95, 0}, {percentile_99, 0}, {percentile_100, 0}];
calculate_latency(Dist) ->
    #distribution_collector
    {number_data_points = N, aggregate_values = AV, aggregate_values_sqrd = AVS, max = Max} = Dist,
    Mean = AV / N,
    Var = (AVS + (N*Mean*Mean) - (2*Mean*AV)) / N,
    Std = math:sqrt(Var),
    P95 = Mean + 1.645*Std,
    P99 = Mean + 2.326*Std,
    [{mean, round(Mean)}, {percentile_95, round(P95)}, {percentile_99, round(P99)}, {percentile_100, round(Max)}].
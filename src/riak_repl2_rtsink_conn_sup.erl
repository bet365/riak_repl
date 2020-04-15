%% Riak EnterpriseDS
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_rtsink_conn_sup).
-behaviour(supervisor).
-export([start_link/0, start_child/2, started/0, send_shutdown/0, status/0]).
-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Proto, Remote) ->
    supervisor:start_child(?MODULE, [Proto, Remote]).

status() ->
    get_all_status().

started() ->
    [Pid || {_, Pid, _, _} <- supervisor:which_children(?MODULE)].

send_shutdown() ->
    lists:foreach(
        fun(Pid) ->
            riak_repl2_rtsink_conn:send_shutdown(Pid)
        end, started()).

%% @private
init([]) ->
    ChildSpec = {undefined, {riak_repl2_rtsink_conn, start_link, []},
                 temporary, 5000, worker, [riak_repl2_rtsink_conn]},
    {ok, {{simple_one_for_one, 10, 10}, [ChildSpec]}}.




get_all_status() ->
    {PoolboyState, PoolboyQueueLength, PoolboyOverflow, PoolboyMonitorsActive} = poolboy:status(riak_repl2_rtsink_pool),
    PoolboyStats =
        [
            {poolboy_state, PoolboyState},
            {poolboy_queue_length, PoolboyQueueLength},
            {poolboy_overflow, PoolboyOverflow},
            {poolboy_monitors_active, PoolboyMonitorsActive}
        ],
    AllStats = lists:foldl(fun(Pid, Dict) -> merge_stats(Pid, Dict) end, [], started()),
    SinkStats = lists:foldl(
        fun({{Remote, IP, Version}, Stats}, Acc) ->
            SortedStats =
                [
                    {Remote,
                        [
                            {ip, IP},
                            {version, Version},
                            {stats, Stats}]
                    }
                ],
            Acc ++ SortedStats
        end, [], AllStats),
    SinkStats ++ PoolboyStats.


merge_stats(Pid, Dict) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    try
        {Key, StatsDict} = riak_repl2_rtsink_conn:summarized_status(Pid, Timeout),
        {_, MsgLen} = erlang:process_info(Pid, message_queue_len),
        StatsDict1 = orddict:store(message_queue_len, MsgLen, StatsDict),
        MergedStatsDict =
            case orddict:find(Key, Dict) of
                {ok, StatsDict0} ->
                    orddict:merge(fun(_, V1, V2) -> V1 + V2 end, StatsDict0, StatsDict1);
                _ ->
                    StatsDict
            end,
        orddict:store(Key, MergedStatsDict, Dict)
    catch
        _:_ ->
            %% maybe have too_busy here so we know?
            Dict
    end.
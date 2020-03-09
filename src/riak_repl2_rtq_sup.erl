-module(riak_repl2_rtq_sup).
-behaviour(supervisor).

%% API
-export(
[
    start_link/0,
    unregister/1,
    status/0
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(SHUTDOWN, 50000). % how long to give reference queue to persist on shutdown

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

unregister(Remote) ->
    Concurrency = app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)),
    lists:foreach(fun(Id) -> riak_repl2_rtq:unregister(Id, Remote) end, lists:seq(1,Concurrency)).

status() ->
    get_all_status().

init([]) ->
    Concurrency = app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)),
    RTQ = [make_rtq(Id) || Id <- lists:seq(1, Concurrency)],
    Overload = [make_overload(Id) || Id <- lists:seq(1, Concurrency)],
    Children = RTQ ++ Overload,
    {ok, {{one_for_one, 10, 10}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_rtq(Id) ->
    Name = riak_repl2_rtq:name(Id),
    {Name, {riak_repl2_rtq, start_link, [Id]},
        transient, ?SHUTDOWN, worker, [Name]}.

make_overload(Id) ->
    Name = riak_repl2_rtq_overload_counter:name(Id),
    {Name, {riak_repl2_rtq_overload_counter, start_link, [Id]},
        permanent, ?SHUTDOWN, worker, [Name]}.



get_all_status() ->
    try
        N = app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)),
        AllStats =
            lists:foldl(
                fun(X, Acc) ->
                    Status = riak_repl2_rtq:status(X),
                    orddict:to_list(merge_status(orddict:from_list(Status), orddict:from_list(Acc)))
                end, riak_repl2_rtq:status(1), lists:seq(2, N)),

        % I'm having the calling process do derived stats because
        % I don't want to block the rtq from processing objects.
        MaxBytes = proplists:get_value(max_bytes, AllStats),
        CurrentBytes = proplists:get_value(bytes, AllStats),
        PercentBytes = round( (CurrentBytes / MaxBytes) * 100000 ) / 1000,
        [{percent_bytes_used, PercentBytes} | AllStats]
    catch _:_ ->
        []
    end.


merge_status(Status1, Status2) ->
    orddict:merge(fun status_merge_fun/3, Status1, Status2).
status_merge_fun(remotes, V1, V2) ->
    orddict:merge(fun consumer_merge_fun/3, orddict:from_list(V1), orddict:from_list(V2));
status_merge_fun(_, V1, V2) ->
    V1 + V2.
consumer_merge_fun(_, V1, V2) ->
    merge_status(orddict:from_list(V1), orddict:from_list(V2)).
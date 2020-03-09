-module(riak_repl2_reference_rtq_remote_sup).
-behaviour(supervisor).

%% API
-export(
[
    start_link/1,
    shutdown/1
]).

-export([init/1]).

-define(SHUTDOWN, 50000). % how long to give reference queue to persist on shutdown

start_link(Remote) ->
    supervisor:start_link(?MODULE, [Remote]).

shutdown(Remote) ->
    Concurrency = app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)),
    lists:foreach(fun(Id) -> riak_repl2_reference_rtq:shutdown(Remote, Id) end, lists:seq(1, Concurrency)).

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
    Name = riak_repl2_reference_rtq:name(Remote, Id),
    {Name, {riak_repl2_reference_rtq, start_link, [Remote, Id]},
        transient, ?SHUTDOWN, worker, [Name]}.
-module(riak_repl2_rtq_sup).
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

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================


init([]) ->
    Children = lists:foldl(
        fun(N, Acc) ->
            [make_rtq(N) | Acc]
        end, [], lists:seq(1, app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)))),

    {ok, {{one_for_one, 10, 10}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_rtq(N) ->
    {riak_repl2_rtq:rtq_name(N), {riak_repl2_rtq, start_link, [N]},
        transient, 50000, worker, [riak_repl2_rtq:rtq_name(N)]}.
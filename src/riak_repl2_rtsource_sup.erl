%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsource_sup).
-author('Andy Gross <andy@basho.com>').
-behaviour(supervisor).

%% External exports
-export([start_link/0]).
%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

enable(Remote) ->
    ok.

start(Remote) ->
    ok.

stop(Remote) ->
    ok.

disable(Remote) ->
    ok.

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    Processes =
        [
            {riak_repl2_rtsource_conn_data_mgr, {riak_repl2_rtsource_conn_data_mgr, start_link, []},
                permanent, 50000, worker, [riak_repl2_rtsource_conn_data_mgr]},

            {riak_repl2_rtq_sup, {riak_repl2_rtq_sup, start_link, []},
                permanent, infinity, supervisor, [riak_repl2_rtq_sup]},

            {riak_repl2_rtsource_conn_sup, {riak_repl2_rtsource_conn_sup, start_link, []},
                permanent, infinity, supervisor, [riak_repl2_rtsource_conn_sup]}
        ],

    {ok, {{rest_for_one, 9, 10}, Processes}}.
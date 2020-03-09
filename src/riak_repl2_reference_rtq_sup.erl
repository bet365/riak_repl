-module(riak_repl2_reference_rtq_sup).
-behaviour(supervisor).
-export(
[
    start_link/0,
    enable/1,
    disable/1,
    shutdown/0
]).
-export([init/1]).

-define(SERVER, ?MODULE).
-define(SHUTDOWN, 5000). % how long to give reference queue to persist on shutdown


start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

enable(Remote) ->
    lager:info("Starting reference queue for: ~p", [Remote]),
    ChildSpec = make_child(Remote),
    supervisor:start_child(?MODULE, ChildSpec).

disable(Remote) ->
    lager:info("Stopping reference queue for: ~p", [Remote]),
    _ = supervisor:terminate_child(?MODULE, Remote),
    _ = supervisor:delete_child(?MODULE, Remote),
    lager:info("Reference Queue has been stopped, unregistering remote from realtime queue"),
    riak_repl2_rtq_sup:unregister(Remote),
    ok.

shutdown() ->
    lists:foreach(
        fun({Remote, _, [riak_repl2_reference_rtq_remote_sup]}) ->
            riak_repl2_reference_rtq_remote_sup:shutdown(Remote)
        end, supervisor:which_children(?MODULE)).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================


init([]) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Remotes = riak_repl_ring:rt_enabled(Ring),
    Children = [make_child(Remote) || Remote <- Remotes],
    {ok, {{one_for_one, 10, 10}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_child(Remote) ->
    {Remote, {riak_repl2_reference_rtq_remote_sup, start_link, [Remote]},
        transient, infinity, supervisor, [riak_repl2_reference_rtq_remote_sup]}.
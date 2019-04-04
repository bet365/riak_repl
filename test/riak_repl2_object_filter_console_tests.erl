-module(riak_repl2_object_filter_console_tests).
-include_lib("eunit/include/eunit.hrl").
-include("riak_repl2_object_filter.hrl").

-define(setup(F), {setup, fun start/0, fun stop/1, {timeout, 500, F}}).

object_filter_console_test_() ->
    [
        {"Test Enable Both", ?setup(fun test_object_filter_enable_both/0)},
        {"Test Enable Realtime", ?setup(fun test_object_filter_enable_realtime/0)},
        {"Test Enable Fullsync", ?setup(fun test_object_filter_enable_fullsync/0)},
        {"Test Disable Both", ?setup(fun test_object_filter_disable_both/0)},
        {"Test Disable Realtime", ?setup(fun test_object_filter_disable_realtime/0)},
        {"Test Disable Fullsync", ?setup(fun test_object_filter_disable_fullsync/0)},
        {"Test Set Repl Config", ?setup(fun test_object_filter_set_repl_config/0)},
        {"Test Set Realtime Config", ?setup(fun test_object_filter_set_realtime_config/0)},
        {"Test Set Fullsync Config", ?setup(fun test_object_filter_set_fullsync_config/0)},
        {"Test Set Realtime Fullsync Config", ?setup(fun test_object_filter_set_realtime_fullsync_config/0)}
    ].

start() ->
    catch(meck:unload(riak_core_capability)),
    meck:new(riak_core_capability, [passthrough]),
    meck:expect(riak_core_capability, get, 1, fun(_) -> 1.0 end),
    meck:expect(riak_core_capability, get, 2, fun(_, _) -> 1.0 end),

    catch(meck:unload(riak_core_connection)),
    meck:new(riak_core_connection, [passthrough]),
    meck:expect(riak_core_connection, symbolic_clustername, 0, fun() -> "cluster-1" end),

    catch(meck:unload(riak_core_metadata)),
    meck:new(riak_core_metadata, [passthrough]),
    meck:expect(riak_core_metadata, get, 2,
        fun
            ({riak_repl2_object_filter, config}, _) -> [];
            ({riak_repl2_object_filter, status}, _) -> disabled

        end),
    meck:expect(riak_core_metadata, put, 3, fun(_,_,_) -> ok end),

    App1 = riak_repl_test_util:start_lager(),
    App2 = riak_repl2_object_filter_console:start_link(),
    [App1, App2].

stop(StartedApps) ->
    cleanup(),
    process_flag(trap_exit, true),
    catch(meck:unload(riak_core_capability)),
    catch(meck:unload(riak_core_connection)),
    catch(meck:unload(riak_core_metadata)),
    process_flag(trap_exit, false),
    riak_repl_test_util:stop_apps(StartedApps).

cleanup() ->
    file:delete("/tmp/repl.config"),
    file:delete("/tmp/realtime.config"),
    file:delete("/tmp/fullsync.config"),
    riak_repl2_object_filter_console:disable(),
    riak_repl2_object_filter_console:clear_config("all").


%% ===================================================================
%% Enable and Disable
%% ===================================================================
test_object_filter_enable_both() ->
    riak_repl2_object_filter_console:enable(),
    A = enabled == riak_repl2_object_filter:get_status(realtime),
    B = enabled == riak_repl2_object_filter:get_status(fullsync),
    riak_repl2_object_filter_console:disable(),
    ?assertEqual(true, A and B).

test_object_filter_enable_realtime() ->
    riak_repl2_object_filter_console:enable("realtime"),
    A = enabled == riak_repl2_object_filter:get_status(realtime),
    B = disabled == riak_repl2_object_filter:get_status(fullsync),
    riak_repl2_object_filter_console:disable("realtime"),
    ?assertEqual(true, A and B).

test_object_filter_enable_fullsync() ->
    riak_repl2_object_filter_console:enable("fullsync"),
    A = disabled == riak_repl2_object_filter:get_status(realtime),
    B = enabled == riak_repl2_object_filter:get_status(fullsync),
    riak_repl2_object_filter_console:disable("realtime"),
    ?assertEqual(true, A and B).

test_object_filter_disable_both() ->
    riak_repl2_object_filter_console:enable(),
    riak_repl2_object_filter_console:disable(),
    A = disabled == riak_repl2_object_filter:get_status(realtime),
    B = disabled == riak_repl2_object_filter:get_status(fullsync),
    ?assertEqual(true, A and B).

test_object_filter_disable_realtime() ->
    riak_repl2_object_filter_console:enable(),
    riak_repl2_object_filter_console:disable("realtime"),
    A = disabled == riak_repl2_object_filter:get_status(realtime),
    B = enabled == riak_repl2_object_filter:get_status(fullsync),
    riak_repl2_object_filter_console:disable(),
    ?assertEqual(true, A and B).

test_object_filter_disable_fullsync() ->
    riak_repl2_object_filter_console:enable(),
    riak_repl2_object_filter_console:disable("fullsync"),
    A = enabled == riak_repl2_object_filter:get_status(realtime),
    B = disabled == riak_repl2_object_filter:get_status(fullsync),
    riak_repl2_object_filter_console:disable(),
    ?assertEqual(true, A and B).


%% ===================================================================
%% Set And Get Configs
%% ===================================================================
test_object_filter_set_repl_config() ->
    Fun = fun(X, Type) -> ?assertEqual(true, test_object_filter_load_config(X, Type)), cleanup() end,
    [Fun(X, repl) || X <- get_loading_configs(repl)].

test_object_filter_set_realtime_config() ->
    Fun = fun(X, Type) -> ?assertEqual(true, test_object_filter_load_config(X, Type)), cleanup() end,
    [Fun(X, realtime) || X <- get_loading_configs(realtime)].

test_object_filter_set_fullsync_config() ->
    Fun = fun(X, Type) -> ?assertEqual(true, test_object_filter_load_config(X, Type)), cleanup() end,
    [Fun(X, fullsync) || X <- get_loading_configs(fullsync)].

test_object_filter_set_realtime_fullsync_config() ->
    Fun = fun(X, Type) -> ?assertEqual(true, test_object_filter_load_config(X, Type)), cleanup() end,
    [Fun(X, {realtime, fullsync}) || X <- get_loading_configs({realtime, fullsync})].

%%test_object_filter_set_repl_realtime_config() ->
%%    [{timeout, 15, ?_assertEqual(true, test_object_filter_load_config(X, {repl, realtime}))} || X <- get_loading_configs({repl, realtime})].

%% =====================================================================================================================

test_object_filter_load_config({Config, LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync}, repl) ->
    write_terms("/tmp/repl.config", Config),
    riak_repl2_object_filter_console:load_config("repl", "/tmp/repl.config"),
    check_configs(LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync);
test_object_filter_load_config({Config, LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync}, realtime) ->
    write_terms("/tmp/realtime.config", Config),
    riak_repl2_object_filter_console:load_config("realtime", "/tmp/realtime.config"),
    check_configs(LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync);
test_object_filter_load_config({Config, LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync}, fullsync) ->
    write_terms("/tmp/fullsync.config", Config),
    riak_repl2_object_filter_console:load_config("fullsync", "/tmp/fullsync.config"),
     check_configs(LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync);
test_object_filter_load_config({RTConfig, FSConfig, LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync}, {realtime, fullsync}) ->
    write_terms("/tmp/realtime.config", RTConfig),
    write_terms("/tmp/fullsync.config", FSConfig),
    riak_repl2_object_filter_console:load_config("realtime", "/tmp/realtime.config"),
    riak_repl2_object_filter_console:load_config("fullsync", "/tmp/fullsync.config"),
    check_configs(LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync).

%%test_object_filter_load_config({RTConfig, FSConfig, LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync}, {realtime, fullsync}) ->
%%    write_terms("/tmp/repl.config", RTConfig),
%%    write_terms("/tmp/realtime.config", FSConfig),
%%    riak_repl2_object_filter_console:load_config("realtime", "/tmp/repl.config"),
%%    riak_repl2_object_filter_console:load_config("fullsync", "/tmp/realtime.config"),
%%    check_configs(LoadedRepl, LoadedRealtime, LoadedFullsync, Realtime, Fullsync).


%% ===================================================================
%% Helper Functions
%% ===================================================================
write_terms(Filename, List) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = lists:map(Format, List),
    file:write_file(Filename, Text).

sort_config(Config) ->
    lists:foldl(
        fun({RemoteName, {allow, L1}, {block, L2}}, Acc) ->
            Acc ++ [{RemoteName, {allow, lists:sort(L1)}, {block, lists:sort(L2)}}]
        end,
        [], lists:sort(Config)).


check_configs(LRepl2, LRT2, LFS2, RT2, FS2) ->
    LRepl1 = sort_config(riak_repl2_object_filter:get_config(loaded_repl)),
    LRT1 = sort_config(riak_repl2_object_filter:get_config(loaded_realtime)),
    LFS1 = sort_config(riak_repl2_object_filter:get_config(loaded_fullsync)),
    RT1 = sort_config(riak_repl2_object_filter:get_config(realtime)),
    FS1 = sort_config(riak_repl2_object_filter:get_config(fullsync)),

    A = LRepl1 == sort_config(LRepl2),
    B = LRT1 == sort_config(LRT2),
    C = LFS1 == sort_config(LFS2),
    D = RT1 == sort_config(RT2),
    E = FS1 == sort_config(FS2),

    A and B and C and D and E.




get_loading_configs(repl) ->
    R = [{Rules, true} || Rules <- get_loading_rules(true)] ++ [{Rules, false} || Rules <- get_loading_rules(false)],
    [build_loading_configs_response_1(repl, AllowBlocked, Rules, Loaded) || AllowBlocked <- [allow, block], {Rules, Loaded} <- R];
get_loading_configs(realtime) ->
    R = [{Rules, true} || Rules <- get_loading_rules(true)] ++ [{Rules, false} || Rules <- get_loading_rules(false)],
    [build_loading_configs_response_1(realtime, AllowBlocked, Rules, Loaded) || AllowBlocked <- [allow, block], {Rules, Loaded} <- R];
get_loading_configs(fullsync) ->
    R = [{Rules, true} || Rules <- get_loading_rules(true)] ++ [{Rules, false} || Rules <- get_loading_rules(false)],
    [build_loading_configs_response_1(fullsync, AllowBlocked, Rules, Loaded) || AllowBlocked <- [allow, block], {Rules, Loaded} <- R];

get_loading_configs({realtime, fullsync}) ->
    R = [{Rules, true} || Rules <- get_loading_rules(true)] ++ [{Rules, false} || Rules <- get_loading_rules(false)],
    [build_loading_configs_response_2({realtime, fullsync}, AllowBlocked, Rules1, Loaded1, Rules2, Loaded2) || AllowBlocked <- [allow, block], {Rules1, Loaded1} <- R, {Rules2, Loaded2} <- R].

%%get_loading_configs({repl, realtime}) ->
%%    R = [{Rules, true} || Rules <- get_loading_rules(true)] ++ [{Rules, false} || Rules <- get_loading_rules(false)],
%%    [build_loading_configs_response_2({repl, realtime}, AllowBlocked, Rules1, Loaded1, Rules2, Loaded2) || AllowBlocked <- [allow, block], {Rules1, Loaded1} <- R, {Rules2, Loaded2} <- R].


build_loading_configs_response_1(_, AllowBlocked, Rules, false) ->
    Config = build_config(AllowBlocked, Rules),
    {Config, [],[],[],[],[]};
build_loading_configs_response_1(repl, AllowBlocked, Rules, true) ->
    Config = build_config(AllowBlocked, Rules),
    LoadedRepl = Config,
    LoadedRT = [],
    LoadedFS = [],
    RT = Config,
    FS = Config,
    {Config, LoadedRepl, LoadedRT, LoadedFS, RT, FS};
build_loading_configs_response_1(realtime, AllowBlocked, Rules, true) ->
    Config = build_config(AllowBlocked, Rules),
    LoadedRepl = [],
    LoadedRT = Config,
    LoadedFS = [],
    RT = Config,
    FS = [],
    {Config, LoadedRepl, LoadedRT, LoadedFS, RT, FS};
build_loading_configs_response_1(fullsync, AllowBlocked, Rules, true) ->
    Config = build_config(AllowBlocked, Rules),
    LoadedRepl = [],
    LoadedRT = [],
    LoadedFS = Config,
    RT = [],
    FS = Config,
    {Config, LoadedRepl, LoadedRT, LoadedFS, RT, FS}.

build_loading_configs_response_2({realtime, fullsync}, AllowBlocked, Rules1, Loaded1, Rules2, Loaded2) ->
    RTConfig = build_config(AllowBlocked, Rules1),
    FSConfig = build_config(AllowBlocked, Rules2),
    LoadedRepl = [],
    LoadedRT = case Loaded1 of
                   true ->
                       RTConfig;
                   false ->
                       []
               end,
    LoadedFS = case Loaded2 of
                   true ->
                       FSConfig;
                   false ->
                       []
               end,
    RT = LoadedRT,
    FS = LoadedFS,
    {RTConfig, FSConfig, LoadedRepl, LoadedRT, LoadedFS, RT, FS}.

%%build_loading_configs_response_2({repl, realtime}, AllowBlocked, Rules, Loaded1, Rules, Loaded2) ->
%%    RTConfig = build_config(AllowBlocked, Rules1),
%%    FSConfig = build_config(AllowBlocked, Rules2),
%%    LoadedRepl = [],
%%    LoadedRT = case Loaded1 of
%%                   true ->
%%                       RTConfig;
%%                   false ->
%%                       []
%%               end,
%%    LoadedFS = case Loaded2 of
%%                   true ->
%%                       FSConfig;
%%                   false ->
%%                       []
%%               end,
%%    RT = LoadedRT,
%%    FS = LoadedFS,
%%    {RTConfig, FSConfig, LoadedRepl, LoadedRT, LoadedFS, RT, FS}.



build_config(allow, Rules) ->
    [{"test-cluster", {allow, Rules}, {block, []}}];
build_config(block, Rules) ->
    [{"test-cluster", {allow, []}, {block, Rules}}].

get_loading_rules(true) ->
    A =
        [
            [{bucket, <<"bucket-1">>}],
            [{bucket, {<<"type-1">>, <<"bucket-1">>}}],
            [{metadata, {key, "value"}}],
            [{metadata, {key}}],
            [{lastmod_age_greater_than, 1000}],
            [{lastmod_age_greater_than, -1000}],
            [{lastmod_age_less_than, 1000}],
            [{lastmod_age_less_than, -1000}],
            [{lastmod_greater_than, 10}],
            [{lastmod_greater_than, -10}],
            [{lastmod_less_than, 10}],
            [{lastmod_less_than, -10}]
        ],
    B = [ [{lnot, X}]  || [X] <- A],

    A ++ B;
get_loading_rules(false) ->
    [
        ['*', {bucket, <<"bucket-1">>}],
        [{bucket, <<"bucket-1">>, '*'}],
        [{lnot, '*'}],
        [{lnot, ['*']}],


        [{bucke, <<"bucket-1">>}],
        [{bucke, {<<"type-1">>, <<"bucket-1">>}}],
        [{metadat, {key, "value"}}],
        [{metadat, {key}}],
        [{lastmod_age_greater_tha, 1000}],
        [{lastmod_age_greater_tha, -1000}],
        [{lastmod_age_less_tha, 1000}],
        [{lastmod_age_less_tha, -1000}],
        [{lastmod_greater_tha, 10}],
        [{lastmod_greater_tha, -10}],
        [{lastmod_less_tha, 10}],
        [{lastmod_less_tha, -10}]
    ].
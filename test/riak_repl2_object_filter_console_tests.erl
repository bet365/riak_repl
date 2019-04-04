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
        {"Test Set Realtime Fullsync Config", ?setup(fun test_object_filter_set_realtime_fullsync_config/0)},
        {"Test Set Repl Realtime Config", ?setup(fun test_object_filter_set_repl_realtime_config/0)},
        {"Test Set Repl Fullsync Config", ?setup(fun test_object_filter_set_repl_fullsync_config/0)}
%%        {"Test Set Repl Realtime Fullsync Config", ?setup(fun test_object_filter_set_repl_realtime_fullsync_config/0)}
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
%%%% ===================================================================
test_object_filter_set_repl_config() ->
    [run_test(X) || X <- get_loading_configs([repl])].

test_object_filter_set_realtime_config() ->
    [run_test(X) || X <- get_loading_configs([realtime])].

test_object_filter_set_fullsync_config() ->
    [run_test(X) || X <- get_loading_configs([fullsync])].

test_object_filter_set_realtime_fullsync_config() ->
    [run_test(X) || X <- get_loading_configs([realtime, fullsync])].

test_object_filter_set_repl_realtime_config() ->
    [run_test(X) || X <- get_loading_configs([repl, realtime])].

test_object_filter_set_repl_fullsync_config() ->
    [run_test(X) || X <- get_loading_configs([repl, fullsync])].

%%test_object_filter_set_repl_realtime_fullsync_config() ->
%%    [run_test(X) || X <- get_loading_configs([repl, realtime, fullsync])].

run_test(X) ->
    ?assertEqual(true, test_object_filter_load_config(X)),
    cleanup().


%% =====================================================================================================================
test_object_filter_load_config(OD) ->
    SetReplConfig = orddict:fetch(set_repl_config, OD),
    SetRealtimeConfig = orddict:fetch(set_realtime_config, OD),
    SetFullsyncConfig = orddict:fetch(set_fullsync_config, OD),
    LoadedRepl = orddict:fetch(loaded_repl, OD),
    LoadedRT = orddict:fetch(loaded_realtime, OD),
    LoadedFS = orddict:fetch(loaded_fullsync, OD),
    RT = orddict:fetch(realtime, OD),
    FS = orddict:fetch(fullsync, OD),
    write_terms("/tmp/repl.config", SetReplConfig),
    write_terms("/tmp/realtime.config", SetRealtimeConfig),
    write_terms("/tmp/fullsync.config", SetFullsyncConfig),
    riak_repl2_object_filter_console:load_config("repl", "/tmp/repl.config"),
    riak_repl2_object_filter_console:load_config("realtime", "/tmp/realtime.config"),
    riak_repl2_object_filter_console:load_config("fullsync", "/tmp/fullsync.config"),
    check_configs(LoadedRepl, LoadedRT, LoadedFS, RT, FS).


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

%%    ct:pal(
%%        "loaded repl actual: ~p~n" ++
%%        "loaded repl expected: ~p~n~n" ++
%%        "loaded realtime actual: ~p~n" ++
%%        "loaded realtime expected: ~p~n~n" ++
%%        "loaded fullsync actual: ~p~n" ++
%%        "loaded fullsync expected: ~p~n~n" ++
%%        "realtime actual: ~p~n" ++
%%        "realtime expected: ~p~n~n" ++
%%        "fullsync actual: ~p~n" ++
%%        "fullsync expected: ~p~n~n",
%%    [LRepl1, LRepl2, LRT1, LRT2, LFS1, LFS2, RT1, RT2, FS1, FS2]),

    A and B and C and D and E.




get_loading_configs(List) ->
    ReplG = make_generator(repl, List),
    RTG = make_generator(realtime, List),
    FSG = make_generator(fullsync, List),
    [build_loading_configs_response(Repl, RT, FS, AllowBlocked) ||
        AllowBlocked <- [allow, block], Repl <- ReplG, RT <- RTG, FS <- FSG].


make_generator(Mode, List)->
    case lists:member(Mode, List) of
        true -> [{Rules, true} || Rules <- get_loading_rules(true)] ++ [{Rules, false} || Rules <- get_loading_rules(false)];
        _ -> [{[], true}]
    end.

build_loading_configs_response(
    {ReplRules, _} = Repl,
    {RTRules, _} = RT,
    {FSRules, _} = FS,
    AllowBlocked) ->

    SetLoadedConfig = fun
                          (AB, {Config, true}) -> build_config(AB, Config);
                          (_, {_, false}) -> []
                      end,

    SetConfig = fun
                    (AB, {R1, true}, {R2, true}) -> build_merged_config(AB, R1, R2);
                    (AB, {R1, true}, {_, false}) -> build_config(AB, R1);
                    (AB, {_, false}, {R2, true}) -> build_config(AB, R2);
                    (_, {_, false}, {_, false}) -> []
                end,

    LoadedReplConfig = SetLoadedConfig(AllowBlocked, Repl),
    LoadedRTConfig = SetLoadedConfig(AllowBlocked, RT),
    LoadedFSConfig = SetLoadedConfig(AllowBlocked, FS),
    RTConfig = SetConfig(AllowBlocked, Repl, RT),
    FSConfig = SetConfig(AllowBlocked, Repl, FS),

    orddict:from_list([
        {set_repl_config, build_config(AllowBlocked, ReplRules)},
        {set_realtime_config, build_config(AllowBlocked, RTRules)},
        {set_fullsync_config, build_config(AllowBlocked, FSRules)},
        {loaded_repl, LoadedReplConfig},
        {loaded_realtime, LoadedRTConfig},
        {loaded_fullsync, LoadedFSConfig},
        {realtime, RTConfig},
        {fullsync, FSConfig}
    ]).



build_merged_config(AllowBlocked, ['*'], _) ->
    build_config(AllowBlocked, ['*']);
build_merged_config(AllowBlocked, _, ['*']) ->
    build_config(AllowBlocked, ['*']);
build_merged_config(AllowBlocked, R1, R2) ->
    build_config(AllowBlocked, lists:usort(R1++R2)).

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

    A ++ B ++ [['*']];
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
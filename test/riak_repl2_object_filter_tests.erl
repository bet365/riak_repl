-module(riak_repl2_object_filter_tests).
-import(riak_repl2_object_filter, [filter_object_rule_test/2]).
-include_lib("eunit/include/eunit.hrl").

object_filter_test_() ->
    {spawn,
        [
            {setup,
                fun setup/0,
                fun cleanup/1,
                fun(_) ->
                    [
                        {"Single Rules", fun test_object_filter_single_rules/0},
                        {"Multi Rules (bucket, metadata)", fun test_object_filter_multi_rules_bucket_metadata/0},
                        {"Multi Rules (bucket, not_metadata)", fun test_object_filter_multi_rules_bucket_not_metadata/0},
                        {"Multi Rules (not_bucket, metadata)", fun test_object_filter_multi_rules_not_bucket_metadata/0},
                        {"Multi Rules (not_bucket, not_metadata)", fun test_object_filter_multi_rules_not_bucket_not_metadata/0},
                        {"Test Enable Both", fun test_object_filter_enable_both/0},
                        {"Test Enable Realtime", fun test_object_filter_enable_realtime/0},
                        {"Test Enable Fullsync", fun test_object_filter_enable_fullsync/0},
                        {"Test Disable Both", fun test_object_filter_disable_both/0},
                        {"Test Disable Realtime", fun test_object_filter_disable_realtime/0},
                        {"Test Disable Fullsync", fun test_object_filter_disable_fullsync/0}

                    ]
                end
            }
        ]
    }.

setup() ->
    catch(meck:unload(riak_core_capability)),
    meck:new(riak_core_capability, [passthrough]),
    meck:expect(riak_core_capability, get, 1, fun(_) -> 1.0 end),
    meck:expect(riak_core_capability, get, 2, fun(_, _) -> 1.0 end),

    catch(meck:unload(riak_core_connection)),
    meck:new(riak_core_connection, [passthrough]),
    meck:expect(riak_core_connection, symbolic_clustername, 0, fun() -> "cluster-1" end),

    catch(meck:unload(riak_core_ring_manager)),
    meck:new(riak_core_ring_manager, [passthrough]),
    meck:expect(riak_core_ring_manager, ring_trans, 2, fun(_, _) -> ok end),

    App1 = riak_repl_test_util:start_test_ring(),
    App2 = riak_repl_test_util:start_lager(),
    App3 = riak_repl2_object_filter:start_link(),
    [App1, App2, App3].
cleanup(StartedApps) ->
    process_flag(trap_exit, true),
    catch(meck:unload(riak_core_capability)),
    catch(meck:unload(riak_core_connection)),
    process_flag(trap_exit, false),
    riak_repl_test_util:stop_apps(StartedApps).
%% ===================================================================
%% Sing Rules
%% ===================================================================
test_object_filter_single_rules() ->
    B = <<"bucket">>, K = <<"key">>, V = <<"value">>, M = dict:from_list([{filter, 1}]),
    O = riak_object:new(B,K,V,M),
    [test_object_filter_single_rules(N, O) || N <- lists:seq(1,14)],
    pass.

test_object_filter_single_rules(1, Obj)->
    Actual = filter_object_rule_test([], Obj),
    ?assertEqual(false, Actual);
test_object_filter_single_rules(2, Obj)->
    Actual = filter_object_rule_test(['*'], Obj),
    ?assertEqual(true, Actual);
test_object_filter_single_rules(3, Obj)->
    Actual = filter_object_rule_test([{bucket, <<"bucket">>}], Obj),
    ?assertEqual(true, Actual);
test_object_filter_single_rules(4, Obj)->
    Actual = filter_object_rule_test([{bucket, <<"any other bucket">>}], Obj),
    ?assertEqual(false, Actual);
test_object_filter_single_rules(5, Obj)->
    Actual = filter_object_rule_test([{bucket, all}], Obj),
    ?assertEqual(true, Actual);
test_object_filter_single_rules(6, Obj)->
    Actual = filter_object_rule_test([{not_bucket, <<"bucket">>}], Obj),
    ?assertEqual(false, Actual);
test_object_filter_single_rules(7, Obj)->
    Actual = filter_object_rule_test([{not_bucket, <<"any other bucket">>}], Obj),
    ?assertEqual(true, Actual);
test_object_filter_single_rules(8, Obj)->
    Actual = filter_object_rule_test([{not_bucket, all}], Obj),
    ?assertEqual(false, Actual);
test_object_filter_single_rules(9, Obj)->
    Actual = filter_object_rule_test([{metadata, {filter, 1}}], Obj),
    ?assertEqual(true, Actual);
test_object_filter_single_rules(10, Obj)->
    Actual = filter_object_rule_test([{metadata, {filter, 2}}], Obj),
    ?assertEqual(false, Actual);
test_object_filter_single_rules(11, Obj)->
    Actual = filter_object_rule_test([{metadata, {filter, all}}], Obj),
    ?assertEqual(true, Actual);
test_object_filter_single_rules(12, Obj)->
    Actual = filter_object_rule_test([{not_metadata, {filter, 1}}], Obj),
    ?assertEqual(false, Actual);
test_object_filter_single_rules(13, Obj)->
    Actual = filter_object_rule_test([{not_metadata, {filter, 2}}], Obj),
    ?assertEqual(true, Actual);
test_object_filter_single_rules(14, Obj)->
    Actual = filter_object_rule_test([{not_metadata, {filter, all}}], Obj),
    ?assertEqual(false, Actual).


%% ===================================================================
%% Multi Rules: bucket and metadata
%% ===================================================================
test_object_filter_multi_rules_bucket_metadata() ->
    B = <<"bucket">>, K = <<"key">>, V = <<"value">>, M = dict:from_list([{filter, 1}]),
    O = riak_object:new(B,K,V,M),
    [test_object_filter_multi_rules_bucket_metadata(N, O) || N <- lists:seq(1,18)],
    pass.

test_object_filter_multi_rules_bucket_metadata(1, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {metadata, {filter, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_metadata(2, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {metadata, {filter, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(3, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {metadata, {filter, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_metadata(4, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {metadata, {filter, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(5, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {metadata, {filter, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(6, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {metadata, {filter, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(7, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {metadata, {filter, all}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_metadata(8, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(9, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {metadata, {filter, all}}]], Obj),
    ?assertEqual(true, Actual);

test_object_filter_multi_rules_bucket_metadata(10, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {metadata, {other, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(11, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {metadata, {other, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(12, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {metadata, {other, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(13, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {metadata, {other, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(14, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {metadata, {other, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(15, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {metadata, {other, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(16, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {metadata, {other, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(17, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {metadata, {other, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_metadata(18, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {metadata, {other, all}}]], Obj),
    ?assertEqual(false, Actual).


%% ===================================================================
%% Multi Rules: bucket and not_metadata
%% ===================================================================
test_object_filter_multi_rules_bucket_not_metadata() ->
    B = <<"bucket">>, K = <<"key">>, V = <<"value">>, M = dict:from_list([{filter, 1}]),
    O = riak_object:new(B,K,V,M),
    [test_object_filter_multi_rules_bucket_not_metadata(N, O) || N <- lists:seq(1,18)],
    pass.

test_object_filter_multi_rules_bucket_not_metadata(1, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {not_metadata, {filter, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_not_metadata(2, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {not_metadata, {filter, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_not_metadata(3, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {not_metadata, {filter, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_not_metadata(4, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {not_metadata, {filter, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_not_metadata(5, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {not_metadata, {filter, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_not_metadata(6, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {not_metadata, {filter, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_not_metadata(7, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {not_metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_not_metadata(8, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {not_metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_not_metadata(9, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {not_metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);

test_object_filter_multi_rules_bucket_not_metadata(10, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {not_metadata, {other, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_not_metadata(11, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {not_metadata, {other, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_not_metadata(12, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {not_metadata, {other, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_not_metadata(13, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {not_metadata, {other, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_not_metadata(14, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {not_metadata, {other, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_not_metadata(15, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {not_metadata, {other, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_not_metadata(16, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"bucket">>}, {not_metadata, {other, all}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_bucket_not_metadata(17, Obj)->
    Actual = filter_object_rule_test([[{bucket, <<"anything">>}, {not_metadata, {other, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_bucket_not_metadata(18, Obj)->
    Actual = filter_object_rule_test([[{bucket, all}, {not_metadata, {other, all}}]], Obj),
    ?assertEqual(true, Actual).


%% ===================================================================
%% Multi Rules: not_bucket and metadata
%% ===================================================================
test_object_filter_multi_rules_not_bucket_metadata() ->
    B = <<"bucket">>, K = <<"key">>, V = <<"value">>, M = dict:from_list([{filter, 1}]),
    O = riak_object:new(B,K,V,M),
    [test_object_filter_multi_rules_not_bucket_metadata(N, O) || N <- lists:seq(1,18)],
    pass.

test_object_filter_multi_rules_not_bucket_metadata(1, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {metadata, {filter, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(2, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {metadata, {filter, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_metadata(3, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {metadata, {filter, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(4, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {metadata, {filter, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(5, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {metadata, {filter, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(6, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {metadata, {filter, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(7, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(8, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {metadata, {filter, all}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_metadata(9, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);

test_object_filter_multi_rules_not_bucket_metadata(10, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {metadata, {other, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(11, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {metadata, {other, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(12, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {metadata, {other, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(13, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {metadata, {other, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(14, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {metadata, {other, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(15, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {metadata, {other, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(16, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {metadata, {other, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(17, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {metadata, {other, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_metadata(18, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {metadata, {other, all}}]], Obj),
    ?assertEqual(false, Actual).


%% ===================================================================
%% Multi Rules: not_bucket and not_metadata
%% ===================================================================
test_object_filter_multi_rules_not_bucket_not_metadata() ->
    B = <<"bucket">>, K = <<"key">>, V = <<"value">>, M = dict:from_list([{filter, 1}]),
    O = riak_object:new(B,K,V,M),
    [test_object_filter_multi_rules_not_bucket_not_metadata(N, O) || N <- lists:seq(1,18)],
    pass.

test_object_filter_multi_rules_not_bucket_not_metadata(1, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {filter, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(2, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {filter, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(3, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {filter, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(4, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {filter, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(5, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {filter, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(6, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {filter, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(7, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(8, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {filter, all}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(9, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);

test_object_filter_multi_rules_not_bucket_not_metadata(10, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {other, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(11, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {other, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(12, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {other, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(13, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {other, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(14, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {other, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(15, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {other, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(16, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {other, all}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(17, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {other, all}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(18, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {other, all}}]], Obj),
    ?assertEqual(true, Actual).


%% ===================================================================
%% Enable and Disable
%% ===================================================================
test_object_filter_enable_both() ->
    riak_repl2_object_filter:enable(),
    ?assertEqual(enabled, riak_repl2_object_filter:get_status(realtime)),
    ?assertEqual(enabled, riak_repl2_object_filter:get_status(fullsync)),
    riak_repl2_object_filter:disable(),
    pass.

test_object_filter_enable_realtime() ->
    riak_repl2_object_filter:enable("realtime"),
    ?assertEqual(enabled, riak_repl2_object_filter:get_status(realtime)),
    ?assertEqual(disabled, riak_repl2_object_filter:get_status(fullsync)),
    riak_repl2_object_filter:disable("realtime"),
    pass.

test_object_filter_enable_fullsync() ->
    riak_repl2_object_filter:enable("fullsync"),
    ?assertEqual(disabled, riak_repl2_object_filter:get_status(realtime)),
    ?assertEqual(enabled, riak_repl2_object_filter:get_status(fullsync)),
    riak_repl2_object_filter:disable("realtime"),
    pass.

test_object_filter_disable_both() ->
    riak_repl2_object_filter:enable(),
    riak_repl2_object_filter:disable(),
    ?assertEqual(disabled, riak_repl2_object_filter:get_status(realtime)),
    ?assertEqual(disabled, riak_repl2_object_filter:get_status(fullsync)),
    pass.

test_object_filter_disable_realtime() ->
    riak_repl2_object_filter:enable(),
    riak_repl2_object_filter:disable("realtime"),
    ?assertEqual(disabled, riak_repl2_object_filter:get_status(realtime)),
    ?assertEqual(enabled, riak_repl2_object_filter:get_status(fullsync)),
    riak_repl2_object_filter:disable(),
    pass.

test_object_filter_disable_fullsync() ->
    riak_repl2_object_filter:enable(),
    riak_repl2_object_filter:disable("fullsync"),
    ?assertEqual(enabled, riak_repl2_object_filter:get_status(realtime)),
    ?assertEqual(disabled, riak_repl2_object_filter:get_status(fullsync)),
    riak_repl2_object_filter:disable(),
    pass.


%% ===================================================================
%% Load Config
%% ===================================================================



%% ===================================================================
%% Realtime Blacklist
%% ===================================================================



%% ===================================================================
%% Fullsync downgrade config
%% ===================================================================





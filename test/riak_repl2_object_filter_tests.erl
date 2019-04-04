-module(riak_repl2_object_filter_tests).
-import(riak_repl2_object_filter, [filter_object_rule_test/2]).
-include_lib("eunit/include/eunit.hrl").
-include("riak_repl2_object_filter.hrl").

object_filter_test_() ->
    {spawn,
        [
            {setup,
                fun setup/0,
                fun cleanup/1,
                fun(_) ->
                    [
                        {"Single Rules", fun test_object_filter_single_rules/0},
                        {"Mult Rules - Pairwise", fun test_object_filter_multi_rules_pairwise/0},
                        {"Multi Rules - Triplets", fun test_object_filter_multi_rules_triplets/0},
                        {"Get Fullsync Config", fun test_object_filter_get_fullsync_config_3/0}

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
cleanup(StartedApps) ->
    process_flag(trap_exit, true),
    catch(meck:unload(riak_core_capability)),
    catch(meck:unload(riak_core_connection)),
    catch(meck:unload(riak_core_metadata)),
    process_flag(trap_exit, false),
    riak_repl_test_util:stop_apps(StartedApps).

%% ===================================================================
%% Sing Rules
%% ===================================================================
test_object_filter_single_rules() ->
    B = <<"bucket">>, K = <<"key">>, V = <<"value">>, M = dict:from_list([{filter, 1}, {<<"X-Riak-Last-Modified">>, os:timestamp()}]),
    O = riak_object:new(B,K,V,M),
    [test_object_filter_single_rules(N, O) || N <- lists:seq(1,3)].

test_object_filter_single_rules(1, Obj)->
    Actual = filter_object_rule_test([], Obj),
    ?assertEqual(false, Actual);
test_object_filter_single_rules(2, Obj)->
    Actual = filter_object_rule_test(['*'], Obj),
    ?assertEqual(true, Actual);
test_object_filter_single_rules(3, Obj) ->
    Config = get_configs(bucket) ++ get_configs(metadata) ++ get_configs(lastmod),
    TestFun =
        fun({Rule, Outcome}) ->
            Actual = filter_object_rule_test([Rule], Obj),
            ?assertEqual(Outcome, Actual)
        end,
    [TestFun(A) || A <- Config].




%% ===================================================================
%% Multi Rules: pairwise
%% ===================================================================
test_object_filter_multi_rules_pairwise() ->
    B = <<"bucket">>, K = <<"key">>, V = <<"value">>, M = dict:from_list([{filter, 1}, {<<"X-Riak-Last-Modified">>, os:timestamp()}]),
    Obj = riak_object:new(B,K,V,M),

    Config =
        lists:flatten([
            [{[Rule1, Rule2], Outcome1 and Outcome2} || {Rule1, Outcome1} <- get_configs(bucket), {Rule2, Outcome2} <- get_configs(metadata)],
            [{{lnot, [Rule1, Rule2]}, not (Outcome1 and Outcome2)} || {Rule1, Outcome1} <- get_configs(bucket), {Rule2, Outcome2} <- get_configs(metadata)],
            [{{lnot, {lnot, [Rule1, Rule2]}}, not (not (Outcome1 and Outcome2))} || {Rule1, Outcome1} <- get_configs(bucket), {Rule2, Outcome2} <- get_configs(metadata)],

            [{[Rule1, Rule2], Outcome1 and Outcome2} || {Rule1, Outcome1} <- get_configs(bucket), {Rule2, Outcome2} <- get_configs(lastmod)],
            [{{lnot, [Rule1, Rule2]}, not (Outcome1 and Outcome2)} || {Rule1, Outcome1} <- get_configs(bucket), {Rule2, Outcome2} <- get_configs(lastmod)],
            [{{lnot, {lnot, [Rule1, Rule2]}}, not (not (Outcome1 and Outcome2))} || {Rule1, Outcome1} <- get_configs(bucket), {Rule2, Outcome2} <- get_configs(lastmod)],

            [{[Rule1, Rule2], Outcome1 and Outcome2} || {Rule1, Outcome1} <- get_configs(lastmod), {Rule2, Outcome2} <- get_configs(metadata)],
            [{{lnot, [Rule1, Rule2]}, not (Outcome1 and Outcome2)} || {Rule1, Outcome1} <- get_configs(lastmod), {Rule2, Outcome2} <- get_configs(metadata)],
            [{{lnot, {lnot, [Rule1, Rule2]}}, not (not (Outcome1 and Outcome2))} || {Rule1, Outcome1} <- get_configs(lastmod), {Rule2, Outcome2} <- get_configs(metadata)]
        ]),

    TestFun =
        fun({Rule, Outcome}) ->
            Actual = filter_object_rule_test([Rule], Obj),
            ?assertEqual(Outcome, Actual)
        end,
    [TestFun(A) || A <- Config].

%% ===================================================================
%% Multi Rules: triplets
%% ===================================================================
test_object_filter_multi_rules_triplets() ->
    B = <<"bucket">>, K = <<"key">>, V = <<"value">>, M = dict:from_list([{filter, 1}, {<<"X-Riak-Last-Modified">>, os:timestamp()}]),
    Obj = riak_object:new(B,K,V,M),

    Config =
        lists:flatten([
            [{[Rule1, Rule2, Rule3], (Outcome1 and Outcome2 and Outcome3)} ||
                {Rule1, Outcome1} <- get_configs(bucket), {Rule2, Outcome2} <- get_configs(metadata), {Rule3, Outcome3} <- get_configs(lastmod)],

            [{{lnot, [Rule1, Rule2, Rule3]}, not (Outcome1 and Outcome2 and Outcome3)} ||
                {Rule1, Outcome1} <- get_configs(bucket), {Rule2, Outcome2} <- get_configs(metadata), {Rule3, Outcome3} <- get_configs(lastmod)],

            [{{lnot, {lnot, [Rule1, Rule2, Rule3]}}, not (not (Outcome1 and Outcome2 and Outcome3))} ||
                {Rule1, Outcome1} <- get_configs(bucket), {Rule2, Outcome2} <- get_configs(metadata), {Rule3, Outcome3} <- get_configs(lastmod)]
        ]),

    TestFun =
        fun({Rule, Outcome}) ->
            Actual = filter_object_rule_test([Rule], Obj),
            ?assertEqual(Outcome, Actual)
        end,
    [TestFun(A) || A <- Config].

%% ===================================================================
%% Get Fullsync Config (with altered lastmod_age_*)
%% ===================================================================
test_object_filter_get_fullsync_config_3() ->
    TimeStamp = os:timestamp(),
    TS1 = timestamp_to_secs(TimeStamp) + 1000,
    TS2 = timestamp_to_secs(TimeStamp) - 1000,
    Testset1 =
        [
            {bucket,<<"bucket">>},
            {lnot,{bucket,<<"bucket">>}},
            {lnot,{lnot,{bucket,<<"bucket">>}}},
            {bucket,<<"anything">>},
            {lnot,{bucket,<<"anything">>}},
            {lnot,{lnot,{bucket,<<"anything">>}}},
            {metadata,{filter,1}},
            {lnot,{metadata,{filter,1}}},
            {lnot,{lnot,{metadata,{filter,1}}}},
            {metadata,{filter,2}},
            {lnot,{metadata,{filter,2}}},
            {lnot,{lnot,{metadata,{filter,2}}}},
            {metadata,{filter}},
            {lnot,{metadata,{filter}}},
            {lnot,{lnot,{metadata,{filter}}}},
            {metadata,{other,1}},
            {lnot,{metadata,{other,1}}},
            {lnot,{lnot,{metadata,{other,1}}}},
            {metadata,{other,2}},
            {lnot,{metadata,{other,2}}},
            {lnot,{lnot,{metadata,{other,2}}}},
            {metadata,{other}},
            {lnot,{metadata,{other}}},
            {lnot,{lnot,{metadata,{other}}}},
            {lastmod_age_greater_than, -1000},
            {lnot, {lastmod_age_greater_than, -1000}},
            {lnot, {lnot, {lastmod_age_greater_than, -1000}}},
            {lastmod_age_greater_than, 1000},
            {lnot, {lastmod_age_greater_than, 1000}},
            {lnot, {lnot, {lastmod_age_greater_than, 1000}}},
            {lastmod_age_less_than, -1000},
            {lnot, {lastmod_age_less_than, -1000}},
            {lnot, {lnot, {lastmod_age_less_than, -1000}}},
            {lastmod_age_less_than, 1000},
            {lnot, {lastmod_age_less_than, 1000}},
            {lnot, {lnot, {lastmod_age_less_than, 1000}}},
            {lastmod_greater_than, TS1},
            {lnot, {lastmod_greater_than, TS1}},
            {lnot, {lnot, {lastmod_greater_than, TS1}}},
            {lastmod_greater_than, TS2},
            {lnot, {lastmod_greater_than, TS2}},
            {lnot, {lnot, {lastmod_greater_than, TS2}}},
            {lastmod_less_than, TS1},
            {lnot, {lastmod_less_than, TS1}},
            {lnot, {lnot, {lastmod_less_than, TS1}}},
            {lastmod_less_than, TS2},
            {lnot, {lastmod_less_than, TS2}},
            {lnot, {lnot, {lastmod_less_than, TS2}}},
            [{bucket,<<"bucket">>}, {lastmod_age_greater_than, -1000}],
            [{lnot, {lastmod_age_greater_than, -1000}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_age_greater_than, -1000}}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lastmod_age_greater_than, 1000}],
            [{lnot, {lastmod_age_greater_than, 1000}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_age_greater_than, 1000}}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lastmod_age_less_than, -1000}],
            [{lnot, {lastmod_age_less_than, -1000}},{bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_age_less_than, -1000}}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lastmod_age_less_than, 1000}],
            [{lnot, {lastmod_age_less_than, 1000}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_age_less_than, 1000}}}, {bucket,<<"bucket">>}],
            {lnot, [{bucket,<<"bucket">>}, {lastmod_age_greater_than, -1000}]},
            {lnot, [{lnot, {lastmod_age_greater_than, -1000}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_age_greater_than, -1000}}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lastmod_age_greater_than, 1000}]},
            {lnot, [{lnot, {lastmod_age_greater_than, 1000}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_age_greater_than, 1000}}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lastmod_age_less_than, -1000}]},
            {lnot, [{lnot, {lastmod_age_less_than, -1000}},{bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_age_less_than, -1000}}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lastmod_age_less_than, 1000}]},
            {lnot, [{lnot, {lastmod_age_less_than, 1000}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_age_less_than, 1000}}}, {bucket,<<"bucket">>}]}
        ],
    Allowed = Testset1,
    Blocked = Testset1,
    Config = [{"test_cluster", {allow, Allowed}, {block, Blocked}}],
    application:set_env(?OBF_CONFIG_KEY, fullsync, Config),

    Expected =
        lists:sort([
            {bucket,<<"bucket">>},
            {lnot,{bucket,<<"bucket">>}},
            {lnot,{lnot,{bucket,<<"bucket">>}}},
            {bucket,<<"anything">>},
            {lnot,{bucket,<<"anything">>}},
            {lnot,{lnot,{bucket,<<"anything">>}}},
            {metadata,{filter,1}},
            {lnot,{metadata,{filter,1}}},
            {lnot,{lnot,{metadata,{filter,1}}}},
            {metadata,{filter,2}},
            {lnot,{metadata,{filter,2}}},
            {lnot,{lnot,{metadata,{filter,2}}}},
            {metadata,{filter}},
            {lnot,{metadata,{filter}}},
            {lnot,{lnot,{metadata,{filter}}}},
            {metadata,{other,1}},
            {lnot,{metadata,{other,1}}},
            {lnot,{lnot,{metadata,{other,1}}}},
            {metadata,{other,2}},
            {lnot,{metadata,{other,2}}},
            {lnot,{lnot,{metadata,{other,2}}}},
            {metadata,{other}},
            {lnot,{metadata,{other}}},
            {lnot,{lnot,{metadata,{other}}}},
            {lastmod_greater_than, TS1},
            {lnot, {lastmod_greater_than, TS1}},
            {lnot, {lnot, {lastmod_greater_than, TS1}}},
            {lastmod_greater_than, TS2},
            {lnot, {lastmod_greater_than, TS2}},
            {lnot, {lnot, {lastmod_greater_than, TS2}}},
            {lastmod_less_than, TS1},
            {lnot, {lastmod_less_than, TS1}},
            {lnot, {lnot, {lastmod_less_than, TS1}}},
            {lastmod_less_than, TS2},
            {lnot, {lastmod_less_than, TS2}},
            {lnot, {lnot, {lastmod_less_than, TS2}}},
            {lastmod_greater_than, TS1},
            {lnot, {lastmod_greater_than, TS1}},
            {lnot, {lnot, {lastmod_greater_than, TS1}}},
            {lastmod_greater_than, TS2},
            {lnot, {lastmod_greater_than, TS2}},
            {lnot, {lnot, {lastmod_greater_than, TS2}}},
            {lastmod_less_than, TS1},
            {lnot, {lastmod_less_than, TS1}},
            {lnot, {lnot, {lastmod_less_than, TS1}}},
            {lastmod_less_than, TS2},
            {lnot, {lastmod_less_than, TS2}},
            {lnot, {lnot, {lastmod_less_than, TS2}}},
            [{bucket,<<"bucket">>}, {lastmod_greater_than, TS1}],
            [{lnot, {lastmod_greater_than, TS1}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_greater_than, TS1}}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lastmod_greater_than, TS2}],
            [{lnot, {lastmod_greater_than, TS2}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_greater_than, TS2}}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lastmod_less_than, TS1}],
            [{lnot, {lastmod_less_than, TS1}},{bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_less_than, TS1}}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lastmod_less_than, TS2}],
            [{lnot, {lastmod_less_than, TS2}}, {bucket,<<"bucket">>}],
            [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_less_than, TS2}}}, {bucket,<<"bucket">>}],
            {lnot, [{bucket,<<"bucket">>}, {lastmod_greater_than, TS1}]},
            {lnot, [{lnot, {lastmod_greater_than, TS1}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_greater_than, TS1}}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lastmod_greater_than, TS2}]},
            {lnot, [{lnot, {lastmod_greater_than, TS2}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_greater_than, TS2}}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lastmod_less_than, TS1}]},
            {lnot, [{lnot, {lastmod_less_than, TS1}},{bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_less_than, TS1}}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lastmod_less_than, TS2}]},
            {lnot, [{lnot, {lastmod_less_than, TS2}}, {bucket,<<"bucket">>}]},
            {lnot, [{bucket,<<"bucket">>}, {lnot, {lnot, {lastmod_less_than, TS2}}}, {bucket,<<"bucket">>}]}
        ]),
    {_, {allow, Allowed2}, {block, Blocked2}} = riak_repl2_object_filter:get_config(fullsync, "test_cluster", TimeStamp),
    Actual1 = lists:sort(Allowed2),
    Actual2 = lists:sort(Blocked2),
    A = Actual1 == Expected,
    B = Actual2 == Expected,
    ?assertEqual(true, A and B).






%% ===================================================================
%% Helper Functions
%% ===================================================================
timestamp_to_secs({M, S, _}) ->
  M * 1000000 + S.

get_configs(bucket) ->
    [
        {{bucket, <<"bucket">>}, true},
        {{lnot, {bucket, <<"bucket">>}}, false},
        {{lnot, {lnot, {bucket, <<"bucket">>}}}, true},

        {{bucket, <<"anything">>}, false},
        {{lnot, {bucket, <<"anything">>}}, true},
        {{lnot, {lnot, {bucket, <<"anything">>}}}, false}
    ];
get_configs(metadata) ->
    [
        {{metadata, {filter, 1}}, true},
        {{lnot, {metadata, {filter, 1}}}, false},
        {{lnot, {lnot, {metadata, {filter, 1}}}}, true},

        {{metadata, {filter, 2}}, false},
        {{lnot, {metadata, {filter, 2}}}, true},
        {{lnot, {lnot, {metadata, {filter, 2}}}}, false},

        {{metadata, {filter}}, true},
        {{lnot, {metadata, {filter}}}, false},
        {{lnot, {lnot, {metadata, {filter}}}}, true},

        {{metadata, {other, 1}}, false},
        {{lnot, {metadata, {other, 1}}}, true},
        {{lnot, {lnot, {metadata, {other, 1}}}}, false},

        {{metadata, {other, 2}}, false},
        {{lnot, {metadata, {other, 2}}}, true},
        {{lnot, {lnot, {metadata, {other, 2}}}}, false},

        {{metadata, {other}}, false},
        {{lnot, {metadata, {other}}}, true},
        {{lnot, {lnot, {metadata, {other}}}}, false}
    ];
get_configs(lastmod) ->
    TS1 = timestamp_to_secs(os:timestamp()) + 1000,
    TS2 = timestamp_to_secs(os:timestamp()) - 1000,
    [
        {{lastmod_age_greater_than, -1000}, true},
        {{lnot, {lastmod_age_greater_than, -1000}}, false},
        {{lnot, {lnot, {lastmod_age_greater_than, -1000}}}, true},

        {{lastmod_age_greater_than, 1000}, false},
        {{lnot, {lastmod_age_greater_than, 1000}}, true},
        {{lnot, {lnot, {lastmod_age_greater_than, 1000}}}, false},

        {{lastmod_age_less_than, -1000}, false},
        {{lnot, {lastmod_age_less_than, -1000}}, true},
        {{lnot, {lnot, {lastmod_age_less_than, -1000}}}, false},

        {{lastmod_age_less_than, 1000}, true},
        {{lnot, {lastmod_age_less_than, 1000}}, false},
        {{lnot, {lnot, {lastmod_age_less_than, 1000}}}, true},

        {{lastmod_greater_than, TS1}, false},
        {{lnot, {lastmod_greater_than, TS1}}, true},
        {{lnot, {lnot, {lastmod_greater_than, TS1}}}, false},

        {{lastmod_greater_than, TS2}, true},
        {{lnot, {lastmod_greater_than, TS2}}, false},
        {{lnot, {lnot, {lastmod_greater_than, TS2}}}, true},

        {{lastmod_less_than, TS1}, true},
        {{lnot, {lastmod_less_than, TS1}}, false},
        {{lnot, {lnot, {lastmod_less_than, TS1}}}, true},

        {{lastmod_less_than, TS2}, false},
        {{lnot, {lastmod_less_than, TS2}}, true},
        {{lnot, {lnot, {lastmod_less_than, TS2}}}, false}
    ].
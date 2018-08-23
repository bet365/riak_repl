-module(riak_repl2_object_filter).
-include("riak_repl.hrl").
-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
    start_link/0,
    enable/0,
    disable/0,
    clear_config/0,
    check_config/1,
    load_config/1,
    print_config/0]).

-export([
    get_maybe_downgraded_fullsync_config/2,
    get_maybe_downgraded_fullsync_config/3,
    get_realtime_blacklist/1,
    get_config/0,
    get_config/1,
    get_status/0,
    get_version/0,
    filter/2,
    filter/3
]).

-export([
    filter_object_rule_test/2
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(SUPPORTED_MATCH_TYPES, supported_match_types(?VERSION)).
-define(SUPPORTED_MATCH_VALUE_FORMATS(MatchType, MatchValue), supported_match_value_formats(?VERSION, MatchType, MatchValue)).
-define(SUPPORTED_KEYWORDS, supported_keywords(?VERSION)).
-define(WILDCARD, '*').
-define(STATUS, app_helper:get_env(riak_repl, object_filtering_status, disabled)).
-define(CONFIG, app_helper:get_env(riak_repl, object_filtering_config, [])).
-define(VERSION, app_helper:get_env(riak_repl, object_filtering_version, 0)).
-define(CLUSTERNAME, app_helper:get_env(riak_repl, clustername, "undefined")).
-define(CURRENT_VERSION, 1.0).

-define(ERROR_NO_FULES, {error, {no_rules, ?VERSION}}).
-define(ERROR_RULE_FORMAT(Rule), {error, {rule_format, ?VERSION, Rule}}).
-define(ERROR_DUPLICATE_REMOTE_ENTRIES, {error, {duplicate_remote_entries, ?VERSION}}).
-define(ERROR_INVALID_REMOTE_NAME(RemoteName), {error, {invalid_remote_name, ?VERSION, RemoteName}}).
-define(ERROR_INVALID_RULE(RemoteName, RuleType, Rule), invalid_rule(RemoteName, RuleType, Rule)).

-define(DEFAULT_FILTERING_RULES, {{whitelist, []}, {blacklist, []}, {matched_rules, {0,0}}}).
-define(DEFAULT_CONFIG(Remote), {Remote, {allow, ['*']}, {block, []}}).

-record(state, {}).

%%%===================================================================
%%% Macro Helper Functions
%%%===================================================================
supported_match_types(1.0) ->
    [bucket, metadata, not_bucket, not_metadata];
supported_match_types(_) ->
    [].

supported_match_value_formats(1.0, bucket, MatchValue) ->
    is_binary(MatchValue) or lists:member(MatchValue, ?SUPPORTED_KEYWORDS);
supported_match_value_formats(1.0, not_bucket, MatchValue) ->
    is_binary(MatchValue) or lists:member(MatchValue, ?SUPPORTED_KEYWORDS);
supported_match_value_formats(1.0, metadata, {_DictKey, _DictValue}) ->
    true;
supported_match_value_formats(1.0, not_metadata, {_DictKey, _DictValue}) ->
    true;
supported_match_value_formats(1.0, not_metadata, _) ->
    false;
supported_match_value_formats(0, _, _) ->
    false;
supported_match_value_formats(_, _, _) ->
    false.

supported_keywords(1.0) ->
    [all];
supported_keywords(_) ->
    [].

invalid_rule(RemoteName, allowed, Rule) -> {error, {invalid_rule_type_allowed, ?VERSION, RemoteName, Rule}};
invalid_rule(RemoteName, blocked, Rule) -> {error, {invalid_rule_type_blocked, ?VERSION, RemoteName, Rule}}.
%%%===================================================================
%%% API (Function Callbacks)
%%%===================================================================

%% returns true/false for a rule and an object
filter_object_rule_test(Rule, Object) ->
    filter_object_rule_check(Rule, get_object_data(Object)).

%% returns the entire config for all clusters
get_config() ->
    ?CONFIG.
%% returns config only for the remote that is named in the argument
get_config(RemoteName) ->
    case lists:keyfind(RemoteName, 1, ?CONFIG) of
        false -> ?DEFAULT_CONFIG(RemoteName);
        Rconfig -> Rconfig
    end.

get_maybe_downgraded_fullsync_config(Config, Version, RemoteName) ->
    {_, A, B} = Config,
    maybe_downgrade_config({RemoteName, A, B}, Version).
get_maybe_downgraded_fullsync_config(Config, Version) ->
    maybe_downgrade_config(Config, Version).


%% returns the status of our local cluster for object filtering
get_status()->
    ?STATUS.
%% returns the version of our local cluster for object filtering
get_version() ->
    ?VERSION.


% Returns true or false to say if we need to filter based on an object and remote name
filter({fullsync, disabled, _Version, _Config}, _Object) ->
    false;
filter({fullsync, enabled, 0, _Config}, _Object) ->
    false;
filter({fullsync, enabled, _Version, Config}, Object) ->
    filter_object_single_remote(Config, get_object_data(Object)).


%% returns a list of allowed and blocked remotes
get_realtime_blacklist(Object) ->
    F = fun({Remote, Allowed, Blocked}, Object) ->
        Filter = filter_object_single_remote({Remote, Allowed, Blocked}, get_object_data(Object)),
        {Remote, Filter}
        end,
    AllFilteredResults = [F({Remote, Allowed, Blocked}, Object) || {Remote, Allowed, Blocked} <- ?CONFIG],
    [Remote || {Remote, Filtered} <- AllFilteredResults, Filtered == false].

%% reutrns true or false to say if you can send an object to a remote name
filter(realtime, RemoteName, Meta) ->
    case orddict:find(?BT_META_BLACKLIST, Meta) of
        {ok, Blacklist} ->
            lists:member(RemoteName, Blacklist);
        _ ->
            false
    end.

%%%===================================================================
%%% API (Private) Helper Functions
%%%===================================================================

get_object_data(Object) ->
    Bucket = riak_object:bucket(Object),
    Metadatas = riak_object:get_metadatas(Object),
    {Bucket, Metadatas}.
get_object_bucket({Bucket, _})-> Bucket.
get_object_metadatas({_, MetaDatas}) -> MetaDatas.

filter_object_single_remote({_RemoteName, {allow, Allowed}, {block, Blocked}}, ObjectData) ->
    case filter_object_rule_check(Blocked, ObjectData) of
        true -> true;
        false -> not filter_object_rule_check(Allowed, ObjectData)
    end.

%% decision on whether or not the object data matches any of the rules
filter_object_rule_check([], _) -> false;
filter_object_rule_check([MultiRule| Rest], ObjectData) when is_list(MultiRule) ->
    case filter_object_check_multi_rule(MultiRule, ObjectData) of
        true -> true;
        false -> filter_object_rule_check(Rest, ObjectData)
    end;
filter_object_rule_check([Rule| Rest], ObjectData) ->
    case filter_object_check_single_rule(Rule, ObjectData) of
        true -> true;
        false -> filter_object_rule_check(Rest, ObjectData)
    end.

filter_object_check_multi_rule([], _) -> true;
filter_object_check_multi_rule([Rule | Rest], ObjectData) ->
    case filter_object_check_single_rule(Rule, ObjectData) of
        true -> filter_object_check_multi_rule(Rest, ObjectData);
        false -> false
    end.

filter_object_check_single_rule(Rule, ObjectData) ->
    MatchBucket = get_object_bucket(ObjectData),
    MatchMetaDatas = get_object_metadatas(ObjectData),
    case Rule of
        '*' ->                          true;
        {bucket, MatchBucket} ->        true;
        {bucket, all} ->                true;
        {bucket, _} ->                  false;
        {not_bucket, MatchBucket} ->    false;
        {not_bucket, all} ->            false;
        {not_bucket, _} ->              true;
        {metadata, {K, V}} ->           filter_object_check_metadatas(K, V, MatchMetaDatas);
        {not_metadata, {K, V}} ->       not filter_object_check_metadatas(K, V, MatchMetaDatas)
    end.

filter_object_check_metadatas(_, _, []) -> false;
filter_object_check_metadatas(Key, Value, [Metadata| Rest]) ->
    case filter_object_check_metadata(Key, Value, Metadata) of
        true -> true;
        false -> filter_object_check_metadatas(Key, Value, Rest)
    end.

filter_object_check_metadata(Key, all, Metadata) ->
    case dict:find(Key, Metadata) of
        {ok, _} -> true;
        error -> false
    end;
filter_object_check_metadata(Key, Value, Metadata) ->
    case dict:find(Key, Metadata) of
        {ok, Value} -> true;
        {ok, _} -> false;
        error -> false
    end.

%%%===================================================================
%%maybe_downgrade_config([], _Version) -> [];
%%maybe_downgrade_config([{Remote, {allow, AllowedRules}, {block, BlockedRules}} | Rest], Version) ->
%%    DowngradedAllowedRules = maybe_downgrade_rules(AllowedRules, Version, []),
%%    DowngradedBlockedRules = maybe_downgrade_rules(BlockedRules, Version, []),
%%    DowngradeRest = maybe_downgrade_config(Rest, Version),
%%    [{Remote, {allow, DowngradedAllowedRules}, {block, DowngradedBlockedRules}}] ++ DowngradeRest.

maybe_downgrade_config({Remote, {allow, AllowedRules}, {block, BlockedRules}}, Version) ->
    DowngradedAllowedRules = maybe_downgrade_rules(AllowedRules, Version, []),
    DowngradedBlockedRules = maybe_downgrade_rules(BlockedRules, Version, []),
    {Remote, {allow, DowngradedAllowedRules}, {block, DowngradedBlockedRules}}.

maybe_downgrade_rules([], _Version, Downgraded) -> Downgraded;
maybe_downgrade_rules([Rule| Rules], Version, Downgraded) ->
    case maybe_downgrade_rule(Rule, Version) of
        removed ->
            maybe_downgrade_rules(Rules, Version, Downgraded);
        R ->
            maybe_downgrade_rules(Rules, Version, Downgraded++[R])
    end.

maybe_downgrade_rule(Rule, Version) when is_list(Rule) ->
    case maybe_downgrade_multi_rule(Rule, Version) of
        true -> Rule;
        false -> removed
    end;
maybe_downgrade_rule(Rule, Version) ->
    case maybe_downgrade_single_rule(Rule, Version) of
        true -> Rule;
        false -> removed
    end.

maybe_downgrade_multi_rule([], _Version) -> true;
maybe_downgrade_multi_rule([Rule | Rest], Version)->
    maybe_downgrade_single_rule(Rule, Version) and maybe_downgrade_multi_rule(Rest, Version).
maybe_downgrade_single_rule(Rule, Version) ->
    case Rule of
        ?WILDCARD -> true;
        {MatchType, MatchValue} -> supported_match_value_formats(Version, MatchType, MatchValue)
    end.
%%%===================================================================

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).
enable()->
    gen_server:call(?SERVER, enable).
disable()->
    gen_server:call(?SERVER, disable).
clear_config() ->
    gen_server:call(?SERVER, clear_config).
check_config(ConfigFilePath) ->
    gen_server:call(?SERVER, {check_config, ConfigFilePath}).
load_config(ConfigFilePath) ->
    gen_server:call(?SERVER, {load_config, ConfigFilePath}).
print_config() ->
    gen_server:call(?SERVER, print_config).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    {Status, Config} = riak_repl_ring:get_object_filtering_data(),
    application:set_env(riak_repl, object_filtering_status, Status),
    application:set_env(riak_repl, object_filtering_config, Config),
    Version = riak_core_capability:get({riak_repl, object_filtering_version}, 0),
    application:set_env(riak_repl, object_filtering_version, Version),
    application:set_env(riak_repl, clustername, riak_core_connection:symbolic_clustername()),
    case Version == ?CURRENT_VERSION of
        false ->
            erlang:send_after(5000, self(), poll_core_capability);
        true ->
            ok
    end,
    {ok, #state{}}.

handle_call(Request, _From, State) ->
    Response = case Request of
                   enable ->
                       object_filtering_enable();
                   disable ->
                       object_filtering_disable();
                   {check_config, Path} ->
                       object_filtering_config_file(check, Path);
                   {load_config, Path} ->
                       object_filtering_config_file(load, Path);
                   clear_config ->
                       object_filtering_clear_config();
                   print_config ->
                       object_filtering_config();
                   _ ->
                       error
               end,
    {reply, Response, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(poll_core_capability, State) ->
    Version = riak_core_capability:get({riak_repl, object_filtering_version}, 0),
    case Version == ?CURRENT_VERSION of
        true ->
            erlang:send_after(5000, self(), poll_core_capability);
        false ->
            application:set_env(riak_repl, object_filtering_version, Version)
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
object_filtering_disable() ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, disabled),
    application:set_env(riak_repl, object_filtering_status, disabled),
    ok.

object_filtering_enable() ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, enabled),
    application:set_env(riak_repl, object_filtering_status, enabled),
    ok.

object_filtering_config_file(Action, Path) ->
    case file:consult(Path) of
        {ok, FilteringConfig} ->
            case check_filtering_rules(FilteringConfig) of
                ok ->
                    case Action of
                        check -> ok;
                        load ->
                            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_config/2, FilteringConfig),
                            application:set_env(riak_repl, object_filtering_config, FilteringConfig),
                            ok
                    end;
                Error2 ->
                    Error2
            end;
        Error1 ->
            Error1
    end.

object_filtering_config() ->
    {print_config, {?VERSION, ?STATUS, ?CONFIG}}.

object_filtering_clear_config() ->
    application:set_env(riak_repl, object_filtering_config, []),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_config/2, []),
    ok.


check_filtering_rules([]) -> ?ERROR_NO_FULES();
check_filtering_rules(FilteringRules) ->
    AllRemotes = lists:sort(lists:foldl(fun({RemoteName, _, _}, Acc) -> [RemoteName] ++ Acc end, [], FilteringRules)),
    NoDuplicateRemotes = lists:usort(AllRemotes),
    case AllRemotes == NoDuplicateRemotes of
        true ->
            check_filtering_rules_helper(FilteringRules, check_rule_format, ok);
        false ->
            ?ERROR_DUPLICATE_REMOTE_ENTRIES
    end.

check_filtering_rules_helper(R, complete, Outcome) when length(R) == 1 -> Outcome;
check_filtering_rules_helper(_, _NextCheck, {error, Error}) -> {error, Error};
check_filtering_rules_helper(Rules = [_|Rest], NextCheck, ok) ->
    case NextCheck of
        complete ->
            check_filtering_rules_helper(Rest, check_rule_format, ok);
        check_rule_format ->
            check_rule_format(Rules, check_remote_name);
        check_remote_name ->
            check_remote_name(Rules, check_allowed_rules);
        check_allowed_rules ->
            check_rules(allowed, Rules, check_blocked_rules);
        check_blocked_rules ->
            check_rules(blocked, Rules, complete)
    end.

check_rule_format(Rules = [{_RemoteName, {allow, _AllowedRule}, {block, _BlockedRules}} | _RestOfRemotes], NextCheck) ->
    check_filtering_rules_helper(Rules, NextCheck, ok);
check_rule_format(Rules = [Rule|_R], NextCheck) ->
    check_filtering_rules_helper(Rules, NextCheck, ?ERROR_RULE_FORMAT(Rule)).

check_remote_name(Rules = [{RemoteName, _, _} | _RestOfRemotes], NextCheck) ->
    Check = lists:foldl(fun(E, Acc) ->
                            case {is_integer(E), Acc} of
                                {true, []} ->
                                    [true];
                                {true, [true]} ->
                                    [true];
                                {true, _} ->
                                    Acc;
                                {false, _} ->
                                    [false]
                            end
                        end, [], RemoteName),
    case Check of
        [true] ->
            check_filtering_rules_helper(Rules, NextCheck, ok);
        [false] ->
            check_filtering_rules_helper(Rules, NextCheck, ?ERROR_INVALID_REMOTE_NAME(RemoteName))
    end.

check_rules(allowed, Rules = [{RemoteName, {allow, AllowedRules}, _} | _], NextCheck) ->
    case AllowedRules of
        [?WILDCARD] ->
            check_filtering_rules_helper(Rules, NextCheck, ok);
        _ ->
            check_filtering_rules_helper(Rules, NextCheck, check_rules_helper(RemoteName, allowed, AllowedRules))
    end;
check_rules(blocked, Rules = [{RemoteName, _, {block, BlockedRules}} | _], NextCheck) ->
    case BlockedRules of
        [?WILDCARD] ->
            check_filtering_rules_helper(Rules, NextCheck, ok);
        _ ->
            check_filtering_rules_helper(Rules, NextCheck, check_rules_helper(RemoteName, blocked, BlockedRules))
    end.

check_rules_helper(_RemoteName, _RuleType, []) ->
    ok;
check_rules_helper(RemoteName, RuleType, [Rule | Rest]) ->
    case is_rule_supported(Rule) of
        true ->
            check_rules_helper(RemoteName, RuleType, Rest);
        false ->
            ?ERROR_INVALID_RULE(RemoteName, RuleType, Rule)
    end.

is_rule_supported(Rule) when (is_list(Rule) and (length(Rule) > 1)) -> is_multi_rule_supported(Rule);
is_rule_supported(Rule) -> is_single_rule_supported(Rule).

is_single_rule_supported({MatchType, MatchValue}) ->
    lists:member(MatchType, ?SUPPORTED_MATCH_TYPES) and ?SUPPORTED_MATCH_VALUE_FORMATS(MatchType, MatchValue);
is_single_rule_supported(_) -> false.

is_multi_rule_supported([]) -> true;
is_multi_rule_supported([Rule|Rest]) ->
    is_single_rule_supported(Rule) and is_multi_rule_supported(Rest).






%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

object_filter_test_() ->
    {setup,
     fun setup/0,
     [
         {"Single Rules", fun test_object_filter_single_rules/0},
         {"Multi Rules (bucket, metadata)", fun test_object_filter_multi_rules_bucket_metadata/0 },
         {"Multi Rules (bucket, not_metadata)", fun test_object_filter_multi_rules_bucket_not_metadata/0 },
         {"Multi Rules (not_bucket, metadata)", fun test_object_filter_multi_rules_not_bucket_metadata/0 },
         {"Multi Rules (not_bucket, not_metadata)", fun test_object_filter_multi_rules_not_bucket_not_metadata/0 }
     ]
    }.

setup() ->
    riak_repl_test_util:start_test_ring(),
    riak_repl_test_util:start_lager().

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
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(3, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {filter, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(4, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {filter, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(5, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {filter, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(6, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {filter, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(7, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(8, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(9, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {filter, all}}]], Obj),
    ?assertEqual(false, Actual);

test_object_filter_multi_rules_not_bucket_not_metadata(10, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {other, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(11, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {other, 1}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(12, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {other, 1}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(13, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {other, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(14, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {other, 2}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(15, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {other, 2}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(16, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"bucket">>}, {not_metadata, {other, all}}]], Obj),
    ?assertEqual(false, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(17, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, <<"anything">>}, {not_metadata, {other, all}}]], Obj),
    ?assertEqual(true, Actual);
test_object_filter_multi_rules_not_bucket_not_metadata(18, Obj)->
    Actual = filter_object_rule_test([[{not_bucket, all}, {not_metadata, {other, all}}]], Obj),
    ?assertEqual(false, Actual).
%% ===================================================================





-endif.
-module(riak_repl2_object_filter).
-include("riak_repl.hrl").
-include("riak_repl2_object_filter.hrl").


%% internal API function calls
-export([
    get_maybe_downgraded_remote_config/2,
    get_maybe_downgraded_config/2,
    get_config/1,
    get_config/2,
    get_config/3,
    get_status/1,
    get_version/0,
    fullsync_filter/2,
    get_realtime_blacklist/1,
    realtime_filter/2
]).

%% =================================================
%% API (for testing rules)
-export([
    filter_object_rule_test/2
]).



%%%===================================================================
%%% API
%%%===================================================================

%% returns true/false for a rule and an object
filter_object_rule_test(Rule, Object) ->
    filter_object_rule_check(Rule, get_object_data(Object)).



%% returns the entire config for all clusters
get_config(fullsync) ->
    app_helper:get_env(riak_repl, object_filtering_merged_fullsync_config, []);
get_config(realtime) ->
    app_helper:get_env(riak_repl, object_filtering_merged_realtime_config, []);
get_config(loaded_repl) ->
    app_helper:get_env(riak_repl, object_filtering_repl_config, []);
get_config(loaded_realtime) ->
    app_helper:get_env(riak_repl, object_filtering_realtime_config, []);
get_config(loaded_fullsync) ->
    app_helper:get_env(riak_repl, object_filtering_fullsync_config, []);
get_config(_) ->
    [].


%% returns config only for the remote that is named in the argument
get_config(ReplMode, RemoteName) ->
    get_config(ReplMode, RemoteName, undefined).
get_config(ReplMode, RemoteName, TimeStamp) ->
    Config = case ReplMode of
                 fullsync -> get_config(fullsync);
                 realtime -> get_config(realtime);
                 loaded_repl -> get_config(loaded_repl);
                 loaded_realtime -> get_config(loaded_realtime);
                 loaded_fullsync -> get_config(loaded_fullsync)
             end,

    ResConfig = case lists:keyfind(RemoteName, 1, Config) of
                    false -> ?DEFAULT_CONFIG(RemoteName);
                    Rconfig -> Rconfig
                end,

    maybe_set_lastmod_age(ReplMode, ResConfig, TimeStamp).


get_maybe_downgraded_remote_config(Config, RemoteName) ->
    {_, A, B} = Config,
    maybe_downgrade_config({RemoteName, A, B}, get_version()).
get_maybe_downgraded_config(Config, Version) ->
    maybe_downgrade_config(Config, Version).


%% returns the status of our local cluster for object filtering
get_status(realtime) ->
    app_helper:get_env(riak_repl, object_filtering_realtime_status, disabled);
get_status(fullsync) ->
    app_helper:get_env(riak_repl, object_filtering_fullsync_status, disabled).


%% returns the version of our local cluster for object filtering
get_version() ->
    app_helper:get_env(riak_repl, object_filtering_version, 0).




% Returns true or false to say if we need to filter based on an object and remote name
fullsync_filter({disabled, _Version, _Config}, _Object) ->
    false;
fullsync_filter({enabled, 0, _Config}, _Object) ->
    false;
fullsync_filter({enabled, _Version, Config}, Object) ->
    filter_object_single_remote(Config, get_object_data(Object)).


%% returns a list of allowed and blocked remotes
get_realtime_blacklist(Object) ->
    case get_status(realtime) of
        enabled ->
            F = fun({Remote, Allowed, Blocked}, Obj) ->
                Filter = filter_object_single_remote({Remote, Allowed, Blocked}, get_object_data(Obj)),
                {Remote, Filter}
                end,
            AllFilteredResults = [F({Remote, Allowed, Blocked}, Object) || {Remote, Allowed, Blocked} <- get_config(realtime)],
            [Remote || {Remote, Filtered} <- AllFilteredResults, Filtered == true];
        _ ->
            []
    end.

%% reutrns true or false to say if you can send an object to a remote name
realtime_filter(RemoteName, Meta) ->
    case get_status(realtime) of
        enabled ->
            case orddict:find(?BT_META_BLACKLIST, Meta) of
                {ok, Blacklist} ->
                    lists:member(RemoteName, Blacklist);
                _ ->
                    false
            end;
        _ ->
            false
    end.

%%%===================================================================
%%% Helper Functions
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

filter_object_rule_check([], _) -> false;
filter_object_rule_check([MultiRule| Rest], ObjectData) when is_list(MultiRule) ->
    case filter_object_check_multi_rule(MultiRule, ObjectData, []) of
        true -> true;
        false -> filter_object_rule_check(Rest, ObjectData)
    end;
filter_object_rule_check([Rule| Rest], ObjectData) ->
    case filter_object_check_single_rule(Rule, ObjectData) of
        true -> true;
        false -> filter_object_rule_check(Rest, ObjectData)
    end.

filter_object_check_multi_rule([], _, Results) ->
    lists:all(fun(Elem) -> Elem end, Results);
filter_object_check_multi_rule([Rule | Rest], ObjectData, Results) ->
    R = filter_object_check_single_rule(Rule, ObjectData),
    filter_object_check_multi_rule(Rest, ObjectData, [R | Results]).

filter_object_check_single_rule({lnot, Rule}, ObjectData) ->
    not filter_object_rule_check([Rule], ObjectData);
filter_object_check_single_rule(Rule, ObjectData) ->
    MatchBucket = get_object_bucket(ObjectData),
    MatchMetaDatas = get_object_metadatas(ObjectData),
    case Rule of
        '*' ->                              true;
        {bucket, MatchBucket} ->            true;
        {bucket, _} ->                      false;
        {metadata, {K}} ->                  filter_object_check_metadatas(K, any, MatchMetaDatas);
        {metadata, {K, V}} ->               filter_object_check_metadatas(K, V, MatchMetaDatas);
        {lastmod_age_greater_than, Age} ->  filter_object_lastmod_age(greater, Age, MatchMetaDatas);
        {lastmod_age_less_than, Age} ->     filter_object_lastmod_age(less, Age, MatchMetaDatas);
        {lastmod_greater_than, TS} ->       filter_object_lastmod(greater, TS, MatchMetaDatas);
        {lastmod_less_than, TS} ->          filter_object_lastmod(less, TS, MatchMetaDatas)
    end.

filter_object_check_metadatas(_, _, []) -> false;
filter_object_check_metadatas(Key, Value, [Metadata| Rest]) ->
    case filter_object_check_metadata(Key, Value, Metadata) of
        true -> true;
        false -> filter_object_check_metadatas(Key, Value, Rest)
    end.

filter_object_check_metadata(Key, any, Metadata) ->
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

filter_object_lastmod_age(Mode, Age, MetaDatas) ->
    NowSecs = timestamp_to_secs(os:timestamp()),
    TS = NowSecs + Age,
    filter_object_lastmod(Mode, TS, MetaDatas).

filter_object_lastmod(Mode, FilterTS, MetaDatas) ->
    AllTimeStamps = lists:foldl(fun(Dict, Acc) ->
        case dict:find(?LASTMOD, Dict) of
            {ok, TS} -> [timestamp_to_secs(TS) | Acc];
            _ -> Acc
        end end, [], MetaDatas),
    ObjectTS = lists:max(AllTimeStamps),

    case Mode of
        greater -> ObjectTS >= FilterTS;
        less -> ObjectTS =< FilterTS
    end.


%%%===================================================================
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
        {MatchType, MatchValue} ->
            riak_repl2_object_filter_console:supported_match_value_formats(Version, MatchType, MatchValue)
    end.
%% ================================================================================================================== %%
timestamp_to_secs({M, S, _}) ->
    M * 1000000 + S.


maybe_set_lastmod_age(fullsync, Config, undefined) ->
    Config;
maybe_set_lastmod_age(fullsync, Config, Timestamp) ->
    set_lastmod_age(Config, Timestamp);
maybe_set_lastmod_age(_, Config, _) ->
    Config.

set_lastmod_age({RemoteName, {allow, Allowed}, {block, Blocked}}, TimeStamp) ->
    Now = timestamp_to_secs(TimeStamp),
    UpdatedAllowed = lists:reverse(set_lastmod_age_helper(Allowed, Now, [])),
    UpdatedBlocked = lists:reverse(set_lastmod_age_helper(Blocked, Now, [])),
    {RemoteName, {allow, UpdatedAllowed}, {block, UpdatedBlocked}}.

set_lastmod_age_helper([], _, OutRules) -> OutRules;
set_lastmod_age_helper([Rule | Rules], Now, OutRules) when is_list(Rule) ->
    OutRules1 = set_lastmod_age_multi(Rule, Now, OutRules),
    set_lastmod_age_helper(Rules, Now, OutRules1);
set_lastmod_age_helper([Rule | Rules], Now, OutRules) ->
    OutRules1 = set_lastmod_age_single(Rule, Now, OutRules),
    set_lastmod_age_helper(Rules, Now, OutRules1).

set_lastmod_age_multi(RuleList, Now, OutRules) ->
    Multi = lists:reverse(lists:foldl(fun(Rule, Acc) -> set_lastmod_age_single(Rule, Now, Acc) end, [], RuleList)),
    [Multi | OutRules].

set_lastmod_age_single({lnot, Rule}, Now, OutRules) ->
    [UpdatedRule] = set_lastmod_age_helper([Rule], Now, []),
    [{lnot, UpdatedRule} | OutRules];
set_lastmod_age_single({lastmod_age_greater_than, Age}, Now, OutRules) ->
    TS = Now + Age,
    [{lastmod_greater_than, TS} | OutRules];
set_lastmod_age_single({lastmod_age_less_than, Age}, Now, OutRules) ->
    TS = Now + Age,
    [{lastmod_less_than, TS} | OutRules];
set_lastmod_age_single(Rule, _, OutRules) ->
    [Rule | OutRules].
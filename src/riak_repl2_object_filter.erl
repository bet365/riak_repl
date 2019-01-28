-module(riak_repl2_object_filter).
-include("riak_repl.hrl").
-behaviour(gen_server).

%% =================================================
%% repl_console function calls
-export([
    status/0,
    status_all/0,
    get_config_external/1,
    get_config_external/2
]).


%% internal API function calls
-export([
    get_maybe_downgraded_remote_config/2,
    get_maybe_downgraded_config/2,
    get_realtime_blacklist/1,
    get_config/1,
    get_config/2,
    get_config/3,
    get_status/1,
    get_version/0,
    filter/2,
    filter/3
]).

%% =================================================
%% API (for testing rules)
-export([
    filter_object_rule_test/2
]).

%% =================================================
%% API (gen_server calls)
-export([
    start_link/0,
    enable/0,
    disable/0,
    enable/1,
    disable/1,
    clear_config/1,
    check_config/1,
    load_config/2,
    ring_update/2
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).
%% =================================================


-define(SERVER, ?MODULE).
-define(LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(SUPPORTED_MATCH_TYPES, supported_match_types(?VERSION)).
-define(SUPPORTED_MATCH_VALUE_FORMATS(MatchType, MatchValue), supported_match_value_formats(?VERSION, MatchType, MatchValue)).
-define(SUPPORTED_KEYWORDS, supported_keywords(?VERSION)).
-define(WILDCARD, '*').

-define(RT_STATUS, app_helper:get_env(riak_repl, object_filtering_realtime_status, disabled)).
-define(FS_STATUS, app_helper:get_env(riak_repl, object_filtering_fullsync_status, disabled)).

-define(VERSION, app_helper:get_env(riak_repl, object_filtering_version, 0)).
-define(REPL_CONFIG, app_helper:get_env(riak_repl, object_filtering_repl_config, [])).
-define(RT_CONFIG, app_helper:get_env(riak_repl, object_filtering_realtime_config, [])).
-define(FS_CONFIG, app_helper:get_env(riak_repl, object_filtering_fullsync_config, [])).
-define(MERGED_RT_CONFIG, app_helper:get_env(riak_repl, object_filtering_merged_realtime_config, [])).
-define(MERGED_FS_CONFIG, app_helper:get_env(riak_repl, object_filtering_merged_fullsync_config, [])).
-define(CLUSTERNAME, app_helper:get_env(riak_repl, clustername, "undefined")).

-define(RT_STATUS(S), application:set_env(riak_repl, object_filtering_realtime_status, S)).
-define(FS_STATUS(S), application:set_env(riak_repl, object_filtering_fullsync_status, S)).

-define(VERSION(V), application:set_env(riak_repl, object_filtering_version, V)).
-define(REPL_CONFIG(C), application:set_env(riak_repl, object_filtering_repl_config, C)).
-define(RT_CONFIG(C), application:set_env(riak_repl, object_filtering_realtime_config, C)).
-define(FS_CONFIG(C), application:set_env(riak_repl, object_filtering_fullsync_config, C)).
-define(MERGED_RT_CONFIG(C), application:set_env(riak_repl, object_filtering_merged_realtime_config, C)).
-define(MERGED_FS_CONFIG(C), application:set_env(riak_repl, object_filtering_merged_fullsync_config, C)).
-define(CLUSTERNAME(CName), application:set_env(riak_repl, clustername, CName)).

-define(CURRENT_VERSION, 1.0).

-define(ERROR_NO_FULES, {error, {no_rules, ?VERSION}}).
-define(ERROR_RULE_FORMAT(Rule), {error, {rule_format, ?VERSION, Rule}}).
-define(ERROR_DUPLICATE_REMOTE_ENTRIES, {error, {duplicate_remote_entries, ?VERSION}}).
-define(ERROR_INVALID_REMOTE_NAME(RemoteName), {error, {invalid_remote_name, ?VERSION, RemoteName}}).
-define(ERROR_INVALID_RULE(RemoteName, RuleType, Rule), invalid_rule(RemoteName, RuleType, Rule)).
-define(ERROR_UNKNOWN_REPL_MODE(RelatedFun, Mode), {error, unknown_repl_mode, RelatedFun, Mode}).

-define(DEFAULT_CONFIG(Remote), {Remote, {allow, ['*']}, {block, []}}).
-define(TIMEOUT, 10000).
-record(state, {}).

%%%===================================================================
%%% Macro Helper Functions
%%%===================================================================
supported_match_types(1.0) ->
    [
        bucket,
        metadata,
        not_bucket,
        not_metadata,
        lastmod_age_greater_than,
        lastmod_age_less_than,
        lastmod_greater_than,
        lastmod_less_than
    ];
supported_match_types(_) ->
    [].

%% Typed bucket
supported_match_value_formats(1.0, bucket, {MatchValue1, MatchValue2}) ->
    is_binary(MatchValue1) and is_binary(MatchValue2);
%% Bucket
supported_match_value_formats(1.0, bucket, MatchValue) ->
    is_binary(MatchValue) or lists:member(MatchValue, ?SUPPORTED_KEYWORDS);
%% Typed Bucket
supported_match_value_formats(1.0, not_bucket, {MatchValue1, MatchValue2}) ->
    is_binary(MatchValue1) and is_binary(MatchValue2);
%% Bucket
supported_match_value_formats(1.0, not_bucket, MatchValue) ->
    is_binary(MatchValue) or lists:member(MatchValue, ?SUPPORTED_KEYWORDS);

supported_match_value_formats(1.0, metadata, {_DictKey, _DictValue}) ->
    true;
supported_match_value_formats(1.0, not_metadata, {_DictKey, _DictValue}) ->
    true;
supported_match_value_formats(1.0, not_metadata, _) ->
    false;

supported_match_value_formats(1.0, lastmod_age_greater_than, Age) ->
    is_integer(Age);
supported_match_value_formats(1.0, lastmod_age_less_than, Age) ->
    is_integer(Age);
supported_match_value_formats(1.0, lastmod_greater_than, TS) ->
    is_integer(TS);
supported_match_value_formats(1.0, lastmod_less_than, TS) ->
    is_integer(TS);

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

% returns config to repl_console or errors out
get_config_external(Mode) ->
    ConvertedMode = list_to_atom(Mode),
    List = [fullsync, realtime, loaded_repl, loaded_realtime, loaded_fullsync],
    case lists:member(ConvertedMode, List) of
        true -> {config, get_config(ConvertedMode)};
        false -> ?ERROR_UNKNOWN_REPL_MODE(get_config_external, Mode)
    end.
get_config_external(Mode, Remote) ->
    ConvertedMode = list_to_atom(Mode),
    List = [fullsync, realtime, loaded_repl, loaded_realtime, loaded_fullsync],
    case lists:member(ConvertedMode, List) of
        true -> {config, get_config(ConvertedMode, Remote)};
        false -> ?ERROR_UNKNOWN_REPL_MODE(get_config_external, Mode)
    end.

%% returns the entire config for all clusters
get_config(fullsync) ->
    ?MERGED_FS_CONFIG;
get_config(realtime) ->
    ?MERGED_RT_CONFIG;
get_config(loaded_repl) ->
    ?REPL_CONFIG;
get_config(loaded_realtime) ->
    ?RT_CONFIG;
get_config(loaded_fullsync) ->
    ?FS_CONFIG;
get_config(_) ->
    [].
%% returns config only for the remote that is named in the argument
get_config(ReplMode, RemoteName) ->
    get_config(ReplMode, RemoteName, undefined).
get_config(ReplMode, RemoteName, TimeStamp) ->
    Config = case ReplMode of
                 fullsync -> ?MERGED_FS_CONFIG;
                 realtime -> ?MERGED_RT_CONFIG;
                 loaded_repl -> ?REPL_CONFIG;
                 loaded_realtime -> ?RT_CONFIG;
                 loaded_fullsync -> ?FS_CONFIG
             end,

    ResConfig = case lists:keyfind(RemoteName, 1, Config) of
                    false -> ?DEFAULT_CONFIG(RemoteName);
                    Rconfig -> Rconfig
                end,

    maybe_set_lastmod_age(ReplMode, ResConfig, TimeStamp).


get_maybe_downgraded_remote_config(Config, RemoteName) ->
    {_, A, B} = Config,
    maybe_downgrade_config({RemoteName, A, B}, ?VERSION).
get_maybe_downgraded_config(Config, Version) ->
    maybe_downgrade_config(Config, Version).


%% returns the status of our local cluster for object filtering
get_status(realtime) ->
    ?RT_STATUS;
get_status(fullsync) ->
    ?FS_STATUS.
%% returns the version of our local cluster for object filtering
get_version() ->
    ?VERSION.

%% Function calls for repl_console
status() ->
    {status_single_node,
        {node(),
            {
                ?VERSION,
                ?RT_STATUS,
                ?FS_STATUS,
                erlang:phash2(?MERGED_RT_CONFIG),
                erlang:phash2(?MERGED_FS_CONFIG),
                erlang:phash2(?REPL_CONFIG),
                erlang:phash2(?FS_CONFIG),
                erlang:phash2(?RT_CONFIG)
            }
        }
    }.

status_all() ->
    {StatusAllNodes, _} = riak_core_util:rpc_every_member(riak_repl2_object_filter, status, [], ?TIMEOUT),
    Result = [R ||{status_single_node, R} <- StatusAllNodes],
    {status_all_nodes, lists:sort(Result)}.



% Returns true or false to say if we need to filter based on an object and remote name
filter({fullsync, disabled, _Version, _Config}, _Object) ->
    false;
filter({fullsync, enabled, 0, _Config}, _Object) ->
    false;
filter({fullsync, enabled, _Version, Config}, Object) ->
    filter_object_single_remote(Config, get_object_data(Object)).


%% returns a list of allowed and blocked remotes
get_realtime_blacklist(Object) ->
    case ?RT_STATUS of
        enabled ->
            F = fun({Remote, Allowed, Blocked}, Obj) ->
                Filter = filter_object_single_remote({Remote, Allowed, Blocked}, get_object_data(Obj)),
                {Remote, Filter}
                end,
            AllFilteredResults = [F({Remote, Allowed, Blocked}, Object) || {Remote, Allowed, Blocked} <- ?MERGED_RT_CONFIG],
            [Remote || {Remote, Filtered} <- AllFilteredResults, Filtered == true];
        _ ->
            []
    end.

%% reutrns true or false to say if you can send an object to a remote name
filter(realtime, RemoteName, Meta) ->
    case ?RT_STATUS of
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

filter_object_rule_check([], _) -> false;
filter_object_rule_check([MultiRule| Rest], ObjectData) when is_list(MultiRule) ->
    case filter_object_check_multi_rule(MultiRule, ObjectData, []) of
        true -> true;
        false -> filter_object_rule_check(Rest, ObjectData)
    end;
filter_object_rule_check([Rule| Rest], ObjectData) ->
    {R1, R2} = filter_object_check_single_rule(Rule, ObjectData),
    case evaluate_single_rule_result(R1, R2) of
        true -> true;
        false -> filter_object_rule_check(Rest, ObjectData)
    end.

filter_object_check_multi_rule([], _, Results) -> evaluate_multi_rule_results(Results);
filter_object_check_multi_rule([Rule | Rest], ObjectData, Results) ->
    R = filter_object_check_single_rule(Rule, ObjectData),
    filter_object_check_multi_rule(Rest, ObjectData, Results++[R]).

evaluate_single_rule_result(R1, R2) ->
    case {R1, R2} of
        {true, X} -> X;
        {false, X} -> not X
    end.
evaluate_multi_rule_results(Results) ->
    [{A, B, C, D}] = lists:foldl(
        fun({A1,B1}, [{A21, A22, B21, B22}]) ->
            case {A1, B1} of
                {true, _} -> [{(A1 or A21), (A1 and A22), (B1 and B21), (B1 and B22)}];
                {false, _} -> [{(A1 or A21), (A1 and A22), (B1 and B21), (not B1 and B22)}]
            end
        end, [{false, true, true, true}], Results),
    case {A, B, C, D} of
        {true, true, _, _} -> C;
        {true, false, _, _} -> D;
        {false, true, _, _} -> D;
        {false, false, _, _} -> not C
    end.

%% {is_not_rule, should_filter}
filter_object_check_single_rule(Rule, ObjectData) ->
    MatchBucket = get_object_bucket(ObjectData),
    MatchMetaDatas = get_object_metadatas(ObjectData),
    case Rule of
        '*' ->                          {true, true};
        {bucket, MatchBucket} ->        {true, true};
        {bucket, all} ->                {true, true};
        {bucket, _} ->                  {true, false};
        {not_bucket, MatchBucket} ->    {false, true};
        {not_bucket, all} ->            {false, true};
        {not_bucket, _} ->              {false, false};
        {metadata, {K, V}} ->           {true, filter_object_check_metadatas(K, V, MatchMetaDatas)};
        {not_metadata, {K, V}} ->       {false, filter_object_check_metadatas(K, V, MatchMetaDatas)};
        {lastmod_age_greater_than, Age} ->           {false, filter_object_lastmod_age(greater, Age, MatchMetaDatas)};
        {lastmod_age_less_than, Age} ->           {false, filter_object_lastmod_age(less, Age, MatchMetaDatas)};
        {lastmod_greater_than, TS} ->   {false, filter_object_lastmod(greater, TS, MatchMetaDatas)};
        {lastmod_less_than, TS} ->   {false, filter_object_lastmod(less, TS, MatchMetaDatas)}
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

filter_object_lastmod_age(Mode, Age, MetaDatas) ->
    NowSecs = timestamp_to_secs(os:timestamp()),
    TS = NowSecs - Age,
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
        {MatchType, MatchValue} -> supported_match_value_formats(Version, MatchType, MatchValue)
    end.
%% ================================================================================================================== %%
timestamp_to_secs({M, S, _}) ->
    {M, S, _} = os:timestamp(),
    M * 1000000 + S.

maybe_set_lastmod_age(fullsync, Config, undefined) ->
    Config;
maybe_set_lastmod_age(fullsync, Config, Timestamp) ->
    set_lastmod_age(Config, Timestamp);
maybe_set_lastmod_age(_, Config, _) ->
    Config.

set_lastmod_age({RemoteName, {allow, Allowed}, {block, Blocked}}, TimeStamp) ->
    Now = timestamp_to_secs(TimeStamp),
    UpdatedAllowed = lists:reverse(allow, set_lastmod_age_helper(Allowed, Now, [])),
    UpdatedBlocked = lists:reverse(block, set_lastmod_age_helper(Blocked, Now, [])),
    {RemoteName, {allow, UpdatedAllowed}, {block, UpdatedBlocked}}.

set_lastmod_age_helper([Rule | Rules], Now, OutRules) when is_list(Rule) ->
    OutRules1 = set_lastmod_age_multi(Rule, Now, OutRules),
    set_lastmod_age_helper(Rules, Now, OutRules1);
set_lastmod_age_helper([Rule | Rules], Now, OutRules) ->
    OutRules1 = set_lastmod_age_single(Rule, Now, OutRules),
    set_lastmod_age_helper(Rules, Now, OutRules1).

set_lastmod_age_multi(RuleList, Now, OutRules) ->
    Multi = lists:reverse(lists:foldl(fun(Rule, Acc) -> set_lastmod_age_single(Rule, Now, Acc) end, [], RuleList)),
    [Multi | OutRules].

set_lastmod_age_single({lastmod_age_greater_than, Age}, Now, OutRules) ->
    TS = Now - Age,
    [{lastmod_greater_than, TS} | OutRules];
set_lastmod_age_single({lastmod_age_less_than, Age}, Now, OutRules) ->
    TS = Now - Age,
    [{lastmod_less_than, TS} | OutRules];
set_lastmod_age_single(Rule, _, OutRules) ->
    [Rule | OutRules].





%%%===================================================================
%%% gen_server API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

check_config(ConfigFilePath) ->
    gen_server:call(?SERVER, {check_config, ConfigFilePath}, ?TIMEOUT).
load_config(ReplMode, ConfigFilePath) ->
    gen_server:call(?SERVER, {load_config, ReplMode, ConfigFilePath}, ?TIMEOUT).
enable()->
    gen_server:call(?SERVER, enable, ?TIMEOUT).
enable(Mode)->
    gen_server:call(?SERVER, {enable, Mode}, ?TIMEOUT).
disable()->
    gen_server:call(?SERVER, disable, ?TIMEOUT).
disable(Mode)->
    gen_server:call(?SERVER, {disable, Mode}, ?TIMEOUT).

clear_config(Mode) ->
    gen_server:cast(?SERVER, {clear_config, Mode}).
ring_update(NewStatus, NewConfigs) ->
    gen_server:cast(?SERVER, {ring_update, NewStatus, NewConfigs}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    load_ring_configs_and_status(),
    Version = riak_core_capability:get({riak_repl, object_filtering_version}, 0),
    ?VERSION(Version),
    ?CLUSTERNAME(riak_core_connection:symbolic_clustername()),
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
                       object_filtering_enable(repl);
                   {enable, Mode} ->
                       object_filtering_enable(list_to_atom(Mode));
                   disable ->
                       object_filtering_disable(repl);
                   {disable, Mode} ->
                       object_filtering_disable(list_to_atom(Mode));
                   {check_config, Path} ->
                       object_filtering_config_file(check, Path);
                   {load_config, Mode, Path} ->
                       object_filtering_config_file({load, list_to_atom(Mode)}, Path);
                   _ ->
                       error
               end,
    {reply, Response, State}.

handle_cast(Request, State) ->
    case Request of
        {clear_config, Mode} ->
            object_filtering_clear_config(list_to_atom(Mode));
        {ring_update, Statuses, Configs} ->
            ring_update_update_configs(Statuses, Configs);
        _ ->
            ok
    end,
    {noreply, State}.

handle_info(poll_core_capability, State) ->
    Version = riak_core_capability:get({riak_repl, object_filtering_version}, 0),
    case Version == ?CURRENT_VERSION of
        false ->
            erlang:send_after(5000, self(), poll_core_capability);
        true ->
            ?VERSION(Version)
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% gen_server Internal functions
%%%===================================================================
ring_update_update_configs({NewRTStatus, NewFSStatus}, {NewReplConfig, NewRTConfig, NewFSConfig, NewMergedRTConfig, NewMergedFSConfig}) ->
    List =
        [
            {NewRTStatus, ?RT_STATUS, object_filtering_realtime_status},
            {NewFSStatus, ?FS_STATUS, object_filtering_fullsync_status},
            {NewReplConfig, ?REPL_CONFIG, object_filtering_repl_config},
            {NewRTConfig, ?RT_CONFIG, object_filtering_realtime_config},
            {NewFSConfig, ?FS_CONFIG, object_filtering_fullsync_config},
            {NewMergedRTConfig, ?MERGED_RT_CONFIG, object_filtering_merged_realtime_config},
            {NewMergedFSConfig, ?MERGED_FS_CONFIG, object_filtering_merged_fullsync_config}
        ],
    UpdateFun =
        fun(New, Old, Key) ->
            case New == Old of
                true -> ok;
                false -> application:set_env(riak_repl, Key, New)
            end
        end,
    [UpdateFun(A, B, C) || {A, B, C} <- List].

update_ring_configs() ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_configs/2,
        {?REPL_CONFIG, ?RT_CONFIG, ?FS_CONFIG, ?MERGED_RT_CONFIG, ?MERGED_FS_CONFIG}).

load_ring_configs_and_status() ->
    {{RT_Status, FS_Status}, {ReplConfig, RealtimeConfig, FullsyncConfig, MergedRTConfig, MergedFSConfig}} = riak_repl_ring:get_object_filtering_data(),
    ?RT_STATUS(RT_Status),
    ?FS_STATUS(FS_Status),
    ?REPL_CONFIG(ReplConfig),
    ?RT_CONFIG(RealtimeConfig),
    ?FS_CONFIG(FullsyncConfig),
    ?MERGED_RT_CONFIG(MergedRTConfig),
    ?MERGED_FS_CONFIG(MergedFSConfig).

object_filtering_disable(repl) ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {disabled, disabled}),
    application:set_env(riak_repl, object_filtering_realtime_status, disabled),
    application:set_env(riak_repl, object_filtering_fullsync_status, disabled),
    ok;
object_filtering_disable(realtime) ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {disabled, ?FS_STATUS}),
    application:set_env(riak_repl, object_filtering_realtime_status, disabled),
    ok;
object_filtering_disable(fullsync) ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {?RT_STATUS, disabled}),
    application:set_env(riak_repl, object_filtering_fullsync_status, disabled),
    ok;
object_filtering_disable(Mode) ->
    ?ERROR_UNKNOWN_REPL_MODE(object_filtering_disable, Mode).

object_filtering_enable(repl) ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {enabled, enabled}),
    application:set_env(riak_repl, object_filtering_realtime_status, enabled),
    application:set_env(riak_repl, object_filtering_fullsync_status, enabled),
    ok;
object_filtering_enable(realtime) ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {enabled, ?FS_STATUS}),
    application:set_env(riak_repl, object_filtering_realtime_status, enabled),
    ok;
object_filtering_enable(fullsync) ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {?RT_STATUS, enabled}),
    application:set_env(riak_repl, object_filtering_fullsync_status, enabled),
    ok;
object_filtering_enable(Mode) ->
    ?ERROR_UNKNOWN_REPL_MODE(object_filtering_enabled, Mode).

object_filtering_config_file({load, Mode}, Path) ->
    Modes = [repl, fullsync, realtime],
    case lists:member(Mode, Modes) of
        true -> object_filtering_config_file_helper({load, Mode}, Path);
        false -> ?ERROR_UNKNOWN_REPL_MODE(object_filtering_config_file, Mode)
    end;
object_filtering_config_file(Action, Path) ->
    object_filtering_config_file_helper(Action, Path).

object_filtering_config_file_helper(Action, Path) ->
    case file:consult(Path) of
        {ok, FilteringConfig} ->
            case check_filtering_rules(FilteringConfig) of
                ok ->
                    case Action of
                        check -> ok;
                        {load, ReplMode} ->
                            merge_and_load_configs(ReplMode, FilteringConfig)
                    end;
                Error2 ->
                    Error2
            end;
        Error1 ->
            Error1
    end.

merge_and_load_configs(repl, ReplConfig) ->
    %% Change Repl, MergedRT, and MergedFS
    MergedRT = merge_config(ReplConfig, ?RT_CONFIG),
    MergedFS = merge_config(ReplConfig, ?FS_CONFIG),
    ?REPL_CONFIG(ReplConfig),
    ?MERGED_RT_CONFIG(MergedRT),
    ?MERGED_FS_CONFIG(MergedFS),
    update_ring_configs(),
    ok;
merge_and_load_configs(realtime, RTConfig) ->
    %% Change RT, MergedRT
    MergedRT = merge_config(?REPL_CONFIG, RTConfig),
    ?RT_CONFIG(RTConfig),
    ?MERGED_RT_CONFIG(MergedRT),
    update_ring_configs(),
    ok;
merge_and_load_configs(fullsync, FSConfig) ->
    %% Change FS, MergedFS
    MergedFS = merge_config(?REPL_CONFIG, FSConfig),
    ?FS_CONFIG(FSConfig),
    ?MERGED_FS_CONFIG(MergedFS),
    update_ring_configs(),
    ok.

merge_config(Config1, Config2) ->
    merge_config_helper(Config1, Config2, []).
merge_config_helper([], [], MergedConfig) ->
    MergedConfig;
merge_config_helper([], Config2, MergedConfig) ->
    MergedConfig ++ Config2;
merge_config_helper(Config1, [], MergedConfig) ->
    MergedConfig ++ Config1;
merge_config_helper([ R = {RemoteName, {allow, AllowedRules1}, {block, BlockedRules1}} | Rest1 ], Config2, MergedConfigA) ->
    case lists:keytake(RemoteName, 1, Config2) of
        {value, {RemoteName, {allow, AllowedRules2}, {block, BlockedRules2}}, Rest2} ->
            MergedAllowedRules = merge_rules(AllowedRules1, AllowedRules2),
            MergedBlockedRules = merge_rules(BlockedRules1, BlockedRules2),
            MergedConfigB = MergedConfigA ++ [{RemoteName, {allow, MergedAllowedRules}, {block, MergedBlockedRules}}],
            merge_config_helper(Rest1, Rest2, MergedConfigB);
        false ->
            merge_config_helper(Rest1, Config2, MergedConfigA ++ [R])
    end.

merge_rules([], []) -> [];
merge_rules([?WILDCARD], _) -> [?WILDCARD];
merge_rules(_, [?WILDCARD]) -> [?WILDCARD];
merge_rules([], Config) -> Config;
merge_rules(Config, []) -> Config;
merge_rules(Config1, Config2) ->
    join_configs(Config1, Config2).

join_configs(Config1, []) ->
    Config1;
join_configs(Config1, [Elem|Rest]) ->
    case lists:member(Elem, Config1) of
        false -> join_configs(Config1++[Elem], Rest);
        true -> join_configs(Config1, Rest)
    end.


object_filtering_clear_config(Mode) ->
    object_filtering_clear_config_helper(Mode).

object_filtering_clear_config_helper(all) ->
    ?REPL_CONFIG([]),
    ?RT_CONFIG([]),
    ?FS_CONFIG([]),
    ?MERGED_RT_CONFIG([]),
    ?MERGED_FS_CONFIG([]),
    update_ring_configs(),
    ok;
object_filtering_clear_config_helper(repl) ->
    merge_and_load_configs(repl, []);
object_filtering_clear_config_helper(realtime) ->
    merge_and_load_configs(realtime, []);
object_filtering_clear_config_helper(fullsync) ->
    merge_and_load_configs(fullsync, []);
object_filtering_clear_config_helper(Mode) ->
    ?ERROR_UNKNOWN_REPL_MODE(object_filtering_clear_config, Mode).


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

check_rule_format(Rules = [{_RemoteName, {allow, _AllowedRules}, {block, _BlockedRules}} | _RestOfRemotes], NextCheck) ->
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

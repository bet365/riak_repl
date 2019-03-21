-module(riak_repl2_object_filter_console).
-behaviour(gen_server).
-include("riak_repl2_object_filter.hrl").

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

%% repl_console function calls
-export([
    status/0,
    status_all/0,
    get_config/1,
    get_config/2,
    set_clustername/1
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).



-define(SERVER, ?MODULE).
-define(CURRENT_VERSION, 1.0).
-record(state, {}).



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

% returns config to repl_console or errors out
get_config(Mode) ->
    ConvertedMode = list_to_atom(Mode),
    List = [fullsync, realtime, loaded_repl, loaded_realtime, loaded_fullsync],
    case lists:member(ConvertedMode, List) of
        true -> {config, riak_repl2_object_filter:get_config(ConvertedMode)};
        false -> return_error_unknown_repl_mode(get_config_external, Mode)
    end.
get_config(Mode, Remote) ->
    ConvertedMode = list_to_atom(Mode),
    List = [fullsync, realtime, loaded_repl, loaded_realtime, loaded_fullsync],
    case lists:member(ConvertedMode, List) of
        true -> {config, riak_repl2_object_filter:get_config(ConvertedMode, Remote)};
        false -> return_error_unknown_repl_mode(get_config_external, Mode)
    end.


status() ->
    {status_single_node,
        {node(),
            {
                riak_repl2_object_filter:get_version(),
                riak_repl2_object_filter:get_status(realtime),
                riak_repl2_object_filter:get_status(fullsync),
                erlang:phash2(riak_repl2_object_filter:get_config(realtime)),
                erlang:phash2(riak_repl2_object_filter:get_config(fullsync)),
                erlang:phash2(riak_repl2_object_filter:get_config(loaded_repl)),
                erlang:phash2(riak_repl2_object_filter:get_config(loaded_fullsync)),
                erlang:phash2(riak_repl2_object_filter:get_config(loaded_realtime))
            }
        }
    }.

status_all() ->
    {StatusAllNodes, _} = riak_core_util:rpc_every_member(riak_repl2_object_filter, status, [], ?TIMEOUT),
    Result = [R ||{status_single_node, R} <- StatusAllNodes],
    {status_all_nodes, lists:sort(Result)}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    load_ring_configs_and_status(),

    Version = riak_core_capability:get({riak_repl, ?VERSION}, 0),
    set_version(Version),

    ClusterName = riak_core_connection:symbolic_clustername(),
    set_clustername(ClusterName),

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
                       object_filtering_check_config_file(Path);
                   {load_config, Mode, Path} ->
                       object_filtering_load_config_file(list_to_atom(Mode), Path);
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
    Version = riak_core_capability:get({riak_repl, ?VERSION}, 0),
    case Version == ?CURRENT_VERSION of
        false ->
            erlang:send_after(5000, self(), poll_core_capability);
        true ->
            set_version(Version)
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%%%===================================================================
%%% Ring Update
%%%===================================================================

ring_update_update_configs({NewRTStatus, NewFSStatus}, {NewReplConfig, NewRTConfig, NewFSConfig, NewMergedRTConfig, NewMergedFSConfig}) ->
    List =
        [
            {NewRTStatus,       riak_repl2_object_filter:get_status(realtime),           ?RT_STATUS},
            {NewFSStatus,       riak_repl2_object_filter:get_status(fullsync),           ?FS_STATUS},
            {NewReplConfig,     riak_repl2_object_filter:get_config(loaded_repl),        ?REPL_CONFIG},
            {NewRTConfig,       riak_repl2_object_filter:get_config(loaded_realtime),    ?RT_CONFIG},
            {NewFSConfig,       riak_repl2_object_filter:get_config(loaded_fullsync),    ?FS_CONFIG},
            {NewMergedRTConfig, riak_repl2_object_filter:get_config(realtime),           ?MERGED_RT_CONFIG},
            {NewMergedFSConfig, riak_repl2_object_filter:get_config(fullsync),           ?MERGED_FS_CONFIG}
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
    ReplConfig = riak_repl2_object_filter:get_config(loaded_repl),
    RTConfig = riak_repl2_object_filter:get_config(loaded_realtime),
    FSConfig = riak_repl2_object_filter:get_config(loaded_fullsync),
    MergedRTConfig = riak_repl2_object_filter:get_config(realtime),
    MergedFSConfig = riak_repl2_object_filter:get_config(fullsync),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_configs/2,
        {ReplConfig, RTConfig, FSConfig, MergedRTConfig, MergedFSConfig}).

load_ring_configs_and_status() ->
    {{RT_Status, FS_Status}, {ReplConfig, RealtimeConfig, FullsyncConfig, MergedRTConfig, MergedFSConfig}} = riak_repl_ring:get_object_filtering_data(),
    set_status(realtime, RT_Status),
    set_status(fullsync, FS_Status),
    set_config(loaded_repl, ReplConfig),
    set_config(loaded_realtime, RealtimeConfig),
    set_config(loaded_fullsync, FullsyncConfig),
    set_config(realtime, MergedRTConfig),
    set_config(fullsync, MergedFSConfig).

%%%===================================================================
%%% Disable
%%%===================================================================
object_filtering_disable(repl) ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {disabled, disabled}),
    set_status(realtime, disabled),
    set_status(fullsync, disabled),
    ok;
object_filtering_disable(realtime) ->
    FSStatus = riak_repl2_object_filter:get_status(fullsync),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {disabled, FSStatus}),
    set_status(realtime, disabled),
    ok;
object_filtering_disable(fullsync) ->
    RTStatus = riak_repl2_object_filter:get_status(realtime),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {RTStatus, disabled}),
    set_status(fullsync, disabled),
    ok;
object_filtering_disable(Mode) ->
    return_error_unknown_repl_mode(object_filtering_disable, Mode).

%%%===================================================================
%%% Enable
%%%===================================================================
object_filtering_enable(repl) ->
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {enabled, enabled}),
    set_status(realtime, enabled),
    set_status(fullsync, enabled),
    ok;
object_filtering_enable(realtime) ->
    FSStatus = riak_repl2_object_filter:get_status(fullsync),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {enabled, FSStatus}),
    set_status(realtime, enabled),
    ok;
object_filtering_enable(fullsync) ->
    RTStatus = riak_repl2_object_filter:get_status(realtime),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_status/2, {RTStatus, enabled}),
    set_status(fullsync, enabled),
    ok;
object_filtering_enable(Mode) ->
    return_error_unknown_repl_mode(object_filtering_enabled, Mode).

%%%===================================================================
%%% Check Config
%%%===================================================================
object_filtering_check_config_file(Path) ->
    case file:consult(Path) of
        {ok, FilteringConfig} ->
            case check_filtering_rules(FilteringConfig) of
                ok ->
                    ok;
                Error2 ->
                    Error2
            end;
        Error1 ->
            Error1
    end.

%%%===================================================================
%%% Load Config
%%%===================================================================
object_filtering_load_config_file(Mode, Path) ->
    Modes = [repl, fullsync, realtime],
    case lists:member(Mode, Modes) of
        true ->
            object_filtering_load_config_file_helper(Mode, Path);
        false ->
            return_error_unknown_repl_mode(object_filtering_config_file, Mode)
    end.


object_filtering_load_config_file_helper(Mode, Path) ->
    case file:consult(Path) of
        {ok, FilteringConfig} ->
            case check_filtering_rules(FilteringConfig) of
                ok ->
                    merge_and_load_configs(Mode, FilteringConfig);
                Error2 ->
                    Error2
            end;
        Error1 ->
            Error1
    end.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

supported_match_types() ->
    V = riak_repl2_object_filter:get_version(),
    supported_match_types(V).
supported_match_types(1.0) ->
    [
        lnot,
        bucket,
        metadata,
        lastmod_age_greater_than,
        lastmod_age_less_than,
        lastmod_greater_than,
        lastmod_less_than
    ];
supported_match_types(_) ->
    [].
%% ====================================================================================================================
supported_match_value_formats(MatchType, MatchValue) ->
    V = app_helper:get_env(riak_repl, ?VERSION, 0),
    supported_match_value_formats(V, MatchType, MatchValue).
%% Typed bucket
supported_match_value_formats(1.0, bucket, {MatchValue1, MatchValue2}) ->
    is_binary(MatchValue1) and is_binary(MatchValue2);
%% Bucket
supported_match_value_formats(1.0, bucket, MatchValue) ->
    is_binary(MatchValue);
supported_match_value_formats(1.0, metadata, {_DictKey, _DictValue}) ->
    true;
supported_match_value_formats(1.0, metadata, {_DictKey}) ->
    true;
supported_match_value_formats(1.0, lastmod_age_greater_than, Age) ->
    is_integer(Age);
supported_match_value_formats(1.0, lastmod_age_less_than, Age) ->
    is_integer(Age);
supported_match_value_formats(1.0, lastmod_greater_than, TS) ->
    is_integer(TS);
supported_match_value_formats(1.0, lastmod_less_than, TS) ->
    is_integer(TS);
supported_match_value_formats(1.0, lnot, _) ->
    true;
supported_match_value_formats(0, _, _) ->
    false;
supported_match_value_formats(_, _, _) ->
    false.
%% ====================================================================================================================

check_filtering_rules([]) -> return_error_no_rules();
check_filtering_rules(FilteringRules) ->
    AllRemotes = lists:sort(lists:foldl(fun({RemoteName, _, _}, Acc) -> [RemoteName] ++ Acc end, [], FilteringRules)),
    NoDuplicateRemotes = lists:usort(AllRemotes),
    case AllRemotes == NoDuplicateRemotes of
        true ->
            check_filtering_rules_helper(FilteringRules, check_rule_format, ok);
        false ->
            return_error_duplicate_remote_entries()
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
    check_filtering_rules_helper(Rules, NextCheck, return_error_rule_format(Rule)).

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
            check_filtering_rules_helper(Rules, NextCheck, return_error_invalid_remote_name(RemoteName))
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
            return_error_invalid_rule(RemoteName, RuleType, Rule)
    end.

is_rule_supported(Rule) when (is_list(Rule) and (length(Rule) > 1)) -> is_multi_rule_supported(Rule);
is_rule_supported(Rule) -> is_single_rule_supported(Rule).


is_single_rule_supported({lnot, Rule}) ->
    is_rule_supported(Rule);
is_single_rule_supported({MatchType, MatchValue}) ->
    lists:member(MatchType, supported_match_types()) and supported_match_value_formats(MatchType, MatchValue);
is_single_rule_supported(_) -> false.

is_multi_rule_supported([]) -> true;
is_multi_rule_supported([Rule|Rest]) ->
    is_single_rule_supported(Rule) and is_multi_rule_supported(Rest).
%% ====================================================================================================================
merge_and_load_configs(repl, ReplConfig) ->
    %% Change Repl, MergedRT, and MergedFS
    RTConfig = riak_repl2_object_filter:get_config(loaded_realtime),
    FSConfig = riak_repl2_object_filter:get_config(loaded_fullsync),
    MergedRT = merge_config(ReplConfig, RTConfig),
    MergedFS = merge_config(ReplConfig, FSConfig),
    set_config(loaded_repl, ReplConfig),
    set_config(realtime, MergedRT),
    set_config(fullsync, MergedFS),
    update_ring_configs(),
    ok;
merge_and_load_configs(realtime, RTConfig) ->
    %% Change RT, MergedRT
    ReplConfig = riak_repl2_object_filter:get_config(loaded_repl),
    MergedRT = merge_config(ReplConfig, RTConfig),
    set_config(loaded_realtime, RTConfig),
    set_config(realtime, MergedRT),
    update_ring_configs(),
    ok;
merge_and_load_configs(fullsync, FSConfig) ->
    %% Change FS, MergedFS
    ReplConfig = riak_repl2_object_filter:get_config(loaded_repl),
    MergedFS = merge_config(ReplConfig, FSConfig),
    set_config(loaded_fullsync, FSConfig),
    set_config(fullsync, MergedFS),
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
%% ====================================================================================================================
object_filtering_clear_config(Mode) ->
    object_filtering_clear_config_helper(Mode).

object_filtering_clear_config_helper(all) ->
    set_config(loaded_repl, []),
    set_config(loaded_realtime, []),
    set_config(loaded_fullsync, []),
    set_config(realtime, []),
    set_config(fullsync, []),
    update_ring_configs(),
    ok;
object_filtering_clear_config_helper(repl) ->
    merge_and_load_configs(repl, []);
object_filtering_clear_config_helper(realtime) ->
    merge_and_load_configs(realtime, []);
object_filtering_clear_config_helper(fullsync) ->
    merge_and_load_configs(fullsync, []);
object_filtering_clear_config_helper(Mode) ->
    return_error_unknown_repl_mode(object_filtering_clear_config, Mode).
%% ====================================================================================================================
set_config(fullsync, Config) ->
    application:set_env(riak_repl, ?MERGED_FS_CONFIG, Config);
set_config(realtime, Config) ->
    application:set_env(riak_repl, ?MERGED_RT_CONFIG, Config);
set_config(loaded_repl, Config) ->
    application:set_env(riak_repl, ?REPL_CONFIG, Config);
set_config(loaded_realtime, Config) ->
    application:set_env(riak_repl, ?RT_CONFIG, Config);
set_config(loaded_fullsync, Config) ->
    application:set_env(riak_repl, ?FS_CONFIG, Config).
%% ====================================================================================================================
set_status(realtime, Status) ->
    application:set_env(riak_repl, ?RT_STATUS, Status);
set_status(fullsync, Status) ->
    application:set_env(riak_repl, ?FS_STATUS, Status).
%% ====================================================================================================================
set_version(Version) ->
    application:set_env(riak_repl, ?VERSION, Version).
%% ====================================================================================================================
set_clustername(Name) ->
    application:set_env(riak_repl, clustername, Name).
%% ====================================================================================================================
invalid_rule(RemoteName, allowed, Rule) -> {error, {invalid_rule_type_allowed, riak_repl2_object_filter:get_version(), RemoteName, Rule}};
invalid_rule(RemoteName, blocked, Rule) -> {error, {invalid_rule_type_blocked, riak_repl2_object_filter:get_version(), RemoteName, Rule}}.
%% ====================================================================================================================
return_error_no_rules() ->
    {error, {no_rules, riak_repl2_object_filter:get_version()}}.
return_error_rule_format(Rule) ->
    {error, {rule_format, riak_repl2_object_filter:get_version(), Rule}}.
return_error_duplicate_remote_entries() ->
    {error, {duplicate_remote_entries, riak_repl2_object_filter:get_version()}}.
return_error_invalid_remote_name(RemoteName) ->
    {error, {invalid_remote_name, riak_repl2_object_filter:get_version(), RemoteName}}.
return_error_invalid_rule(RemoteName, RuleType, Rule) ->
    invalid_rule(RemoteName, RuleType, Rule).
return_error_unknown_repl_mode(RelatedFun, Mode)->
    {error, unknown_repl_mode, RelatedFun, Mode}.
%% ====================================================================================================================
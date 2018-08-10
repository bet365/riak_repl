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
    get_versioned_config/1,
    get_versioned_config/2,
    create_config_for_remote_cluster/2,
    get_config/0,
    get_status/0,
    get_version/0,
    get_filter_rules/1,
    filter/2,
    filter/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(SUPPORTED_MATCH_TYPES(Version), supported_match_types(Version)).
-define(SUPPORTED_FILTER_TYPES(Version), supported_filter_types(Version)).
-define(STATUS, app_helper:get_env(riak_repl, object_filtering_status, disabled)).
-define(CONFIG, app_helper:get_env(riak_repl, object_filtering_config, [])).
-define(VERSION, app_helper:get_env(riak_repl, object_filtering_version, 0)).
-define(CLUSTERNAME, app_helper:get_env(riak_repl, clustername, "undefined")).
-define(CURRENT_VERSION, 1.0).

-define(DEFAULT_FILTERING_RULES, {{whitelist, []}, {blacklist, []}, {matched_rules, {0,0}}}).

-record(state, {}).

%%%===================================================================
%%% Macro Helper Functions
%%%===================================================================
%%supported_match_types(1.1) ->
%%    [bucket, metadata, key];
supported_match_types(1.0) ->
    [bucket, metadata];
supported_match_types(_) ->
    [].

%%supported_filter_types(1.1) ->
%%    supported_filter_types(1.0);
supported_filter_types(1.0) ->
    [blacklist, whitelist];
supported_filter_types(_) ->
    [].

%%%===================================================================
%%% API (Function Callbacks)
%%%===================================================================
get_config() ->
    get_versioned_config(?VERSION).
get_status()->
    ?STATUS.
get_version() ->
    ?VERSION.
get_versioned_config(Version) ->
    get_versioned_config(?CONFIG, Version).
get_versioned_config(Config, Version) ->
    case Version >= ?VERSION of
        true ->
            Config;
        false ->
            downgrade_config(Config, Version)
    end.

create_config_for_remote_cluster(ClusterName, AgreedVersion) ->
    Config = get_versioned_config(AgreedVersion),
    invert_config(ClusterName, Config).

%% returns {{whitelist, Whitelist}, {blacklist, Blacklist}, {matched_rules, {X, Y}}}
%% whitelist and blacklist are remote names!
get_filter_rules(Object) ->
    case ?STATUS of
        enabled ->
            Bucket = riak_object:bucket(Object),
            Metadatas = riak_object:get_metadatas(Object),
            filter_helper(?VERSION, ?CONFIG, {Bucket, Metadatas}, ?DEFAULT_FILTERING_RULES);
        disabled ->
            ?DEFAULT_FILTERING_RULES
    end.

% Returns true or false to say if we need to filter based on an object and remote name
filter({fullsync, disabled, _Version, _Config, _RemoteName}, _Object) ->
    false;
filter({fullsync, enabled, 0, _Config, _RemoteName}, _Object) ->
    false;
filter({fullsync, enabled, Version, Config, RemoteName}, Object) ->
    Bucket = riak_object:bucket(Object),
    Metadatas = riak_object:get_metadatas(Object),
    FilteredRemotes = filter_helper(Version, Config, {Bucket, Metadatas}, ?DEFAULT_FILTERING_RULES),
    AllowedRemotes = allowed_remotes([RemoteName], FilteredRemotes),
    not lists:member(RemoteName, AllowedRemotes).

%% reutrns true or false to say if you can send an object to a remote name
filter(realtime, RemoteName, Meta) ->
    realtime_filter(?STATUS, RemoteName, Meta).

%%%===================================================================
%%% API (Function Callbacks) Helper Functions
%%%===================================================================
downgrade_config(Config, Version) ->
    downgrade_config_helper(Config, Version, []).
downgrade_config_helper([], _, NewConfig) ->
    NewConfig;
%% TODO change format of config here
downgrade_config_helper([ Rule = {{MatchType, _MatchValue}, {FilterType, _RemoteNodes}} | Rest], Version, NewConfig) ->
    case {lists:member(MatchType, ?SUPPORTED_MATCH_TYPES(Version)), lists:member(FilterType, ?SUPPORTED_FILTER_TYPES(Version))} of
        {true, true} ->
            downgrade_config_helper(Rest, Version, NewConfig++[Rule]);
        _ ->
            downgrade_config_helper(Rest, Version, NewConfig)
    end.

invert_config(ClusterName, Config) ->
    invert_config_helper(ClusterName, Config, []).
invert_config_helper(_, [], NewConfig) ->
    NewConfig;
invert_config_helper(ClusterName, [{{MatchType, MatchValue}, {FilterType, RemoteNodes}} | Rest], NewConfig) ->
    case FilterType of
        whitelist ->
            case lists:member(ClusterName, RemoteNodes) of
                true ->
                    invert_config_helper(ClusterName, Rest, NewConfig);
                false ->
                    Rule = [{{MatchType, MatchValue}, {blacklist, [?CLUSTERNAME]}}],
                    invert_config_helper(ClusterName, Rest, NewConfig++Rule)
            end;
        blacklist ->
            case lists:member(ClusterName, RemoteNodes) of
                true ->
                    Rule = [{{MatchType, MatchValue}, {blacklist, [?CLUSTERNAME]}}],
                    invert_config_helper(ClusterName, Rest, NewConfig++Rule);
                false ->
                    invert_config_helper(ClusterName, Rest, NewConfig)
            end
    end.

filter_helper(0, _Config, _ObjectData, FilteredRemotes) ->
    FilteredRemotes;
filter_helper(_V, [], _ObjectData, FilteredRemotes) ->
    FilteredRemotes;
%% TODO change format of config here
filter_helper(Version, [{{MatchType, MatchValue}, {FilterType, RemoteNodes}} | RestOfRules], ObjectData, FilteredRemotes) ->
    case does_data_match_rule(MatchType, MatchValue, ObjectData) of
        true ->
            NewFilteredRemotes = add_filtered_remote(FilterType, RemoteNodes, FilteredRemotes),
            filter_helper(Version, RestOfRules, ObjectData, NewFilteredRemotes);
        false ->
            filter_helper(Version, RestOfRules, ObjectData, FilteredRemotes)
    end.



does_data_match_rule(bucket, MatchBucket, {Bucket, _Metadatas}) ->
    MatchBucket == Bucket;
does_data_match_rule(metadata, {MatchKey, MatchValue}, {_Bucket, Metadatas}) ->
    match_metadata(MatchKey, MatchValue, Metadatas).

match_metadata(_, _, []) ->
    false;
match_metadata(MatchKey, MatchValue, [Dict | Rest]) ->
    case dict:find(MatchKey, Dict) of
        {ok, MatchValue} ->
            true;
        _ ->
            match_metadata(MatchKey, MatchValue, Rest)
    end.


add_filtered_remote(whitelist, RemoteNodes, {{whitelist, _W}, {blacklist, B}, {matched_rules, {0, BN}}}) ->
    {{whitelist, RemoteNodes}, {blacklist, B}, {matched_rules, {1, BN}}};
add_filtered_remote(whitelist, RemoteNodes, {{whitelist, W}, {blacklist, B}, {matched_rules, {WN, BN}}}) ->
    NewW = sets:to_list(sets:intersection(sets:from_list(RemoteNodes), sets:from_list(W))),
    {{whitelist, NewW}, {blacklist, B}, {matched_rules, {WN+1, BN}}};
add_filtered_remote(blacklist, RemoteNodes, {{whitelist, W}, {blacklist, B}, {matched_rules, {WN, BN}}}) ->
    {{whitelist, W}, {blacklist, B++RemoteNodes}, {matched_rules, {WN, BN+1}}}.

allowed_remotes(Remotes, ?DEFAULT_FILTERING_RULES) ->
    Remotes;
allowed_remotes(Remotes, {{whitelist, []}, {blacklist, Blacklist}, {matched_rules, {0,_}}}) ->
    Remotes -- Blacklist;
allowed_remotes(Remotes, {{whitelist, Whitelist}, {blacklist, []}, {matched_rules, {_, 0}}}) ->
    sets:to_list(sets:intersection(sets:from_list(Remotes), sets:from_list(Whitelist)));
allowed_remotes(Remotes, {{whitelist, Whitelist}, {blacklist, Blacklist}, {matched_rules, {_WN, _BN}}}) ->
    WhitelistedRemotes = sets:to_list(sets:intersection(sets:from_list(Remotes), sets:from_list(Whitelist))),
    WhitelistedRemotes -- Blacklist.

realtime_filter(enabled, RemoteName, Meta) ->
    case orddict:find(?BT_OBJECT_FILTERING_RULES, Meta) of
        {ok, ObjectFilteringRules} ->
            AllowedRemotes = allowed_remotes([RemoteName], ObjectFilteringRules),
            not lists:member(RemoteName, AllowedRemotes);
        _ ->
            false
    end;
realtime_filter(disabled, _, _) ->
    false.

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
        {ok, FilteringRules} ->
            case check_filtering_rules(FilteringRules) of
                ok ->
                    case Action of
                        check -> ok;
                        load ->
                            SortedConfig = sort_config(FilteringRules),
                            riak_core_ring_manager:ring_trans(fun riak_repl_ring:overwrite_object_filtering_config/2, SortedConfig),
                            application:set_env(riak_repl, object_filtering_config, SortedConfig),
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


check_filtering_rules([]) -> {error, {no_rules, ?VERSION}};
check_filtering_rules(FilteringRules) -> check_filtering_rules_helper(FilteringRules, 1).
check_filtering_rules_helper([], _) -> ok;
check_filtering_rules_helper([{{MatchType, MatchValue}, {FilterType, RemoteNodes}} | RestOfRules], N) ->
    case duplicate_rule({MatchType, MatchValue}, RestOfRules) of
        false ->
            case lists:member(MatchType, ?SUPPORTED_MATCH_TYPES(?VERSION)) of
                true ->
                    case lists:member(FilterType, ?SUPPORTED_FILTER_TYPES(?VERSION)) of
                        true ->
                            case is_list_of_names(RemoteNodes) of
                                true ->
                                    check_filtering_rules_helper(RestOfRules, N+1);
                                false ->
                                    {error, {filter_value, ?VERSION, N, RemoteNodes, lists}}
                            end;
                        false ->
                            {error, {filter_type, ?VERSION, N, FilterType, ?SUPPORTED_FILTER_TYPES(?VERSION)}}
                    end;
                false ->
                    {error, {match_type, ?VERSION, N, MatchType, ?SUPPORTED_MATCH_TYPES(?VERSION)}}
            end;
        true ->
            {error, {duplicate_rule, ?VERSION, N, MatchType, MatchValue}}
    end.


duplicate_rule(Rule, RestOfRules) ->
    case lists:keyfind(Rule, 1, RestOfRules) of
        false ->
            false;
        _ ->
            true
    end.

sort_config(Config) ->
    sort_config_helper(Config, []).
sort_config_helper([], Sorted) ->
    Sorted;
sort_config_helper([Rule = {_, {blacklist, _}} | Rest], Sorted) ->
    sort_config_helper(Rest, Sorted++[Rule]);
sort_config_helper([Rule | Rest], Sorted) ->
    sort_config_helper(Rest, [Rule]++Sorted).

is_list_of_names([]) -> false;
is_list_of_names(Names) -> is_list_of_names_helper(Names).
is_list_of_names_helper([]) -> true;
is_list_of_names_helper([Name|Rest]) ->
    case is_list(Name) of
        true ->
            is_list_of_names_helper(Rest);
        false ->
            false
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

object_filter_test() ->
    {spawn,
        [setup,
            fun setup/0,
            [
                fun fullsync_filter_test/0,
                fun fullsync_config_for_remote_cluster_test/0
            ]
        ]
    }.

setup() ->
    riak_repl_test_util:start_test_ring(),
    riak_repl_test_util:start_lager().

fullsync_filter_test() ->
    [fullsync_filter_test(N) || N <- lists:seq(1,3)].

%% Testing Version 0 (for rolling upgrade, no filtering should start)
fullsync_filter_test(1) ->
    RObj = riak_object:new(<<"bucket">>, <<"key">>, <<"value">>),
    Config = [{{bucket, <<"bucket">>}, {whitelist, [remote_name]}}],
    ?assertEqual(false, filter({fullsync, disabled, 0, [], remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, disabled, 0, Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, enabled, 0, [], remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, enabled, 0, Config, remote_name}, RObj));

%% Testing 1.0 disabled
fullsync_filter_test(2) ->
    MetaData = dict:from_list([{filter, test}]),
    RObj = riak_object:new(<<"bucket">>, <<"key">>, <<"value">>, MetaData),
    BucketConfig = [{{bucket, <<"bucket">>}, {whitelist, [remote_name]}}],
    MetaDataConfig = [{{metadata, {filter, test}}, {whitelist, [remote_name]}}],
    BucketAndMetaDataConfig = BucketConfig ++ MetaDataConfig,

    ?assertEqual(false, filter({fullsync, disabled, 1.0, [], remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, disabled, 1.0, BucketConfig, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, disabled, 1.0, MetaDataConfig, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, disabled, 1.0, BucketAndMetaDataConfig, remote_name}, RObj));

%% Testing 1.0 blacklisting and whitelisting
fullsync_filter_test(3) ->
    MetaData = dict:from_list([{filter, 1}]),
    RObj = riak_object:new(<<"bucket-1">>, <<"key">>, <<"value">>, MetaData),
    AllowBucket1Config = [{{bucket, <<"bucket-1">>}, {whitelist, [remote_name]}}],
    AllowBucket2Config = [{{bucket, <<"bucket-2">>}, {whitelist, [remote_name]}}],
    AllowMetaData1Config = [{{metadata, {filter, 1}}, {whitelist, [remote_name]}}],
    AllowMetaData2Config = [{{metadata, {filter, 2}}, {whitelist, [remote_name]}}],
    WhitelistBlockBucket1Config = [{{bucket, <<"bucket-1">>}, {whitelist, [anything_other_remote]}}],
    WhitelistBlockMetaData1Config = [{{metadata, {filter, 1}}, {whitelist, [anything_other_remote]}}],
    BlacklistAllowBucket1Config = [{{bucket, <<"bucket-1">>}, {blacklist, [anything_other_remote]}}],
    BlacklistAllowMetaData1Config = [{{metadata, {filter, 1}}, {blacklist, [anything_other_remote]}}],
    BlacklistBlockBucket1Config = [{{bucket, <<"bucket-1">>}, {blacklist, [remote_name]}}],
    BlacklistBlockMetaData1Config = [{{metadata, {filter, 1}}, {blacklist, [remote_name]}}],
    ?assertEqual(false, filter({fullsync, disabled, 1.0, [], remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, disabled, 1.0, AllowBucket1Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, disabled, 1.0, AllowBucket2Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, disabled, 1.0, AllowMetaData1Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, disabled, 1.0, AllowMetaData2Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, enabled, 1.0, [], remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, enabled, 1.0, AllowBucket1Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, enabled, 1.0, AllowBucket2Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, enabled, 1.0, AllowMetaData1Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, enabled, 1.0, AllowMetaData2Config, remote_name}, RObj)),
    ?assertEqual(true, filter({fullsync, enabled, 1.0, WhitelistBlockBucket1Config, remote_name}, RObj)),
    ?assertEqual(true, filter({fullsync, enabled, 1.0, WhitelistBlockMetaData1Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, enabled, 1.0, BlacklistAllowBucket1Config, remote_name}, RObj)),
    ?assertEqual(false, filter({fullsync, enabled, 1.0, BlacklistAllowMetaData1Config, remote_name}, RObj)),
    ?assertEqual(true, filter({fullsync, enabled, 1.0, BlacklistBlockBucket1Config, remote_name}, RObj)),
    ?assertEqual(true, filter({fullsync, enabled, 1.0, BlacklistBlockMetaData1Config, remote_name}, RObj)).

fullsync_config_for_remote_cluster_test() ->
    [fullsync_config_for_remote_cluster_test(N) || N <- lists:seq(1,14)].

fullsync_config_for_remote_cluster_test(1) ->
    RemoteName = test_cluster,
    SourceConfig = [],
    ExpectedConfig = [],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));
fullsync_config_for_remote_cluster_test(2) ->
    RemoteName = test_cluster,
    SourceConfig = [{{bucket, <<"bucket">>}, {whitelist, [RemoteName]}}],
    ExpectedConfig = [],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));
fullsync_config_for_remote_cluster_test(3) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig = [{{bucket, <<"bucket">>}, {whitelist, [any_other_cluster]}}],
    ExpectedConfig = [{{bucket, <<"bucket">>}, {blacklist, [SourceName]}}],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));
fullsync_config_for_remote_cluster_test(4) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig = [{{bucket, <<"bucket">>}, {blacklist, [RemoteName]}}],
    ExpectedConfig = [{{bucket, <<"bucket">>}, {blacklist, [SourceName]}}],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));
fullsync_config_for_remote_cluster_test(5) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig = [{{bucket, <<"bucket">>}, {blacklist, [any_other_cluster]}}],
    ExpectedConfig = [],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));
fullsync_config_for_remote_cluster_test(6) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig =
        [
            {{bucket, <<"bucket-1">>}, {blacklist, [RemoteName]}},
            {{bucket, <<"bucket-2">>}, {whitelist, [any_other_cluster]}}

        ],
    ExpectedConfig = [],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, object_filtering_version, 1.0),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 0));
fullsync_config_for_remote_cluster_test(7) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig =
        [
            {{bucket, <<"bucket-1">>}, {blacklist, [RemoteName]}},
            {{bucket, <<"bucket-2">>}, {whitelist, [any_other_cluster]}}

        ],
    ExpectedConfig =
        [
            {{bucket, <<"bucket-1">>}, {blacklist, [SourceName]}},
            {{bucket, <<"bucket-2">>}, {blacklist, [SourceName]}}

        ],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, object_filtering_version, 1.0),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));

%% ================================================================================================== %%

fullsync_config_for_remote_cluster_test(8) ->
    RemoteName = test_cluster,
    SourceConfig = [{{metadata, {filter, test}}, {whitelist, [RemoteName]}}],
    ExpectedConfig = [],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));
fullsync_config_for_remote_cluster_test(9) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig = [{{metadata, {filter, test}}, {whitelist, [any_other_cluster]}}],
    ExpectedConfig = [{{metadata, {filter, test}}, {blacklist, [SourceName]}}],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));
fullsync_config_for_remote_cluster_test(10) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig = [{{metadata, {filter, test}}, {blacklist, [RemoteName]}}],
    ExpectedConfig = [{{metadata, {filter, test}}, {blacklist, [SourceName]}}],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));
fullsync_config_for_remote_cluster_test(11) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig = [{{metadata, {filter, test}}, {blacklist, [any_other_cluster]}}],
    ExpectedConfig = [],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));
fullsync_config_for_remote_cluster_test(12) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig =
        [
            {{metadata, {filter, test_1}}, {blacklist, [RemoteName]}},
            {{metadata, {filter, test_2}}, {whitelist, [any_other_cluster]}}

        ],
    ExpectedConfig = [],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, object_filtering_version, 1.0),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 0));
fullsync_config_for_remote_cluster_test(13) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig =
        [
            {{metadata, {filter, test_1}}, {blacklist, [RemoteName]}},
            {{metadata, {filter, test_2}}, {whitelist, [any_other_cluster]}}

        ],
    ExpectedConfig =
        [
            {{metadata, {filter, test_1}}, {blacklist, [SourceName]}},
            {{metadata, {filter, test_2}}, {blacklist, [SourceName]}}

        ],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, object_filtering_version, 1.0),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0));

%% ================================================================================================== %%

fullsync_config_for_remote_cluster_test(14) ->
    RemoteName = test_cluster,
    SourceName = source_cluster,
    SourceConfig =
        [
            {{bucket, <<"bucket-1">>}, {blacklist, [any_other_cluster, RemoteName, another_cluster]}},
            {{bucket, <<"bucket-2">>}, {blacklist, [any_other_cluster, another_cluster]}},
            {{bucket, <<"bucket-3">>}, {whitelist, [any_other_cluster, RemoteName, another_cluster]}},
            {{bucket, <<"bucket-4">>}, {whitelist, [any_other_cluster, another_cluster]}},
            {{metadata, {filter, test_1}}, {blacklist, [any_other_cluster, RemoteName, another_cluster]}},
            {{metadata, {filter, test_2}}, {blacklist, [any_other_cluster, another_cluster]}},
            {{metadata, {filter, test_3}}, {whitelist, [any_other_cluster, RemoteName, another_cluster]}},
            {{metadata, {filter, test_4}}, {whitelist, [any_other_cluster, another_cluster]}}

        ],
    ExpectedConfig =
        [
            {{bucket, <<"bucket-1">>}, {blacklist, [SourceName]}},
            {{bucket, <<"bucket-4">>}, {blacklist, [SourceName]}},
            {{metadata, {filter, test_1}}, {blacklist, [SourceName]}},
            {{metadata, {filter, test_4}}, {blacklist, [SourceName]}}


        ],
    application:set_env(riak_repl, object_filtering_config, SourceConfig),
    application:set_env(riak_repl, object_filtering_version, 1.0),
    application:set_env(riak_repl, clustername, SourceName),
    ?assertEqual(ExpectedConfig, create_config_for_remote_cluster(RemoteName, 1.0)).



-endif.
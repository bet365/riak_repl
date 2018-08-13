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
    convert_fullsync_config/2,
    get_realtime_config/1,
    get_versioned_config/1,
    get_versioned_config/2,
    get_config/0,
    get_config/1,
    get_config/2,
    get_status/0,
    get_version/0,
    filter/2,
    filter/3
]).

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
%% returns the entire config for all clusters
get_config() ->
    get_versioned_config(?VERSION).
%% returns config only for the remote that is named in the argument
get_config(RemoteName) ->
    get_versioned_config(RemoteName, ?VERSION).
%% returns config only for the remote named in the argument, at a particular version
get_config(RemoteName, Version) ->
    get_versioned_config(RemoteName, Version).
%% returns the status of our local cluster for object filtering
get_status()->
    ?STATUS.
%% returns the version of our local cluster for object filtering
get_version() ->
    ?VERSION.


%% converts a remote clusters config to be a config that is used for our local cluster
convert_fullsync_config(_RemoteConfig, _AgreedVersion) ->
    ok.
% Returns true or false to say if we need to filter based on an object and remote name
filter({fullsync, disabled, _Version, _Config, _RemoteName}, _Object) ->
    false;
filter({fullsync, enabled, 0, _Config, _RemoteName}, _Object) ->
    false;
filter({fullsync, enabled, _Version, _Config, _RemoteName}, Object) ->
    _Bucket = riak_object:bucket(Object),
    _Metadatas = riak_object:get_metadatas(Object),
    false.


%% returns a list of allowd and blocked remotes
get_realtime_config(_Object) ->
    ok.
%% reutrns true or false to say if you can send an object to a remote name
filter(realtime, _RemoteName, _Meta) ->
    false.

%%%===================================================================
%%% API (Function Callbacks) Helper Functions
%%%===================================================================
get_versioned_config(Version) ->
    get_versioned_config(all, Version).
get_versioned_config(all, _Version) ->
    ?CONFIG;
get_versioned_config(_RemoteCluster, Version) ->
    case Version >= ?VERSION of
        true ->
            ?CONFIG;
        false ->
            ?CONFIG
%%            downgrade_config(Config, Version)
    end.

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
    pass.

fullsync_config_for_remote_cluster_test() ->
    pass.



-endif.
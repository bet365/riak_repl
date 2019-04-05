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
    force_update/0,
    update/2

]).

%% repl_console function calls
-export([
    status/0,
    get_status/0,
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

-record(state, {
    config = [],
    update_list = [],
    update_ref
}).

-define(CURRENT_VERSION, 1.0).
-define(RETRY_UPDATE, 6).


%%%===================================================================
%%% gen_server API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

check_config(ConfigFilePath) ->
    safe_call(fun() -> gen_server:call(?SERVER, {check_config, ConfigFilePath}, ?TIMEOUT) end).
load_config(ReplMode, ConfigFilePath) ->
    safe_call(fun() -> gen_server:call(?SERVER, {load_config, ReplMode, ConfigFilePath}, ?TIMEOUT) end).
clear_config(Mode) ->
    safe_call(fun() -> gen_server:call(?SERVER, {clear_config, Mode}, ?TIMEOUT) end).
enable()->
    safe_call(fun() -> gen_server:call(?SERVER, enable, ?TIMEOUT) end).
enable(Mode)->
    safe_call(fun() -> gen_server:call(?SERVER, {enable, Mode}, ?TIMEOUT) end).
disable()->
    safe_call(fun() -> gen_server:call(?SERVER, disable, ?TIMEOUT) end).
disable(Mode)->
    safe_call(fun() -> gen_server:call(?SERVER, {disable, Mode}, ?TIMEOUT) end).

safe_call(Fun) ->
    R = try Fun()
        catch Type:Error ->
            {error, {Type, Error}}
        end,
    print_response(R).





update(Node, List) when Node =:= node() ->
    gen_server:cast(?SERVER, {update, List});
update(Node, List) ->
    gen_server:cast({?SERVER, Node}, {update, List}).

force_update() ->
    gen_server:cast(?SERVER, force_update).





% returns config to repl_console or errors out
get_config("all") ->
    R = {all_config, riak_repl2_object_filter:get_config(all)},
    print_response(R);
get_config(Mode) ->
    ConvertedMode = list_to_atom(Mode),
    List = [fullsync, realtime, loaded_repl, loaded_realtime, loaded_fullsync],
    R =
        case lists:member(ConvertedMode, List) of
            true -> {config, riak_repl2_object_filter:get_config(ConvertedMode)};
            false -> return_error_unknown_repl_mode(get_config_external, Mode)
        end,
    print_response(R).
get_config(Mode, Remote) ->
    ConvertedMode = list_to_atom(Mode),
    List = [fullsync, realtime, loaded_repl, loaded_realtime, loaded_fullsync],
    R =
        case lists:member(ConvertedMode, List) of
            true -> {config, riak_repl2_object_filter:get_config(ConvertedMode, Remote)};
            false -> return_error_unknown_repl_mode(get_config_external, Mode)
        end,
    print_response(R).


get_status() ->
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
status() ->
    S = get_status(),
    print_response(S).

status_all() ->
    {StatusAllNodes, _} = riak_core_util:rpc_every_member(riak_repl2_object_filter_console, get_status, [], ?TIMEOUT),
    Result = [R ||{status_single_node, R} <- StatusAllNodes],
    print_response({status_all_nodes, lists:sort(Result)}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    load_config(),

    Version = riak_core_capability:get(?OBF_VERSION_KEY, 0),
    set_version(Version),

    ClusterName = riak_core_connection:symbolic_clustername(),
    set_clustername(ClusterName),

    case Version == ?CURRENT_VERSION of
        false ->
            erlang:send_after(5000, self(), poll_core_capability);
        true ->
            ok
    end,
    {ok, #state{config = active_config()}}.

handle_call(Request, _From, State) ->
    {Response, NewState} =
        case Request of
            enable ->
                object_filtering_enable(repl, State);
            {enable, Mode} ->
                object_filtering_enable(list_to_atom(Mode), State);
            disable ->
                object_filtering_disable(repl, State);
            {disable, Mode} ->
                object_filtering_disable(list_to_atom(Mode), State);
            {check_config, Path} ->
                object_filtering_check_config_file(Path, State);
            {load_config, Mode, Path} ->
                object_filtering_load_config_file(list_to_atom(Mode), Path, State);
            {clear_config, Mode} ->
                object_filtering_clear_config(list_to_atom(Mode), State);
            R ->
                {{error, no_request, R}, State}
        end,
    {reply, Response, NewState}.

handle_cast({update, List}, State = #state{update_ref = undefined, update_list = L}) ->
    Time = app_helper:get_env(riak_repl, object_filter_update_config_time, 500),
    Ref = erlang:send_after(Time, self(), update_config),
    {noreply, State#state{update_list = L ++ List, update_ref = Ref}};

handle_cast({update, List}, State = #state{update_list = U}) ->
    {noreply, State#state{update_list = U ++ List}};

handle_cast(force_update, _State) ->
    load_config(),
    {noreply, #state{config = active_config()}}.

handle_info(poll_core_capability, State) ->
    Version = riak_core_capability:get(?OBF_VERSION_KEY, 0),
    case Version == ?CURRENT_VERSION of
        false ->
            erlang:send_after(5000, self(), poll_core_capability);
        true ->
            set_version(Version)
    end,
    {noreply, State};

handle_info(update_config, State = #state{update_list = L1}) ->
    RetryList = preform_updates(L1),
    case RetryList of
        [] ->
            {noreply, State#state{config = active_config(), update_ref = undefined}};
        L2 ->
            Time = app_helper:get_env(riak_repl, object_filter_update_config_retry_time, 500),
            Ref = erlang:send_after(Time, self(), update_config),
            {noreply, State#state{config = active_config(), update_ref = Ref, update_list = L2}}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%%%===================================================================
%%% Update
%%%===================================================================

preform_updates(L) ->
    Updated = [update_config(Update) || Update <- L],
    lists:foldl(fun
                    (ok, Acc) -> Acc;
                    ({Key, 0}, Acc) -> lager:warning("failed to update key: ~p", [Key]), Acc;
                    ({Key, Counter}, Acc) -> [{Key, Counter}] ++ Acc
                end,
        [], Updated).

update_config({{Key, config}, N}) ->
    New = riak_core_metadata:get(?OBF_CONFIG_KEY, Key),
    Old = riak_repl2_object_filter:get_config(Key),
    case New == Old of
        true ->
            {{Key, config}, N-1};
        false ->
            set_config(Key, New),
            ok
    end;
update_config({{Key, status}, N}) ->
    New = riak_core_metadata:get(?OBF_STATUS_KEY, Key),
    Old = riak_repl2_object_filter:get_status(Key),
    case New == Old of
        true ->
            {{Key, status}, N-1};
        false ->
            set_status(Key, New),
            ok
    end.



update_all_nodes(State = #state{config = Config}) ->
    NewConfig = active_config(),
    case orddict:fetch_keys(NewConfig -- Config) of
        [] ->
            State;
        Changes ->
            List = [{Key, ?RETRY_UPDATE} || Key <- Changes],
            [?MODULE:update(Node, List) || Node <- nodes()],
            State#state{config = NewConfig}
    end.

active_config() ->
    orddict:from_list(
        [
            {{loaded_repl, config}, riak_repl2_object_filter:get_config(loaded_repl)},
            {{loaded_realtime, config} ,riak_repl2_object_filter:get_config(loaded_realtime)},
            {{loaded_fullsync, config}, riak_repl2_object_filter:get_config(loaded_fullsync)},
            {{realtime, config}, riak_repl2_object_filter:get_config(realtime)},
            {{fullsync, config}, riak_repl2_object_filter:get_config(fullsync)},
            {{realtime, status}, riak_repl2_object_filter:get_status(realtime)},
            {{fullsync, status}, riak_repl2_object_filter:get_status(fullsync)}
    ]).

saved_config() ->
    orddict:from_list(
        [
            {{loaded_repl, config}, safe_core_metadata_get(?OBF_CONFIG_KEY, loaded_repl, [])},
            {{loaded_realtime, config}, safe_core_metadata_get(?OBF_CONFIG_KEY, loaded_realtime, [])},
            {{loaded_fullsync, config}, safe_core_metadata_get(?OBF_CONFIG_KEY, loaded_fullsync, [])},
            {{realtime, config}, safe_core_metadata_get(?OBF_CONFIG_KEY, realtime, [])},
            {{fullsync, config},  safe_core_metadata_get(?OBF_CONFIG_KEY, fullsync, [])},
            {{realtime, status},  safe_core_metadata_get(?OBF_STATUS_KEY, realtime, disabled)},
            {{fullsync, status}, safe_core_metadata_get(?OBF_STATUS_KEY, fullsync, disabled)}
    ]).

load_config() ->
    Config = saved_config(),
    set_status(realtime, safe_orddict_find({realtime, status}, Config, disabled)),
    set_status(fullsync, safe_orddict_find({fullsync, status}, Config, disabled)),
    set_config(loaded_repl, safe_orddict_find({loaded_repl, config}, Config, [])),
    set_config(loaded_realtime, safe_orddict_find({loaded_realtime, config}, Config, [])),
    set_config(loaded_fullsync, safe_orddict_find({loaded_fullsync, config}, Config, [])),
    set_config(realtime, safe_orddict_find({realtime, config}, Config, [])),
    set_config(fullsync, safe_orddict_find({fullsync, config}, Config, [])).

safe_orddict_find(Key, Orddict, Default) ->
    case orddict:find(Key, Orddict) of
        {ok, V} ->
            V;
        error ->
            Default
    end.

safe_core_metadata_get(B, K, Default) ->
    case riak_core_metadata:get(B, K) of
        undefined ->
            Default;
        Value ->
            Value
    end.

%%%===================================================================
%%% Disable
%%%===================================================================
object_filtering_disable(repl, State) ->
    save_status(realtime, disabled),
    save_status(fullsync, disabled),
    NewState = update_all_nodes(State),
    {ok, NewState};
object_filtering_disable(realtime, State) ->
    save_status(realtime, disabled),
    NewState = update_all_nodes(State),
    {ok, NewState};
object_filtering_disable(fullsync, State) ->
    save_status(fullsync, disabled),
    NewState = update_all_nodes(State),
    {ok, NewState};
object_filtering_disable(Mode, State) ->
    {return_error_unknown_repl_mode(object_filtering_disable, Mode), State}.

%%%===================================================================
%%% Enable
%%%===================================================================
object_filtering_enable(repl, State) ->
    save_status(realtime, enabled),
    save_status(fullsync, enabled),
    NewState = update_all_nodes(State),
    {ok, NewState};
object_filtering_enable(realtime, State) ->
    save_status(realtime, enabled),
    NewState = update_all_nodes(State),
    {ok, NewState};
object_filtering_enable(fullsync, State) ->
    save_status(fullsync, enabled),
    NewState = update_all_nodes(State),
    {ok, NewState};
object_filtering_enable(Mode, State) ->
    {return_error_unknown_repl_mode(object_filtering_enabled, Mode), State}.

%%%===================================================================
%%% Check Config
%%%===================================================================
object_filtering_check_config_file(Path, State) ->
    Response = case file:consult(Path) of
                   {ok, FilteringConfig} ->
                        case check_filtering_rules(FilteringConfig) of
                            ok ->
                                ok;
                            Error2 ->
                                Error2
                        end;
                    Error1 ->
                        Error1
                end,
    {Response, State}.

%%%===================================================================
%%% Load Config
%%%===================================================================
object_filtering_load_config_file(Mode, Path, State) ->
    Modes = [repl, fullsync, realtime],
    case lists:member(Mode, Modes) of
        true ->
            object_filtering_load_config_file_helper(Mode, Path, State);
        false ->
            {return_error_unknown_repl_mode(object_filtering_config_file, Mode), State}
    end.


object_filtering_load_config_file_helper(Mode, Path, State) ->
    case file:consult(Path) of
        {ok, FilteringConfig} ->
            case check_filtering_rules(FilteringConfig) of
                ok ->
                    NewState = merge_and_load_configs(Mode, FilteringConfig, State),
                    {ok, NewState};
                Error2 ->
                    {Error2, State}
            end;
        Error1 ->
            {Error1, State}
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
    V = riak_repl2_object_filter:get_version(),
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
merge_and_load_configs(repl, ReplConfig, State) ->
    %% Change Repl, MergedRT, and MergedFS
    RTConfig = riak_repl2_object_filter:get_config(loaded_realtime),
    FSConfig = riak_repl2_object_filter:get_config(loaded_fullsync),
    MergedRT = merge_config(ReplConfig, RTConfig),
    MergedFS = merge_config(ReplConfig, FSConfig),
    save_config(loaded_repl, ReplConfig),
    save_config(realtime, MergedRT),
    save_config(fullsync, MergedFS),
    update_all_nodes(State);
merge_and_load_configs(realtime, RTConfig, State) ->
    %% Change RT, MergedRT
    ReplConfig = riak_repl2_object_filter:get_config(loaded_repl),
    MergedRT = merge_config(ReplConfig, RTConfig),
    save_config(loaded_realtime, RTConfig),
    save_config(realtime, MergedRT),
    update_all_nodes(State);
merge_and_load_configs(fullsync, FSConfig, State) ->
    %% Change FS, MergedFS
    ReplConfig = riak_repl2_object_filter:get_config(loaded_repl),
    MergedFS = merge_config(ReplConfig, FSConfig),
    save_config(loaded_fullsync, FSConfig),
    save_config(fullsync, MergedFS),
    update_all_nodes(State).

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
object_filtering_clear_config(Mode, State) ->
    object_filtering_clear_config_helper(Mode, State).

object_filtering_clear_config_helper(all, State) ->
    save_config(loaded_repl, []),
    save_config(loaded_realtime, []),
    save_config(loaded_fullsync, []),
    save_config(realtime, []),
    save_config(fullsync, []),
    NewState = update_all_nodes(State),
    {ok, NewState};
object_filtering_clear_config_helper(repl, State) ->
    NewState = merge_and_load_configs(repl, [], State),
    {ok, NewState};
object_filtering_clear_config_helper(realtime, State) ->
    NewState = merge_and_load_configs(realtime, [], State),
    {ok, NewState};
object_filtering_clear_config_helper(fullsync, State) ->
    NewState = merge_and_load_configs(fullsync, [], State),
    {ok, NewState};
object_filtering_clear_config_helper(Mode, State) ->
    {return_error_unknown_repl_mode(object_filtering_clear_config, Mode), State}.
%% ====================================================================================================================
save_config(Key, Config) ->
    riak_core_metadata:put(?OBF_CONFIG_KEY, Key, Config),
    set_config(Key, Config).
save_status(Key, Status) ->
    riak_core_metadata:put(?OBF_STATUS_KEY, Key, Status),
    set_status(Key, Status).

%% ====================================================================================================================
set_config(Key, Config) ->
    application:set_env(?OBF_CONFIG_KEY, Key, Config).
set_status(Key, Status) ->
    application:set_env(?OBF_STATUS_KEY, Key, Status).
set_version(Version) ->
    application:set_env(?OBF_VERSION_KEY, active, Version).
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

print_response(ok) ->
    io:format("ok ~n");
print_response({status_single_node, Status}) ->
    print_status(Status);
print_response({status_all_nodes, AllStatus}) ->
    lists:foreach(fun(Status) -> print_status(Status) end, AllStatus);
print_response({config, Config}) ->
    io:format("~p ~n", [Config]);
print_response({all_config, AllConfig}) ->
    OD = orddict:from_list(AllConfig),
    io:format("~n", []),
    io:format("Realtime: ~n ~p ~n~n", [orddict:fetch(realtime, OD)]),
    io:format("Fullsync: ~n ~p ~n~n", [orddict:fetch(fullsync, OD)]),
    io:format("Loaded Repl: ~n ~p ~n~n", [orddict:fetch(loaded_repl, OD)]),
    io:format("Loaded Realtime: ~n ~p ~n~n", [orddict:fetch(loaded_realtime, OD)]),
    io:format("Loaded Fullsync: ~n ~p ~n~n", [orddict:fetch(loaded_fullsync, OD)]);


%% Errors
print_response({error,{rule_format, Version, Rule}}) ->
    io:format("[Object Filtering Version: ~p] Error: rule format not supported. ~p ~n",
        [Version, Rule]);
print_response({error,{duplicate_remote_entries, Version}}) ->
    io:format("[Object Filtering Version: ~p] Error: Duplicate remote entries found in config. ~n",
        [Version]);
print_response({error,{invalid_remote_name, Version, RemoteName}}) ->
    io:format("[Object Filtering Version: ~p] Error: Invalid remote name found, ~p ~n",
        [Version, RemoteName]);
print_response({error,{invalid_rule_type_allowed, Version, RemoteName, Rule}}) ->
    io:format("[Object Filtering Version: ~p] Error: Invalid rule found (in allowed rules). ~n
                Remote name: ~p ~n
                Rule: ~p ~n",
        [Version, RemoteName, Rule]);
print_response({error,{invalid_rule_type_blocked, Version, RemoteName, Rule}}) ->
    io:format("[Object Filtering Version: ~p] Error: Invalid rule found (in blocked rules). ~n
                Remote name: ~p ~n
                Rule: ~p ~n",
        [Version, RemoteName, Rule]);
print_response({error, {no_rules, Version}}) ->
    io:format("[Object Filtering Version: ~p], Error: No rules are present in the file ~n", [Version]);
print_response({error, unknown_repl_mode, object_filtering_clear_config, Mode}) ->
    io:format("Error object_filtering_clear_config: unknown_repl_mode ~p, supported modes: [all, repl, realtime, fullsync] ~n", [Mode]);
print_response({error, unknown_repl_mode, get_config_external, Mode}) ->
    io:format("Error get_config_external: unknown_repl_mode ~p, supported modes: [realtime, fullsync, loaded_repl, loaded_realtime, loaded_fullsync]~n", [Mode]);
print_response({error, unknown_repl_mode, RelatedFun, Mode}) ->
    io:format("Error ~p: unknown_repl_mode ~p, supported modes: [repl, realtime, fullsync] ~n", [RelatedFun, Mode]);
print_response({error, no_request, Request}) ->
    io:format("Error: request ~p does not exist~n", [Request]);
print_response({error, Error}) ->
    io:format("Generic error: ~p ~n", [Error]).



print_status({Node, {Version, RTStatus, FSStatus, MergedRTConfigHash, MergedFSConfigHash, _ReplConfigHash, _FSConfigHash, _RTConfigHash}}) ->
    io:format("Node: ~p\t Version: ~p\t Realtime Status: ~p \t Fullsync Status: ~p \t Realtime Config Hash: ~p \t Fullsync Config Hash: ~p~n",
        [Node, Version, RTStatus, FSStatus, MergedRTConfigHash, MergedFSConfigHash]).
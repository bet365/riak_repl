-module(riak_repl2_rtsource_conn_mgr).
-behaviour(gen_server).

-include("riak_repl.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([
    connected/7,
    connect_failed/3,
    maybe_rebalance_delayed/1,
    should_rebalance/3,
    stop/1,
    get_all_status/1,
    get_all_status/2,
    get_source_and_sink_nodes/2,
    get_endpoints/1,
    get_rtsource_conn_pids/1,

    node_watcher_update/1,
    set_leader/2
]).

-define(SERVER, ?MODULE).
-define(CURRENT_VERSION, v2).
-define(CLIENT_SPEC, {{realtime,[{3,0}, {2,0}, {1,5}]},
    {?TCP_OPTIONS, ?SERVER, self()}}).

-define(TCP_OPTIONS,  [{keepalive, true},
    {nodelay, true},
    {packet, 0},
    {active, false}]).

-record(state, {
    version,
    remote, % remote sink cluster name
    connection_ref, % reference handed out by connection manager
    rb_timeout_tref, % Rebalance timeout timer reference
    rebalance_delay_fun,
    source_nodes,
    sink_nodes,
    remove_endpoint,
    endpoints,
    leader
}).

%%%===================================================================
%%% API
%%%===================================================================


start_link(Remote) ->
    gen_server:start_link(?MODULE, [Remote], []).

connected(Socket, Transport, IPPort, Proto, RTSourceConnMgrPid, _Props, Primary) ->
    Transport:controlling_process(Socket, RTSourceConnMgrPid),
    try
    gen_server:call(RTSourceConnMgrPid,
        {connected, Socket, Transport, IPPort, Proto, _Props, Primary})
    catch
        _:Reason ->
            lager:warning("Unable to contact RT Source Conn Manager (~p). Killing it to force a reconnect", RTSourceConnMgrPid),
            exit(RTSourceConnMgrPid, {unable_to_contact, Reason}),
            ok
    end.

connect_failed(_ClientProto, Reason, RTSourceConnMgrPid) ->
    gen_server:cast(RTSourceConnMgrPid, {connect_failed, Reason}).

maybe_rebalance_delayed(Pid) ->
    gen_server:cast(Pid, rebalance_delayed).

stop(Pid) ->
    gen_server:call(Pid, stop).

get_all_status(Pid) ->
    get_all_status(Pid, infinity).

get_all_status(Pid, Timeout) ->
    gen_server:call(Pid, all_status, Timeout).

get_endpoints(Pid) ->
    gen_server:call(Pid, get_endpoints).

get_rtsource_conn_pids(Pid) ->
    gen_server:call(Pid, get_rtsource_conn_pids).

node_watcher_update(Pid) ->
    gen_server:cast(Pid, node_watcher_update).

set_leader(Pid, LeaderNode) ->
    gen_server:cast(Pid, {set_leader_node, LeaderNode}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init([Remote]) ->
    process_flag(trap_exit, true),
    _ = riak_repl2_rtq:register(Remote), % re-register to reset stale deliverfun
    E = dict:new(),
    M = fun(X) -> round(X * crypto:rand_uniform(0, 1000)) end,
    {Version, ConnectionType} =
        case riak_core_capability:get({riak_repl, realtime_connections}, legacy) of
            legacy ->
                {legacy, legacy};
            V ->
                {V, multi_connection}

        end,

    IntervalSecs = app_helper:get_env(riak_repl, realtime_core_capability_polling_interval, 60),
    Time = IntervalSecs * 1000,
    case Version == ?CURRENT_VERSION of
        true -> ok;
        false -> erlang:send_after(Time, self(), poll_core_capability)
    end,

    riak_repl_util:write_realtime_endpoints(Version, Remote, []),
    case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, ConnectionType) of
        {ok, Ref} ->
            State = #state{version = Version, remote = Remote, connection_ref = Ref, endpoints = E,
                rebalance_delay_fun = M},
            {SourceNodes, SinkNodes} =
                case get_source_and_sink_nodes(State, Remote) of
                    no_leader ->
                        {[], []};
                    {X,Y} ->
                        {X,Y}
                end,
            {ok, State#state{source_nodes = SourceNodes, sink_nodes = SinkNodes}};
        {error, Reason}->
            lager:warning("Error connecting to remote, verions: v1"),
            {stop, Reason}
    end.

%%%=====================================================================================================================
handle_call({connected, Socket, Transport, IPPort, Proto, _Props, Primary}, _From,
    State = #state{remote = Remote, endpoints = E, version = V}) ->

    lager:info("Adding a connection and starting rtsource_conn ~p", [Remote]),
    case riak_repl2_rtsource_conn:start_link(Remote) of
        {ok, RtSourcePid} ->
            case riak_repl2_rtsource_conn:connected(Socket, Transport, IPPort, Proto, RtSourcePid, _Props, Primary) of
                ok ->

                    % check remove_endpoint
                    NewState = case State#state.remove_endpoint of
                                   undefined ->
                                       State;
                                   RC ->
                                       E2 = remove_connections([RC], E, Remote, V),
                                       State#state{endpoints = E2, remove_endpoint = undefined}
                               end,

                    case dict:find({IPPort, Primary}, NewState#state.endpoints) of
                        {ok, OldRtSourcePid} ->
                            exit(OldRtSourcePid, {shutdown, rebalance, {IPPort,Primary}}),
                            lager:info("duplicate connections found, removing the old one ~p", [{IPPort,Primary}]);
                        error ->
                            ok
                    end,

                    %% Save {EndPoint, Pid}; Pid will come from the supervisor starting a child
                    NewEndpoints = dict:store({IPPort, Primary}, RtSourcePid, NewState#state.endpoints),

                    case V of
                        legacy ->
                            lager:info("Adding remote connection, however not sending to data_mgr as we are running legacy code base"),
                            ok;
                        _ ->
                            % save to ring
                            lager:info("Adding remote connections to data_mgr: ~p", [dict:fetch_keys(NewEndpoints)]),
                            riak_repl_util:write_realtime_endpoints(V, Remote, dict:fetch_keys(NewEndpoints))
                    end,

                    {reply, ok, NewState#state{endpoints = NewEndpoints}};

                Error ->
                    lager:warning("rtsource_conn failed to recieve connection ~p", [IPPort]),
                    {reply, Error, State}
            end;
        ER ->
            {reply, ER, State}
    end;

handle_call(all_status, _From, State=#state{endpoints = E}) ->
    AllKeys = dict:fetch_keys(E),
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    {reply, lists:flatten(collect_status_data(AllKeys, Timeout, [], E)), State};

handle_call(get_rtsource_conn_pids, _From, State = #state{endpoints = E}) ->
    Result = lists:foldl(fun({_,Pid}, Acc) -> Acc ++ [Pid] end, [], dict:to_list(E)),
    {reply, Result, State};

handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(get_endpoints, _From, State=#state{endpoints = E}) ->
    {reply, E, State};

handle_call(Request, _From, State) ->
    lager:warning("unhandled call: ~p", [Request]),
    {reply, ok, State}.

%%%=====================================================================================================================

%% Connection manager failed to make connection
handle_cast({connect_failed, Reason}, State = #state{remote = Remote, endpoints = E}) ->
    lager:warning("Realtime replication connection to site ~p failed - ~p\n", [Remote, Reason]),
    NewState =
        case dict:fetch_keys(E) of
            [] ->
                cancel_timer(State#state.rb_timeout_tref),
                RbTimeoutTref = erlang:send_after(0, self(), rebalance_now),
                State#state{rb_timeout_tref = RbTimeoutTref};
            _ ->
                State
        end,
    {noreply, NewState};

handle_cast(rebalance_delayed, State=#state{version = legacy}) ->
    {noreply, maybe_rebalance_legacy(State, delayed)};

handle_cast(rebalance_delayed, State) ->
    {noreply, maybe_rebalance(State, delayed)};

handle_cast({set_leader_node, LeaderNode}, State) ->
    lager:info("setting leader node as: ~p", [LeaderNode]),
    case node() of
        LeaderNode ->
            %% we do this incase it was the previous leader that died!
            %% we need to remove its connection data from the shared data store between all nodes
            handle_cast(node_watcher_update, State#state{leader=true});
        _ ->
            {noreply, State#state{leader = false}}
    end;

handle_cast(node_watcher_update, State=#state{leader = true, version = v2, remote = Remote}) ->
    %% If I am the leader, go ahead and loop the data in core_metadata and delete any data for nodes that are now down.
    %% ONLY FOR MY REMOTE!
    NewNodes = riak_core_node_watcher:nodes(riak_kv),
    AllRemoteEndpoints = riak_repl_util:read_realtime_endpoints(v2, Remote),
    OldNodes = dict:fetch_keys(AllRemoteEndpoints),
    DownNodes = OldNodes -- NewNodes,
    lists:foreach(fun(Node) -> riak_repl_util:write_realtime_endpoints(v2, Remote, [], Node) end, DownNodes),
    handle_cast(rebalance_delayed, State);

handle_cast(node_watcher_update, State) ->
    handle_cast(rebalance_delayed, State);

handle_cast(Request, State) ->
    lager:warning("unhandled cast: ~p", [Request]),
    {noreply, State}.

%%%=====================================================================================================================

handle_info({'EXIT', Pid, Reason}, State = #state{endpoints = E, remote = Remote, version = Version}) ->
    NewState = case lists:keyfind(Pid, 2, dict:to_list(E)) of
                   {Key, Pid} ->
                       NewEndpoints = dict:erase(Key, E),
                       State2 = State#state{endpoints = NewEndpoints},
                       riak_repl_util:write_realtime_endpoints(Version, Remote, dict:fetch_keys(NewEndpoints)),
                       case Reason of
                           normal ->
                               lager:info("riak_repl2_rtsource_conn terminated due to reason nomral, Endpoint: ~p", [Key]);
                           {shutdown, routine} ->
                               lager:info("riak_repl2_rtsource_conn terminated due to reason routine shutdown, Endpoint: ~p", [Key]);
                           {shutdown, heartbeat_timeout} ->
                               lager:info("riak_repl2_rtsource_conn terminated due to reason heartbeat timeout, Endpoint: ~p", [Key]);
                           {shutdown, Error} ->
                               lager:info("riak_repl2_rtsource_conn terminated due to reason ~p, Endpoint: ~p", [Error, Key]);
                           OtherError ->
                               lager:warning("riak_repl2_rtsource_conn terminated due to reason ~p, Endpoint: ~p", [OtherError, Key])
                       end,

                       case dict:fetch_keys(NewEndpoints) of
                           [] ->
                               cancel_timer(State2#state.rb_timeout_tref),
                               RbTimeoutTref = erlang:send_after(0, self(), rebalance_now),
                               State2#state{rb_timeout_tref = RbTimeoutTref};
                           _ ->
                               State2
                       end;
                   false ->
                       case Reason of
                           {shutdown, rebalance, RemovedKey} ->
                               lager:info("riak_repl2_rtsource_conn terminated due to rebalancing, Endpoint: ~p", [RemovedKey]);
                           _ ->
                               lager:warning("riak_repl2_rtsource_conn terminated due to reason ~p, [NOT IN ENDPOINTS]", [Reason])
                       end,
                       State
               end,
    {noreply, NewState};

handle_info(rebalance_now, State=#state{version = Version}) ->
    case Version of
        legacy ->
            {noreply, maybe_rebalance_legacy(State#state{rb_timeout_tref = undefined}, now)};
        _ ->
            {noreply, maybe_rebalance(State#state{rb_timeout_tref = undefined}, now)}
    end;


handle_info(poll_core_capability, State=#state{version = OldVersion}) ->
    IntervalSecs = app_helper:get_env(riak_repl, realtime_core_capability_polling_interval, 60),
    Time = IntervalSecs * 1000,
    case riak_core_capability:get({riak_repl, realtime_connections}, legacy) of
        ?CURRENT_VERSION ->
            cancel_timer(State#state.rb_timeout_tref),
            RbTimeoutTref = erlang:send_after(0, self(), rebalance_now),
            {noreply, State#state{version = ?CURRENT_VERSION, rb_timeout_tref = RbTimeoutTref}};
        OldVersion ->
            erlang:send_after(Time, self(), poll_core_capability),
            {noreply, State};
        OtherVersion ->
            cancel_timer(State#state.rb_timeout_tref),
            RbTimeoutTref = erlang:send_after(0, self(), rebalance_now),
            erlang:send_after(Time, self(), poll_core_capability),
            {noreply, State#state{version = OtherVersion, rb_timeout_tref = RbTimeoutTref}}
    end;

handle_info(Info, State) ->
    lager:warning("unhandled info: ~p", [Info]),
    {noreply, State}.

%%%=====================================================================================================================

terminate(Reason, _State=#state{version = V, remote = Remote, endpoints = E}) ->
    lager:info("rtrsource conn mgr terminating, Reason: ~p", [Reason]),
    riak_repl_util:write_realtime_endpoints(V, Remote, []),
    riak_core_connection_mgr:disconnect({rt_repl, Remote}),
    [catch riak_repl2_rtsource_conn:stop(Pid) || {{{_IP, _Port},_Primary},Pid} <- dict:to_list(E)],
    ok.

%%%=====================================================================================================================

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%%%=====================================================================================================================

%%%===================================================================
%%% Internal functions
%%%===================================================================
maybe_rebalance(#state{version = V} = State, now) ->
    RebalanceTimer = app_helper:get_env(riak_repl, realtime_rebalance_on_failure, 5) * 1000,
    case get_source_and_sink_nodes(V, State#state.remote) of
        no_leader ->
            lager:info("rebalancing triggered, but an error occured with regard to the leader in the data mgr, will rebalance in 5 seconds"),
            cancel_timer(State#state.rb_timeout_tref),
            RbTimeoutTref = erlang:send_after(RebalanceTimer, self(), rebalance_now),
            State#state{rb_timeout_tref = RbTimeoutTref};
        {NewSource, NewSink} ->
            case should_rebalance(State, NewSource, NewSink) of
                false ->
                    lager:info("rebalancing triggered but there is no change in source/sink node status and primary active connections"),
                    State;

                inconsistent_data ->
                    lager:info("rebalancing triggered, but an inconsistent data was found in realtime connections on data mgr, will rebalance in 5 seconds"),
                    riak_repl_util:write_realtime_endpoints(V, State#state.remote, dict:fetch_keys(State#state.endpoints)),
                    cancel_timer(State#state.rb_timeout_tref),
                    RbTimeoutTref = erlang:send_after(RebalanceTimer, self(), rebalance_now),
                    State#state{rb_timeout_tref = RbTimeoutTref};

                no_leader ->
                    lager:info("rebalancing triggered, but an error occured with regard to the leader in the data mgr, will rebalance in 5 seconds"),
                    cancel_timer(State#state.rb_timeout_tref),
                    RbTimeoutTref = erlang:send_after(RebalanceTimer, self(), rebalance_now),
                    State#state{rb_timeout_tref = RbTimeoutTref};

                rebalance_needed_empty_list_returned ->
                    lager:info("rebalancing triggered but get_ip_addrs_of_cluster returned [], will rebalance in 5 seconds"),
                    cancel_timer(State#state.rb_timeout_tref),
                    RbTimeoutTref = erlang:send_after(RebalanceTimer, self(), rebalance_now),
                    State#state{rb_timeout_tref = RbTimeoutTref};

                {true, {equal, DropNodes, ConnectToNodes, _Primary, _Secondary, _ConnectedSinkNodes}} ->
                    lager:info("rebalancing triggered but avoided via active connection matching ~n"
                    "drop nodes ~p ~n"
                    "connect nodes ~p", [DropNodes, ConnectToNodes]),
                    State;

                {true, {nodes_up, DropNodes, ConnectToNodes, _Primary, _Secondary, _ConnectedSinkNodes}} ->
                    lager:info("rebalancing triggered and new connections are required ~n"
                    "drop nodes ~p ~n"
                    "connect nodes ~p", [DropNodes, ConnectToNodes]),
                    NewState1 = check_remove_endpoint(State, ConnectToNodes),
                    rebalance_connect(NewState1#state{sink_nodes = NewSink, source_nodes = NewSource}, ConnectToNodes);

                {true, {nodes_down, DropNodes, ConnectToNodes, _Primary, _Secondary, ConnectedSinkNodes}} ->
                    lager:info("rebalancing triggered and active connections required to be dropped ~n"
                    "drop nodes ~p ~n"
                    "connect nodes ~p", [DropNodes, ConnectToNodes]),
                    {_RemoveAllConnections, NewState1} = check_and_drop_connections(State, DropNodes, ConnectedSinkNodes),
                    riak_repl_util:write_realtime_endpoints(V, NewState1#state.remote, dict:fetch_keys(NewState1#state.endpoints)),
                    NewState1#state{sink_nodes = NewSink, source_nodes = NewSource};

                {true, {nodes_up_and_down, DropNodes, ConnectToNodes, Primary, Secondary, ConnectedSinkNodes}} ->
                    lager:info("rebalancing triggered and some active connections required to be dropped, and also new connections to be made ~n"
                    "drop nodes ~p ~n"
                    "connect nodes ~p", [DropNodes, ConnectToNodes]),
                    {RemoveAllConnections, NewState1} = check_and_drop_connections(State, DropNodes, ConnectedSinkNodes),
                    NewState2 = check_remove_endpoint(NewState1, ConnectToNodes),
                    riak_repl_util:write_realtime_endpoints(V, NewState2#state.remote, dict:fetch_keys(NewState2#state.endpoints)),
                    case RemoveAllConnections of
                        true ->
                            rebalance_connect(NewState2#state{sink_nodes = NewSink, source_nodes = NewSource}, Primary++Secondary);
                        false ->
                            rebalance_connect(NewState2#state{sink_nodes = NewSink, source_nodes = NewSource}, ConnectToNodes)
                    end
            end
    end;

maybe_rebalance(State=#state{rebalance_delay_fun = Fun}, delayed) ->
    case State#state.rb_timeout_tref of
        undefined ->
            MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 120),
            RbTimeoutTref = erlang:send_after(Fun(MaxDelaySecs), self(), rebalance_now),
            State#state{rb_timeout_tref = RbTimeoutTref};
        _ ->
            %% Already sent a "rebalance_now"
            State
    end.


should_rebalance(State=
    #state{sink_nodes = OldSink, source_nodes = OldSource, remote = R, version = V}, NewSource, NewSink) ->
    {SourceComparison, _SourceNodesDown, _SourceNodesUp} = compare_nodes(OldSource, NewSource),
    {SinkComparison, _SinkNodesDown, _SinkNodesUp} = compare_nodes(OldSink, NewSink),
    RealtimeConnections = riak_repl_util:read_realtime_endpoints(V, R),
    case RealtimeConnections of
        no_leader ->
            no_leader;
        RTC ->
            case check_realtime_connections_consistency(RTC, State) of
                true ->
                    case {SourceComparison, SinkComparison} of
                        {equal, equal} ->
                            lager:info("rebalancing - should_rebalance hit equal equal"),
                            check_active_primary_connections(RTC, State);
                        _ ->
                            rebalance(State)
                    end;
                false ->
                    inconsistent_data
            end
    end.

check_realtime_connections_consistency(RealtimeConnections, #state{endpoints = Endpoints}) ->
    case dict:find(node(), RealtimeConnections) of
        {ok, Conns} ->
            lists:sort(Conns) == lists:sort(dict:fetch_keys(Endpoints));
        error ->
            false
    end.

check_active_primary_connections(RealtimeConnections, State) ->
    case check_active_primary_connections_source(RealtimeConnections, State) of
        true ->
            lager:info("rebalancing - source active primary connections are correct"),
            case check_active_primary_connections_sink(RealtimeConnections, State) of
                true ->
                    lager:info("rebalancing - sink active primary connections are correct"),
                    false;
                false ->
                    rebalance(State)
            end;
        false ->
            rebalance(State)
    end.


check_active_primary_connections_sink(RealtimeConnections, #state{source_nodes = SourceNodes, sink_nodes = SinkNodes})->
    InvertedRealtimeConnections = invert_dictionary(RealtimeConnections),
    Keys = dict:fetch_keys(InvertedRealtimeConnections),
    ActualConnectionCounts = lists:sort(count_primary_connections(InvertedRealtimeConnections, Keys, [])),
    ExpectedConnectionCounts = lists:sort(build_expected_primary_connection_counts(for_sink_nodes, SourceNodes, SinkNodes)),
    ActualConnectionCounts == ExpectedConnectionCounts.


check_active_primary_connections_source(RealtimeConnections, #state{source_nodes = SourceNodes, sink_nodes = SinkNodes}) ->
    Keys = dict:fetch_keys(RealtimeConnections),
    ActualConnectionCounts = lists:sort(count_primary_connections(RealtimeConnections, Keys, [])),
    ExpectedConnectionCounts = lists:sort(build_expected_primary_connection_counts(for_source_nodes, SourceNodes, SinkNodes)),
    ActualConnectionCounts == ExpectedConnectionCounts.

build_expected_primary_connection_counts(For, SourceNodes, SinkNodes) ->
    case {SourceNodes, SinkNodes} of
        {undefined, _} ->
            [];
        {_, undefined} ->
            [];
        _ ->
            {M,N} = case For of
                        for_source_nodes ->
                            {length(SourceNodes), length(SinkNodes)};
                        for_sink_nodes ->
                            {length(SinkNodes), length(SourceNodes)}
                    end,
            case M*N of
                0 ->
                    [];
                _ ->
                    case M >= N of
                        true ->
                            [1 || _ <-  lists:seq(1,M)];
                        false ->
                            Base = N div M,
                            NumberOfNodesWithOneAdditionalConnection = N rem M,
                            NumberOfNodesWithBaseConnections = M - NumberOfNodesWithOneAdditionalConnection,
                            [Base+1 || _ <-lists:seq(1,NumberOfNodesWithOneAdditionalConnection)] ++ [Base || _
                                <- lists:seq(1,NumberOfNodesWithBaseConnections)]
                    end
            end
    end.


count_primary_connections(_ConnectionDictionary, [], List) ->
    List;
count_primary_connections(ConnectionDictionary, [Key|Keys], List) ->
    NodeConnections = dict:fetch(Key, ConnectionDictionary),
    count_primary_connections(ConnectionDictionary, Keys, List ++ [get_primary_count(NodeConnections,0)]).

get_primary_count([], N) ->
    N;
get_primary_count([{_,Primary}|Rest], N) ->
    case Primary of
        true ->
            get_primary_count(Rest, N+1);
        _ ->
            get_primary_count(Rest, N)
    end.


invert_dictionary(Dictionary) ->
    Keys = dict:fetch_keys(Dictionary),
    invert_dictionary_helper(Dictionary, Keys, dict:new()).

invert_dictionary_helper(_Dictionary, [], NewDict) ->
    NewDict;
invert_dictionary_helper(Dictionary, [Key|Rest], Dict) ->
    Values = dict:fetch(Key, Dictionary),
    NewDict = invert_dictionary_helper_builder(Key, Values, Dict),
    invert_dictionary_helper(Dictionary, Rest, NewDict).

invert_dictionary_helper_builder(_Source, [], Dict) ->
    Dict;
invert_dictionary_helper_builder(Source, [{Sink,Primary}|Rest], Dict) ->
    NewDict = case Primary of
                  true ->
                      dict:append(Sink, {Source,Primary}, Dict);
                  false ->
                      Dict
              end,
    invert_dictionary_helper_builder(Source, Rest, NewDict).



rebalance(#state{endpoints = Endpoints, remote=Remote}) ->
    case riak_core_cluster_mgr:get_ipaddrs_of_cluster(Remote, split) of
        {ok, []} ->
            rebalance_needed_empty_list_returned;
        {ok, {Primary, Secondary}} ->
            ConnectedSinkNodes = [ {IPPort, P} || {{IPPort, P},_Pid} <- dict:to_list(Endpoints)],
            {Action, DropNodes, ConnectToNodes} = compare_nodes(ConnectedSinkNodes, Primary),
            {true, {Action, DropNodes, ConnectToNodes, Primary, Secondary, ConnectedSinkNodes}}
    end.


check_remove_endpoint(State=#state{remove_endpoint = RE}, ConnectToNodes) ->
    case lists:member(RE, ConnectToNodes) of
        true ->
            State#state{remove_endpoint = undefined};
        false ->
            State
    end.


check_and_drop_connections(State=#state{endpoints = E, remote = R, version = V}, DropNodes=[X|Xs], ConnectedSinkNodes) ->
    case ConnectedSinkNodes -- DropNodes of
        [] ->
            NewEndpoints = remove_connections(Xs, E, R, V),
            {true, State#state{endpoints = NewEndpoints, remove_endpoint = X}};
        _ ->
            NewEndpoints = remove_connections(DropNodes, E, R, V),
            {false, State#state{endpoints = NewEndpoints, remove_endpoint = undefined}}
    end.



remove_connections(IPAddrs, Endpoints, Remote, Version) ->
    NewDict = close_rtsource_conn(IPAddrs, Endpoints, Remote),
    riak_repl_util:write_realtime_endpoints(Version, Remote, dict:fetch_keys(NewDict)),
    NewDict.

close_rtsource_conn([], E, _) ->
    E;
close_rtsource_conn([Key | Rest], E, Remote) ->
    RtSourcePid = dict:fetch(Key, E),
    exit(RtSourcePid, {shutdown, rebalance, Key}),
    lager:info("rtsource_conn is killed ~p", [Key]),
    close_rtsource_conn(Rest, dict:erase(Key, E), Remote).

get_source_and_sink_nodes(#state{version = V}, Remote) ->
    case V of
        legacy -> {[], []};
        _ ->
            Source = riak_repl_util:read_realtime_active_nodes(V),
            case Source of
                no_leader ->
                    no_leader;
                SourceNodes ->
                    SinkNodes = riak_core_cluster_mgr:get_unshuffled_ipaddrs_of_cluster(Remote),
                    {SourceNodes, SinkNodes}
            end
    end.

compare_nodes(Old, New) ->
    case Old == New of
        true ->
            {equal, [],[]};
        false ->
            {NodesDownRes, NodesDown} = diff_nodes(Old, New),
            {NodesUpRes, NodesUp} = diff_nodes(New, Old),
            case {NodesDownRes, NodesUpRes} of
                {true, true} ->
                    {nodes_up_and_down, NodesDown, NodesUp};
                {true, false} ->
                    {nodes_down, NodesDown, NodesUp};
                {false, true} ->
                    {nodes_up, NodesDown, NodesUp};
                {false, false} ->
                    %% we should never reach these case statement
                    {equal, NodesDown, NodesUp}
            end
    end.

diff_nodes(N1, N2) ->
    case N1 -- N2 of
        [] ->
            {false, []};
        X ->
            {true, X}
    end.

rebalance_connect(State=#state{remote=Remote}, BetterAddrs) ->
    lager:info("trying reconnect to one of: ~p", [BetterAddrs]),

    %% if we have a pending connection attempt - drop that
    riak_core_connection_mgr:disconnect({rt_repl, Remote}),

    lager:debug("re-connecting to remote ~p", [Remote]),
    case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, BetterAddrs}) of
        {ok, Ref} ->
            lager:debug("connecting ref ~p", [Ref]),

            lager:debug("rebalanced is complete"),

            State#state{connection_ref = Ref};
        {error, Reason}->
            lager:warning("Error connecting to remote ~p (ignoring as we're reconnecting)", [Reason]),
            State
    end.

cancel_timer(undefined) -> ok;
cancel_timer(TRef)      -> _ = erlang:cancel_timer(TRef).

collect_status_data([], _Timeout, Data, _E) ->
    Data;
collect_status_data([Key | Rest], Timeout, Data, E) ->
    Pid = dict:fetch(Key, E),
    NewData = riak_repl2_rtsource_conn:status(Pid, Timeout),
    collect_status_data(Rest, Timeout, [NewData|Data], E).

%% ------------------------------------------------------------------------------------------------------------------ %%
%%                                                 Legacy Code                                                        %%
%% ------------------------------------------------------------------------------------------------------------------ %%

maybe_rebalance_legacy(State=#state{rebalance_delay_fun = Fun}, delayed) ->
    case State#state.rb_timeout_tref of
        undefined ->
            MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 120),
            RbTimeoutTref = erlang:send_after(Fun(MaxDelaySecs), self(), rebalance_now),
            State#state{rb_timeout_tref = RbTimeoutTref};
        _ ->
            %% Already sent a "rebalance_now"
            State
    end;

maybe_rebalance_legacy(State, now) ->
    case should_rebalance_legacy(State) of
        no ->
            State;
        {yes, UsefulAddrs} ->
            reconnect_legacy(State, UsefulAddrs)
    end.

should_rebalance_legacy(#state{endpoints = E, remote=Remote}) ->
    ConnectedAddr = case dict:fetch_keys(E) of
                        [] ->
                            [];
                        [C] ->
                            C;
                        MultipleConns ->
                            lager:warning("(legacy) multiple connections found and should not have been"),
                            hd(MultipleConns)
                    end,
    {ok, ShuffledAddrs} = riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(Remote),
    case (ShuffledAddrs /= []) andalso same_ipaddr(ConnectedAddr, hd(ShuffledAddrs)) of
        true ->
            no; % we're already connected to the ideal buddy
        false ->
            %% compute the addrs that are "better" than the currently connected addr
            BetterAddrs = lists:filter(fun(A) -> not same_ipaddr(ConnectedAddr, A) end,
                ShuffledAddrs),
            %% remove those that are blacklisted anyway
            UsefulAddrs = riak_core_connection_mgr:filter_blacklisted_ipaddrs(BetterAddrs),
            lager:debug("(legacy) betterAddrs: ~p, UsefulAddrs ~p", [BetterAddrs, UsefulAddrs]),
            case UsefulAddrs of
                [] ->
                    no;
                UsefulAddrs ->
                    {yes, UsefulAddrs}
            end
    end.

reconnect_legacy(State=#state{endpoints=Endpoints, remote=Remote, version = Version}, BetterAddrs) ->
    lager:info("(legacy) trying reconnect to one of: ~p", [BetterAddrs]),

    % remove current connection
    Keys = dict:fetch_keys(Endpoints),
    NewEndpoints = remove_connections(Keys, Endpoints, Remote, Version),

    lager:debug("(legacy) re-connecting to remote ~p", [Remote]),
    case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, BetterAddrs}) of
        {ok, Ref} ->
            lager:debug("(legacy) connecting ref ~p", [Ref]),
            State#state{connection_ref = Ref, endpoints = NewEndpoints};
        {error, Reason}->
            lager:warning("(legacy) error connecting to remote ~p (ignoring as we're reconnecting)", [Reason]),
            State#state{endpoints = NewEndpoints}
    end.

same_ipaddr({{IP,Port},_}, {{IP,Port},_}) ->
    true;
same_ipaddr({{_IP1,_Port1},_}, {{_IP2,_Port2}, _}) ->
    false;
same_ipaddr(X,Y) ->
    lager:warning("(legacy) ipaddrs have unexpected format! ~p, ~p", [X,Y]),
    false.
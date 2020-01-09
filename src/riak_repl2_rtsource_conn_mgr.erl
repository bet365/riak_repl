-module(riak_repl2_rtsource_conn_mgr).
-behaviour(gen_server).

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
    get_source_and_sink_nodes/1,
    get_endpoints/1,
    get_rtsource_conn_pids/1
]).

-define(SERVER, ?MODULE).

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
    rebalance_timer,
    max_delay,
    source_nodes,
    sink_nodes,
    endpoints = orddict:new(),
    remove_endpoint = orddict:new()
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


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init([Remote]) ->
    process_flag(trap_exit, true),

    _ = riak_repl2_rtq:register(Remote), % re-register to reset stale deliverfun
    MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 120),
    M = fun(X) -> round(X * crypto:rand_uniform(0, 1000)) end,
    RebalanceTimer = app_helper:get_env(riak_repl, realtime_rebalance_on_failure, 5),

    Concurrency = app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)),
    List1 = lists:foldl(fun(N, Acc) -> [{N, dict:new()} | Acc] end, [], lists:seq(1, Concurrency)),
    List2 = lists:foldl(fun(N, Acc) -> [{N, undefined} | Acc] end, [], lists:seq(1, Concurrency)),

    Endpoints = orddict:from_list(List1),
    RemoveEndpoints = orddict:from_list(List2),


    case riak_core_capability:get({riak_repl, realtime_connections}, legacy) of
        legacy ->
            case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, legacy) of
                {ok, Ref} ->
                    {ok, #state{version = legacy, remote = Remote, connection_ref = Ref,
                        rebalance_delay_fun = M, rebalance_timer=RebalanceTimer*1000, max_delay=MaxDelaySecs,
                        source_nodes = [], sink_nodes = [], endpoints = Endpoints, remove_endpoint = RemoveEndpoints}};
                {error, Reason}->
                    lager:warning("Error connecting to remote, verions: legacy"),
                    {stop, Reason}
            end;
        v1 ->
            riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, Remote, node(), []),
            case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, multi_connection) of
                {ok, Ref} ->
                    {SourceNodes, SinkNodes} = case get_source_and_sink_nodes(Remote) of
                                                   no_leader ->
                                                       {[], []};
                                                   {X,Y} ->
                                                       {X,Y}
                                               end,

                    {ok, #state{version = v1, remote = Remote, connection_ref = Ref,
                        rebalance_delay_fun = M, rebalance_timer=RebalanceTimer*1000, max_delay=MaxDelaySecs,
                        source_nodes = SourceNodes, sink_nodes = SinkNodes, endpoints = Endpoints, remove_endpoint = RemoveEndpoints}};
                {error, Reason}->
                    lager:warning("Error connecting to remote, verions: v1"),
                    {stop, Reason}
            end
    end.

%%%=====================================================================================================================
handle_call({connected, Socket, Transport, IPPort, Proto, _Props, {Primary, N}}, _From,
    State = #state{remote = Remote, endpoints = E, version = V, remove_endpoint = R}) ->

    Endpoints1 = get_endpoints(State, N),
    RemoveEndpoint1 = get_remove_endpoint(State, N),

    lager:info("Adding a connection and starting rtsource_conn ~p", [Remote]),
    lager:info("ADDING CONN FOR REMOTE: ~p, ~p ~p ", [Remote, Primary, N]),
    case riak_repl2_rtsource_conn:start_link(Remote) of
        {ok, RtSourcePid} ->
            case riak_repl2_rtsource_conn:connected(Socket, Transport, IPPort, Proto, RtSourcePid, _Props, {Primary, N}) of
                ok ->

                    % check remove_endpoint
                    NewState = case RemoveEndpoint1 of
                                   undefined ->
                                       State;
                                   RC ->
                                       Endpoints2 = remove_connections([RC], Endpoints1, Remote, V, N),
                                       E2 = orddict:store(N, Endpoints2, E),
                                       State#state{endpoints = E2, remove_endpoint = orddict:store(N, undefined, R)}
                               end,

                    Endpoints3 = get_endpoints(NewState, N),
                    case dict:find({IPPort, Primary}, Endpoints3) of
                        {ok, OldRtSourcePid} ->
                            exit(OldRtSourcePid, {shutdown, rebalance, {IPPort,Primary}}),
                            lager:info("duplicate connections found, removing the old one ~p", [{IPPort,Primary}]);
                        error ->
                            ok
                    end,

                    %% Save {EndPoint, Pid}; Pid will come from the supervisor starting a child
                    NewEndpoints = dict:store({IPPort, Primary}, RtSourcePid, Endpoints3),

                    case V of
                        legacy ->
                            lager:info("Adding remote connection, however not sending to data_mgr as we are running legacy code base"),
                            ok;
                        v1 ->
                            % save to ring

                            case N == 1 of
                                true ->
                                    lager:info("Adding remote connections to data_mgr: ~p", [dict:fetch_keys(NewEndpoints)]),
                                    riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, Remote, node(), dict:fetch_keys(NewEndpoints));
                                false ->
                                    lager:info("SECOND CONN, NOT ADDING TO DATA MGR: ~p", [dict:fetch_keys(NewEndpoints)])
                            end
                    end,

                    {reply, ok, NewState#state{endpoints = orddict:store(N, NewEndpoints, NewState#state.endpoints)}};

                Error ->
                    lager:warning("rtsource_conn failed to recieve connection ~p", [IPPort]),
                    {reply, Error, State}
            end;
        ER ->
            {reply, ER, State}
    end;

handle_call(all_status, _From, State = #state{endpoints = E}) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    Result = lists:flatten(orddict:fold(
        fun(_, V, Acc) ->
            R = lists:flatten(collect_status_data(dict:fetch_keys(V), Timeout, [], V)),
            [R|Acc]
        end,
        [], E)),
    {reply, Result, State};

handle_call(get_rtsource_conn_pids, _From, State = #state{endpoints = E}) ->
    Result = lists:flatten(orddict:fold(
        fun(_, V, Acc1) ->
            R = lists:foldl(fun({_,Pid}, Acc2) -> Acc2 ++ [Pid] end, [], dict:to_list(V)),
            [R|Acc1]
        end,
        [], E)),
    {reply, Result, State};

handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(get_endpoints, _From, State) ->
    {reply, get_endpoints(State, 1), State};

handle_call(Request, _From, State) ->
    lager:warning("unhandled call: ~p", [Request]),
    {reply, ok, State}.

%%%=====================================================================================================================

%% Connection manager failed to make connection
handle_cast({connect_failed, Reason}, State = #state{remote = Remote}) ->
    lager:warning("Realtime replication connection to site ~p failed - ~p\n", [Remote, Reason]),
    E = get_endpoints(State, 1),
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

handle_cast(rebalance_delayed, State=#state{version = Version}) ->
    case Version of
        legacy ->
            {noreply, maybe_rebalance_legacy(State, delayed)};
        v1 ->
            {noreply, maybe_rebalance(State, delayed)}
    end;

handle_cast(Request, State) ->
    lager:warning("unhandled cast: ~p", [Request]),
    {noreply, State}.

%%%=====================================================================================================================

handle_info({'EXIT', Pid, Reason}, State = #state{endpoints = E, remote = Remote, version = Version}) ->

    Res =
        orddict:fold(
            fun(N, V, Result) ->
                case lists:keyfind(Pid, 2, dict:to_list(V)) of
                    {Key, Pid} -> {Key, Pid, N};
                    _ -> Result
                end
            end, false, E),

    NewState = case Res of
                   {Key, Pid, N} ->
                       NewE = orddict:map(fun(_, Dict) -> dict:erase(Key, Dict) end, E),
                       State2 = State#state{endpoints = NewE},
                       {IPPort, Primary} = Key,

                       orddict:fold(
                           fun(K, V, ok) ->
                               case K of
                                   N ->
                                       ok;
                                   _ ->
                                       case dict:fetch(Key, V) of
                                           {ok, P} ->
                                               exit(P, {shutdown, rebalance, Key}),
                                               ok;
                                           _ ->
                                               ok
                                       end
                               end
                           end, ok, E),


                       case {N == 1, Version} of
                           {true, v1} ->
                               riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, Remote, node(), IPPort, Primary);
                           _ ->
                               ok
                       end,

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


                       case dict:fetch_keys(get_endpoints(State2, 1)) of
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
        v1 ->
            {noreply, maybe_rebalance(State#state{rb_timeout_tref = undefined}, now)}
    end;

handle_info(upgrade_connection_version, State) ->
    cancel_timer(State#state.rb_timeout_tref),
    RbTimeoutTref = erlang:send_after(0, self(), rebalance_now),
    {noreply, State#state{version = v1, rb_timeout_tref = RbTimeoutTref}};

handle_info(Info, State) ->
    lager:warning("unhandled info: ~p", [Info]),
    {noreply, State}.

%%%=====================================================================================================================

terminate(Reason, _State=#state{remote = Remote, endpoints = E}) ->
    lager:info("rtrsource conn mgr terminating, Reason: ~p", [Reason]),
    riak_core_connection_mgr:disconnect({rt_repl, Remote}),
    orddict:fold(
        fun(_, V, ok) ->
            [catch riak_repl2_rtsource_conn:stop(Pid) || {{{_IP, _Port},_Primary},Pid} <- dict:to_list(V)],
            ok
        end, ok, E),
    ok.

%%%=====================================================================================================================

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%%%=====================================================================================================================

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_endpoints(#state{endpoints = E}, N) ->
    case orddict:find(N, E) of
        {ok, EP} -> EP;
        _ -> dict:new()
    end.

get_remove_endpoint(#state{remove_endpoint = R}, N) ->
    case orddict:find(N, R) of
        {ok, REP} -> REP;
        _ -> undefined
    end.


maybe_rebalance(State, now) ->
    case get_source_and_sink_nodes(State#state.remote) of
        no_leader ->
            lager:info("rebalancing triggered, but an error occured with regard to the leader in the data mgr, will rebalance in 5 seconds"),
            cancel_timer(State#state.rb_timeout_tref),
            RbTimeoutTref = erlang:send_after(State#state.rebalance_timer, self(), rebalance_now),
            State#state{rb_timeout_tref = RbTimeoutTref};
        {NewSource, NewSink} ->
            case should_rebalance(State, NewSource, NewSink) of
                false ->
                    lager:info("rebalancing triggered but there is no change in source/sink node status and primary active connections"),
                    State;

                inconsistent_data ->
                    lager:info("rebalancing triggered, but an inconsistent data was found in realtime connections on data mgr, will rebalance in 5 seconds"),
                    riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, State#state.remote, node(), dict:fetch_keys(get_endpoints(State, 1))),
                    cancel_timer(State#state.rb_timeout_tref),
                    RbTimeoutTref = erlang:send_after(State#state.rebalance_timer, self(), rebalance_now),
                    State#state{rb_timeout_tref = RbTimeoutTref};

                no_leader ->
                    lager:info("rebalancing triggered, but an error occured with regard to the leader in the data mgr, will rebalance in 5 seconds"),
                    cancel_timer(State#state.rb_timeout_tref),
                    RbTimeoutTref = erlang:send_after(State#state.rebalance_timer, self(), rebalance_now),
                    State#state{rb_timeout_tref = RbTimeoutTref};

                rebalance_needed_empty_list_returned ->
                    lager:info("rebalancing triggered but get_ip_addrs_of_cluster returned [], will rebalance in 5 seconds"),
                    cancel_timer(State#state.rb_timeout_tref),
                    RbTimeoutTref = erlang:send_after(State#state.rebalance_timer, self(), rebalance_now),
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
                    riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, NewState1#state.remote, node(), dict:fetch_keys(get_endpoints(NewState1, 1))),
                    NewState1#state{sink_nodes = NewSink, source_nodes = NewSource};

                {true, {nodes_up_and_down, DropNodes, ConnectToNodes, Primary, Secondary, ConnectedSinkNodes}} ->
                    lager:info("rebalancing triggered and some active connections required to be dropped, and also new connections to be made ~n"
                    "drop nodes ~p ~n"
                    "connect nodes ~p", [DropNodes, ConnectToNodes]),
                    {RemoveAllConnections, NewState1} = check_and_drop_connections(State, DropNodes, ConnectedSinkNodes),
                    NewState2 = check_remove_endpoint(NewState1, ConnectToNodes),
                    riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, NewState2#state.remote, node(), dict:fetch_keys(get_endpoints(NewState2, 1))),
                    case RemoveAllConnections of
                        true ->
                            rebalance_connect(NewState2#state{sink_nodes = NewSink, source_nodes = NewSource}, Primary++Secondary);
                        false ->
                            rebalance_connect(NewState2#state{sink_nodes = NewSink, source_nodes = NewSource}, ConnectToNodes)
                    end
            end
    end;

maybe_rebalance(State=#state{rebalance_delay_fun = Fun, max_delay = M}, delayed) ->
    case State#state.rb_timeout_tref of
        undefined ->
            RbTimeoutTref = erlang:send_after(Fun(M), self(), rebalance_now),
            State#state{rb_timeout_tref = RbTimeoutTref};
        _ ->
            %% Already sent a "rebalance_now"
            State
    end.


should_rebalance(State=#state{sink_nodes = OldSink, source_nodes = OldSource, remote = R}, NewSource, NewSink) ->
    {SourceComparison, _SourceNodesDown, _SourceNodesUp} = compare_nodes(OldSource, NewSource),
    {SinkComparison, _SinkNodesDown, _SinkNodesUp} = compare_nodes(OldSink, NewSink),
    RealtimeConnections = riak_repl2_rtsource_conn_data_mgr:read(realtime_connections, R),
    case RealtimeConnections of
        no_leader ->
            no_leader;
        RTC ->
            case check_data_mgr_conn_mgr_data_consistency(RTC, State) of
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

check_data_mgr_conn_mgr_data_consistency(RealtimeConnections, State) ->

    case dict:find(node(), RealtimeConnections) of
        {ok, Conns} ->
            lists:sort(Conns) == lists:sort(dict:fetch_keys(get_endpoints(State, 1)));
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



rebalance(State = #state{remote=Remote}) ->
    case riak_core_cluster_mgr:get_ipaddrs_of_cluster(Remote, split) of
        {ok, []} ->
            rebalance_needed_empty_list_returned;
        {ok, {Primary, Secondary}} ->
            ConnectedSinkNodes = [{IPPort, P} || {{IPPort, P},_Pid} <- dict:to_list(get_endpoints(State, 1))],
            {Action, DropNodes, ConnectToNodes} = compare_nodes(ConnectedSinkNodes, Primary),
            {true, {Action, DropNodes, ConnectToNodes, Primary, Secondary, ConnectedSinkNodes}}
    end.


check_remove_endpoint(State, ConnectToNodes) ->
    RE = get_remove_endpoint(State, 1),
    case lists:member(RE, ConnectToNodes) of
        true ->
            State#state{remove_endpoint = orddict:map(fun(_, _) -> undefined end, RE)};
        false ->
            State
    end.


check_and_drop_connections(State=#state{endpoints = E, remove_endpoint = RE, remote = R, version = V}, DropNodes=[X|Xs], ConnectedSinkNodes) ->
    case ConnectedSinkNodes -- DropNodes of
        [] ->
            NewE = orddict:map(
                fun(N, Endpoints) ->
                    remove_connections(Xs, Endpoints, R, V, N)
                end, E),
            NewRE = orddict:map(fun(_, _) -> X end, RE),
            {true, State#state{endpoints = NewE, remove_endpoint = NewRE}};
        _ ->
            NewE = orddict:map(
                fun(N, Endpoints) ->
                    remove_connections(Xs, Endpoints, R, V, N)
                end, E),
            NewRE = orddict:map(fun(_, _) -> undefined end, RE),
            {false, State#state{endpoints = NewE, remove_endpoint = NewRE}}
    end.

remove_connections([], E, _, _, _) ->
    E;
remove_connections([Key={Addr, Primary} | Rest], E, Remote, Version, N) ->
    RtSourcePid = dict:fetch(Key, E),
    exit(RtSourcePid, {shutdown, rebalance, Key}),
    lager:info("rtsource_conn is killed ~p", [Key]),
    case {N==1, Version} of
        {true, v1} ->
            riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, Remote, node(), Addr, Primary);
        _ ->
            ok
    end,
    remove_connections(Rest, dict:erase(Key, E), Remote, Version, N).

get_source_and_sink_nodes(Remote) ->
    Source = riak_repl2_rtsource_conn_data_mgr:read(active_nodes),
    case Source of
        no_leader ->
            no_leader;
        SourceNodes ->
            SinkNodes = riak_core_cluster_mgr:get_unshuffled_ipaddrs_of_cluster(Remote),
            {SourceNodes, SinkNodes}
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

maybe_rebalance_legacy(State=#state{rebalance_delay_fun = Fun, max_delay = M}, delayed) ->
    case State#state.rb_timeout_tref of
        undefined ->
            RbTimeoutTref = erlang:send_after(Fun(M), self(), rebalance_now),
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

should_rebalance_legacy(State = #state{remote=Remote}) ->
    ConnectedAddr = case dict:fetch_keys(get_endpoints(State, 1)) of
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

reconnect_legacy(State=#state{endpoints = E, remote=Remote, version = Version}, BetterAddrs) ->
    lager:info("(legacy) trying reconnect to one of: ~p", [BetterAddrs]),

    % remove current connection
    Keys = dict:fetch_keys(get_endpoints(State, 1)),
    NewEndpoints = remove_connections(Keys, get_endpoints(State, 1), Remote, Version, 1),

    lager:debug("(legacy) re-connecting to remote ~p", [Remote]),
    case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, BetterAddrs}) of
        {ok, Ref} ->
            lager:debug("(legacy) connecting ref ~p", [Ref]),
            State#state{connection_ref = Ref, endpoints = orddict:store(1, NewEndpoints, E)};
        {error, Reason}->
            lager:warning("(legacy) error connecting to remote ~p (ignoring as we're reconnecting)", [Reason]),
            State#state{endpoints = orddict:store(1, NewEndpoints, E)}
    end.

same_ipaddr({{IP,Port},_}, {{IP,Port},_}) ->
    true;
same_ipaddr({{_IP1,_Port1},_}, {{_IP2,_Port2}, _}) ->
    false;
same_ipaddr(X,Y) ->
    lager:warning("(legacy) ipaddrs have unexpected format! ~p, ~p", [X,Y]),
    false.
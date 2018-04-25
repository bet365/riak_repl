-module(riak_repl2_rtsource_conn_mgr).
-author("nordine saadouni").
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
  remote, % remote sink cluster name
  connection_ref, % reference handed out by connection manager
  rb_timeout_tref, % Rebalance timeout timer reference
  rebalance_delay_fun,
  max_delay,
  kill_time,
  source_nodes,
  sink_nodes,
  remove_endpoint,
  endpoints
}).

%%%===================================================================
%%% API
%%%===================================================================


start_link(Remote) ->
  gen_server:start_link(?MODULE, [Remote], []).

connected(Socket, Transport, IPPort, Proto, RTSourceConnMgrPid, _Props, Primary) ->
  Transport:controlling_process(Socket, RTSourceConnMgrPid),
  gen_server:call(RTSourceConnMgrPid, {connected, Socket, Transport, IPPort, Proto, _Props, Primary}).

connect_failed(_ClientProto, Reason, RTSourceConnMgrPid) ->
  gen_server:cast(RTSourceConnMgrPid, {connect_failed, Reason}).

maybe_rebalance_delayed(Pid) ->
  gen_server:cast(Pid, rebalance_delayed).

stop(Pid) ->
  gen_server:call(Pid, stop).

%%connection_closed(Pid, Addr, Primary) ->
%%  gen_server:call(Pid, {connection_closed, Addr, Primary}).

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
  riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, Remote, node(), []),

  lager:debug("connecting to remote ~p", [Remote]),
  case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, multi_connection) of
    {ok, Ref} ->
      _ = riak_repl2_rtq:register(Remote), % re-register to reset stale deliverfun
      lager:debug("connection ref ~p", [Ref]),
      E = dict:new(),

      MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 120),
      lager:debug("max delay for connection rebalancing: ~p", [MaxDelaySecs]),
      M = fun(X) -> round(X * crypto:rand_uniform(0, 1000)) end,
      {SourceNodes, SinkNodes} = case get_source_and_sink_nodes(Remote) of
                                   no_leader ->
                                     {[], []};
                                   {X,Y} ->
                                     {X,Y}
                                 end,

      KillTimeSecs = app_helper:get_env(riak_repl, realtime_connection_removal_delay, 60),
      KillTime = KillTimeSecs * 1000,
      lager:debug("realtime connection removal delay: ~p", [KillTime]),

      lager:debug("conn_mgr node source: ~p", [SourceNodes]),
      lager:debug("conn_mgr node sink: ~p", [SinkNodes]),

      {ok, #state{remote = Remote, connection_ref = Ref, endpoints = E, rebalance_delay_fun = M,
        max_delay=MaxDelaySecs, source_nodes = SourceNodes, sink_nodes = SinkNodes, kill_time = KillTime}};
    {error, Reason}->
      lager:warning("Error connecting to remote"),
      {stop, Reason}
  end.

%%%=====================================================================================================================
handle_call({connected, Socket, Transport, IPPort, Proto, _Props, Primary}, _From,
    State = #state{remote = Remote, endpoints = E, kill_time = K}) ->

  lager:debug("rtsource_conn_mgr connection recieved ~p", [{IPPort, Primary}]),

  lager:info("Adding a connection and starting rtsource_conn ~p", [Remote]),
  case riak_repl2_rtsource_conn:start_link(Remote) of
    {ok, RtSourcePid} ->
      lager:debug("we have added the connection"),
      case riak_repl2_rtsource_conn:connected(Socket, Transport, IPPort, Proto, RtSourcePid, _Props, Primary) of
        ok ->

          % check remove_endpoint
          NewState = case State#state.remove_endpoint of
                       undefined ->
                        State;
                      RC ->
                        E2 = remove_connections([RC], E, {K,Remote}),
                        State#state{endpoints = E2, remove_endpoint = undefined}
                     end,

          %% Save {EndPoint, Pid}; Pid will come from the supervisor starting a child
          NewEndpoints = dict:store({IPPort, Primary}, RtSourcePid, NewState#state.endpoints),

          % save to ring
          lager:debug("rtsource_conn_mgr send connection data to data mgr"),
          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, Remote, node(), dict:fetch_keys(NewEndpoints)),

          {reply, ok, NewState#state{endpoints = NewEndpoints}};

        Error ->
          lager:debug("rtsouce_conn failed to recieve connection ~p", [IPPort]),
          {reply, Error, State}
      end;
    ER ->
      {reply, ER, State}
  end;

handle_call(all_status, _From, State=#state{endpoints = E}) ->
  AllKeys = dict:fetch_keys(E),
  {reply, collect_status_data(AllKeys, [], E), State};

handle_call(get_rtsource_conn_pids, _From, State = #state{endpoints = E}) ->
  Result = lists:foldl(fun({_,Pid}, Acc) -> Acc ++ [Pid] end, [], dict:to_list(E)),
  {reply, Result, State};

%%handle_call({connection_closed, Addr, Primary}, _From, State=#state{endpoints = E, remote = R}) ->
%%  NewEndpoints = dict:erase({Addr,Primary}, E),
%%  riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, R, node(), Addr, Primary),
%%  NewState = case NewEndpoints of
%%    [] ->
%%      RbTimeoutTref = erlang:send_after(0, self(), rebalance_now),
%%      State#state{rb_timeout_tref = RbTimeoutTref};
%%    _ ->
%%      State
%%  end,
%%  {reply, ok, NewState#state{endpoints = NewEndpoints}};

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};

handle_call(get_endpoints, _From, State=#state{endpoints = E}) ->
  {reply, E, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%%=====================================================================================================================

%% Connection manager failed to make connection
handle_cast({connect_failed, Reason}, State = #state{remote = Remote, endpoints = E}) ->
  lager:warning("Realtime replication connection to site ~p failed - ~p\n", [Remote, Reason]),
  NewState =
    case dict:fetch_keys(E) of
      [] ->
        RbTimeoutTref = erlang:send_after(0, self(), rebalance_now),
        State#state{rb_timeout_tref = RbTimeoutTref};
      _ ->
        State
    end,
  {noreply, NewState};

handle_cast(rebalance_delayed, State) ->
  {noreply, maybe_rebalance(State, delayed)};

handle_cast(_Request, State) ->
  {noreply, State}.

%%%=====================================================================================================================

handle_info({'EXIT', Pid, Reason}, State = #state{endpoints = E, remote = Remote}) ->
  case Reason of
    normal ->
      lager:info("riak_repl2_rtsource_conn terminated due to reason nomral");
    {rtsource_conn, heartbeat_timeout} ->
      lager:info("riak_repl2_rtsource_conn terminated due to reason heartbeat timeout");
    {rtsource_conn, Error} ->
      lager:info("riak_repl2_rtsource_conn terminated due to reason ~p", [Error]);
    OtherError ->
      lager:warning("riak_repl2_rtsource_conn terminated due to reason ~p", [OtherError])
  end,

  NewState = case lists:keyfind(Pid, 2, dict:to_list(E)) of
              {Key, Pid} ->
                NewEndpoints = dict:erase(Key, E),
                State2 = State#state{endpoints = NewEndpoints},
                {IPPort, Primary} = Key,
                riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, Remote, node(), IPPort, Primary),
                case dict:fetch_keys(NewEndpoints) of
                  [] ->
                    RbTimeoutTref = erlang:send_after(0, self(), rebalance_now),
                    State2#state{rb_timeout_tref = RbTimeoutTref};
                  _ ->
                    State2
                end;
              false ->
                State
            end,
  {noreply, NewState};

handle_info(rebalance_now, State) ->
  {noreply, maybe_rebalance(State#state{rb_timeout_tref = undefined}, now)};

handle_info({kill_rtsource_conn, RtSourceConnPid}, State) ->
  catch riak_repl2_rtsource_conn:stop(RtSourceConnPid),
  {noreply, State};


handle_info(_Info, State) ->
  {noreply, State}.

%%%=====================================================================================================================

terminate(_Reason, _State=#state{remote = Remote, endpoints = E}) ->
  lager:info("rtrsource conn mgr terminating"),
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
maybe_rebalance(State, now) ->
  case get_source_and_sink_nodes(State#state.remote) of
    no_leader ->
      lager:info("rebalancing triggered, but an error occured with regard to the leader in the data mgr, will rebalance in 5 seconds"),
      RbTimeoutTref = erlang:send_after(5000, self(), rebalance_now),
      %% update data manager
%%      riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, State#state.remote, node(), dict:fetch_keys(State#state.endpoints)),
      State#state{rb_timeout_tref = RbTimeoutTref};
    {NewSource, NewSink} ->
      case should_rebalance(State, NewSource, NewSink) of
        false ->
          %% update data manager
%%          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, State#state.remote, node(), dict:fetch_keys(State#state.endpoints)),
          lager:info("rebalancing triggered but there is no change in source/sink node status and primary active connections"),
          State;

        no_leader ->
          lager:info("rebalancing triggered, but an error occured with regard to the leader in the data mgr, will rebalance in 5 seconds"),
          RbTimeoutTref = erlang:send_after(5000, self(), rebalance_now),
          %% update data manager
%%          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, State#state.remote, node(), dict:fetch_keys(State#state.endpoints)),
          State#state{rb_timeout_tref = RbTimeoutTref};

        rebalance_needed_empty_list_returned ->
          lager:info("rebalancing triggered but get_ip_addrs_of_cluster returned [], will rebalance in 5 seconds"),
          %% update data manager
%%          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, State#state.remote, node(), dict:fetch_keys(State#state.endpoints)),
          State;

        {true, {equal, DropNodes, ConnectToNodes, _Primary, _Secondary, _ConnectedSinkNodes}} ->
          lager:info("rebalancing triggered but avoided via active connection matching
      drop nodes ~p
      connect nodes ~p", [DropNodes, ConnectToNodes]),
          %% update data manager
%%          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, State#state.remote, node(), dict:fetch_keys(State#state.endpoints)),
          State;

        {true, {nodes_up, DropNodes, ConnectToNodes, _Primary, _Secondary, _ConnectedSinkNodes}} ->
          lager:info("rebalancing triggered and new connections are required
      drop nodes ~p
      connect nodes ~p", [DropNodes, ConnectToNodes]),
          NewState1 = check_remove_endpoint(State, ConnectToNodes),
          %% update data manager
%%          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, NewState1#state.remote, node(), dict:fetch_keys(NewState1#state.endpoints)),
          rebalance_connect(NewState1#state{sink_nodes = NewSink, source_nodes = NewSource}, ConnectToNodes);

        {true, {nodes_down, DropNodes, ConnectToNodes, _Primary, _Secondary, ConnectedSinkNodes}} ->
          lager:info("rebalancing triggered and active connections required to be dropped
      drop nodes ~p
      connect nodes ~p", [DropNodes, ConnectToNodes]),
          {_RemoveAllConnections, NewState1} = check_and_drop_connections(State, DropNodes, ConnectedSinkNodes),
          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, NewState1#state.remote, node(), dict:fetch_keys(NewState1#state.endpoints)),
          NewState1#state{sink_nodes = NewSink, source_nodes = NewSource};

        {true, {nodes_up_and_down, DropNodes, ConnectToNodes, Primary, Secondary, ConnectedSinkNodes}} ->
          lager:info("rebalancing triggered and some active connections required to be dropped, and also new connections to be made
      drop nodes ~p
      connect nodes ~p", [DropNodes, ConnectToNodes]),
          {RemoveAllConnections, NewState1} = check_and_drop_connections(State, DropNodes, ConnectedSinkNodes),
          NewState2 = check_remove_endpoint(NewState1, ConnectToNodes),
          riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, NewState2#state.remote, node(), dict:fetch_keys(NewState2#state.endpoints)),
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


should_rebalance(State=#state{sink_nodes = OldSink, source_nodes = OldSource}, NewSource, NewSink) ->
  {SourceComparison, _SourceNodesDown, _SourceNodesUp} = compare_nodes(OldSource, NewSource),
  {SinkComparison, _SinkNodesDown, _SinkNodesUp} = compare_nodes(OldSink, NewSink),
  case {SourceComparison, SinkComparison} of
    {equal, equal} ->
      lager:info("rebalancing - should_rebalance hit equal equal"),
      check_active_primary_connections(State);
    _ ->
      rebalance(State)
  end.

check_active_primary_connections(State) ->
  case check_active_primary_connections_source(State) of
    {true, RealtimeConnections} ->
      lager:info("rebalancing - source active primary connections are correct"),
      case check_active_primary_connections_sink(RealtimeConnections, State) of
        true ->
          lager:info("rebalancing - sink active primary connections are correct"),
          false;
        _ ->
          rebalance(State)
      end;
    no_leader ->
      no_leader;
    _ ->
      rebalance(State)
  end.


check_active_primary_connections_sink(RealtimeConnections, #state{source_nodes = SourceNodes, sink_nodes = SinkNodes})->
  InvertedRealtimeConnections = invert_dictionary(RealtimeConnections),
  Keys = dict:fetch_keys(InvertedRealtimeConnections),
  ActualConnectionCounts = lists:sort(count_primary_connections(InvertedRealtimeConnections, Keys, [])),
  ExpectedConnectionCounts = lists:sort(build_expected_primary_connection_counts(for_sink_nodes, SourceNodes, SinkNodes)),
  ActualConnectionCounts == ExpectedConnectionCounts.


check_active_primary_connections_source(#state{remote=R, source_nodes = SourceNodes, sink_nodes = SinkNodes}) ->
  RealtimeConnections = riak_repl2_rtsource_conn_data_mgr:read(realtime_connections, R),
  case RealtimeConnections of
    no_leader ->
      no_leader;
    RTC ->
      Keys = dict:fetch_keys(RTC),
      ActualConnectionCounts = lists:sort(count_primary_connections(RTC, Keys, [])),
      ExpectedConnectionCounts = lists:sort(build_expected_primary_connection_counts(for_source_nodes, SourceNodes, SinkNodes)),
      Exp = ActualConnectionCounts == ExpectedConnectionCounts,
      {Exp, RTC}
  end.

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


check_and_drop_connections(State=#state{endpoints = E, kill_time = K, remote = R}, DropNodes=[X|Xs], ConnectedSinkNodes) ->
  case ConnectedSinkNodes -- DropNodes of
    [] ->
      NewEndpoints = remove_connections(Xs, E, {K,R}),
      {true, State#state{endpoints = NewEndpoints, remove_endpoint = X}};
    _ ->
      NewEndpoints = remove_connections(DropNodes, E, {K,R}),
      {false, State#state{endpoints = NewEndpoints, remove_endpoint = undefined}}
  end.

remove_connections([], E, _) ->
  E;
remove_connections([Key={Addr, Primary} | Rest], E, {KillTime, Remote}) ->
  RtSourcePid = dict:fetch(Key, E),
  HelperPid = riak_repl2_rtsource_conn:get_helper_pid(RtSourcePid),
  riak_repl2_rtsource_helper:stop_pulling(HelperPid),
  lager:debug("rtsource_conn called to gracefully kill itself ~p", [Key]),
  erlang:send_after(KillTime, self(), {kill_rtsource_conn, RtSourcePid}),
  riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, Remote, node(), Addr, Primary),
  remove_connections(Rest, dict:erase(Key, E), {KillTime, Remote}).

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

collect_status_data([], Data, _E) ->
  Data;
collect_status_data([Key | Rest], Data, E) ->
  Pid = dict:fetch(Key, E),
  NewData = [riak_repl2_rtsource_conn:status(Pid) | Data],
  collect_status_data(Rest, NewData, E).
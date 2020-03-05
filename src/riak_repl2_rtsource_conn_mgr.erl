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
    connected/6,
    connect_failed/4,
    maybe_rebalance/1,
    stop/1,
    get_all_status/1,
    get_all_status/2,
    get_rtsource_conn_pids/1
]).

-define(SERVER, ?MODULE).
-define(DEFAULT_NO_CONNECTIONS, 400).
-define(CLIENT_SPEC, {{realtime,[{3,0}, {2,0}, {1,5}]}, {?TCP_OPTIONS, ?SERVER, self()}}).
-define(TCP_OPTIONS,  [{keepalive, true}, {nodelay, true}, {packet, 0}, {active, false}]).

-record(state, {
    remote = undefined,                                 %% remote sink cluster name

    %% current active connections
    connections_monitor_addrs = orddict:new(),          %% monitor references mapped to addr
    connections_monitor_pids = orddict:new(),           %% monitor references mapped to pid

    %% connection counts
    connection_counts = orddict:new(),                  %% number of established connections per ip addr
    balanced_connection_counts = orddict:new(),         %% the balanced version of connection_counts (the ideal to hit)

    number_of_connection = 0,
    number_of_pending_connects = 0,
    number_of_pending_disconnects = 0,
    balancing = false,
    balanced = false,

    connection_failed_counts = orddict:new(), %% for stats

    %% This list is constantly rotated to provide the ability to balance out connections when we cannot connect
    %% to a node, so we use a secondary node instead
    sink_nodes = [],
    bad_sink_nodes = [],

    rb_timeout_tref,                                    %% rebalance timeout timer reference
    bs_timeout_tref,                                    %% bad sink timer to retry the bad sinks
    reference_rtq                                       %% reference queue pid
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Remote) ->
    gen_server:start_link(?MODULE, [Remote], []).

connected(Socket, Transport, IPPort, Proto, RTSourceConnMgrPid, Props) ->
    Transport:controlling_process(Socket, RTSourceConnMgrPid),
    try
    gen_server:call(RTSourceConnMgrPid,
        {connected, Socket, Transport, IPPort, Proto, Props})
    catch
        _:Reason ->
            lager:warning("Unable to contact RT Source Conn Manager (~p). Killing it to force a reconnect", RTSourceConnMgrPid),
            exit(RTSourceConnMgrPid, {unable_to_contact, Reason}),
            ok
    end.

connect_failed(_ClientProto, Reason, RTSourceConnMgrPid, Addr) ->
    gen_server:cast(RTSourceConnMgrPid, {connect_failed, Reason, Addr}).

maybe_rebalance(Pid) ->
    gen_server:cast(Pid, maybe_rebalance).

stop(Pid) ->
    gen_server:call(Pid, stop).

get_all_status(Pid) ->
    get_all_status(Pid, infinity).
get_all_status(Pid, Timeout) ->
    gen_server:call(Pid, all_status, Timeout).

get_rtsource_conn_pids(Pid) ->
    gen_server:call(Pid, get_rtsource_conn_pids).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init([Remote]) ->
    case whereis(riak_repl2_reference_rtq:name(Remote)) of
        undefined ->
            lager:error("undefined reference rtq for remote: ~p", [Remote]),
            {stop, {error, "undefined reference rtq"}};
        ReferenceQ ->
            SinkNodes = riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(Remote),
            State = #state{remote = Remote, reference_rtq = ReferenceQ, sink_nodes = SinkNodes},
            NewState = rebalance_connections(State),
            {ok, NewState}
    end.

%%%=====================================================================================================================
handle_call({connected, Socket, Transport, Addr, Proto, Props}, _From, State) ->
    accept_connection(Socket, Transport, Addr, Proto, Props, State);

handle_call(all_status, _From, State=#state{connections_monitor_pids = ConnectionsMonitorPids}) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    {reply, lists:flatten(collect_status_data(ConnectionsMonitorPids, Timeout)), State};

handle_call(get_rtsource_conn_pids, _From, State = #state{connections_monitor_pids = ConnectionMonitorPids}) ->
    Result = orddict:fold(fun(_, Pid, Acc) -> [Pid | Acc] end, [], ConnectionMonitorPids),
    {reply, Result, State};

handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(Request, _From, State) ->
    lager:warning("unhandled call: ~p", [Request]),
    {reply, ok, State}.

%%%=====================================================================================================================
handle_cast({connect_failed, Reason, Addr}, State) ->
    #state{remote = Remote} = State,
    lager:warning("Realtime replication connection to site ~p; address: ~p; failed - ~p\n", [Remote, Addr, Reason]),
    {noreply, connection_failed(Addr, State)};

handle_cast(maybe_rebalance, State=#state{sink_nodes = SinkNodes, bad_sink_nodes = BadSinkNodes, remote = Remote}) ->
    NewSinkNodes = riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(Remote),
    case NewSinkNodes == SinkNodes of
        true ->
            {noreply, State};
        false ->
            BadSinkNodesDown = BadSinkNodes -- NewSinkNodes,
            NewBadSinkNodes = BadSinkNodes -- BadSinkNodesDown,
            State0 = State#state{bad_sink_nodes = NewBadSinkNodes, sink_nodes = NewSinkNodes},
            State1 = start_rebalance_timer(State0),
            State2 = update_balanced_connections(State1),
            {noreply, State2}
    end;

handle_cast(Request, State) ->
    lager:warning("unhandled cast: ~p", [Request]),
    {noreply, State}.

%%%=====================================================================================================================
handle_info({'DOWN', MonitorRef, process, Pid, {error, sink_shutdown}}, State) ->
    %% add to bad nodes
    %% trigger rebalance (or just leave it and wait for the rebalance to be triggered by the ring update)
    {noreply, State};


handle_info({'DOWN', MonitorRef, process, Pid, {error, source_rebalance}}, State) ->
    %% decrease number of disconnects
    %% update balancing
    %% do nothing else
    {noreply, State};

%% while we operate with cluster on protocol 3, we will hit here on a sink node shutdown!
%% do we want to issue a reconnect to the node going down?
handle_info({'DOWN', MonitorRef, process, Pid, _Reason}, State) ->

    %% unexpected shutdown/ protocol 3 sink node shutting down
    %% just trigger rebalance, the code will now place the node into bad nodes if it needs to

    %% check if we lose all connections to a node, place into bad nodes!


    %% reconnect to the same node in X seconds?
    #state{connections_monitor_pids = ConnectionMonitorPids} = State,
    case orddict:find(MonitorRef, ConnectionMonitorPids) of
        error ->
            lager:error("could not find monitor ref: ~p in ConnectionMonitors (handle_info 'DOWN')", [MonitorRef]),
            {noreply, State};
        {ok, {Pid, Addr}} ->

            NewState = State#state{connections_monitor_pids = orddict:erase(MonitorRef, ConnectionMonitorPids)},
            update_connection_counts(NewState, Addr, -1),

            {noreply, State}
    end;

handle_info(rebalance_now, State) ->
    {noreply, maybe_do_rebalance(State#state{rb_timeout_tref = undefined})};

handle_info(try_bad_sink_nodes, State = #state{bad_sink_nodes = []}) ->
    {noreply, State};
handle_info(try_bad_sink_nodes, State = #state{bad_sink_nodes = BadSinkNodes, remote = Remote}) ->
    lists:foreach(
        fun(Addr) ->
            _ = riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, [Addr]})
        end, BadSinkNodes),
    {noreply, State#state{bs_timeout_tref = undefined}};


handle_info(Info, State) ->
    lager:warning("unhandled info: ~p", [Info]),
    {noreply, State}.

%%%=====================================================================================================================

terminate(Reason, _State=#state{remote = Remote, connections_monitor_pids = ConnectionMonitorPids}) ->
    lager:info("rtrsource conn mgr terminating, Reason: ~p", [Reason]),
    riak_core_connection_mgr:disconnect({rt_repl, Remote}),
    orddict:fold(
        fun(Ref, Pid, _) ->
            erlang:demonitor(Ref),
            catch riak_repl2_rtsource_conn:stop(Pid),
            ok
        end, ok, ConnectionMonitorPids),
    ok.

%%%=====================================================================================================================

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%%%=====================================================================================================================

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_rebalance_timer(State = #state{rb_timeout_tref = undefined}) ->
    MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 60),
    TimeDelay =  10000 + round(MaxDelaySecs * crypto:rand_uniform(0, 1000)),
    RbTimeoutTref = erlang:send_after(TimeDelay, self(), rebalance_now),
    State#state{rb_timeout_tref = RbTimeoutTref};
start_rebalance_timer(State) ->
    State.


start_bad_sink_timer(State = #state{bs_timeout_tref = undefined}) ->
    TimeDelay = app_helper:get_env(riak_repl, rt_retry_bad_sinks, 120) * 1000,
    BsTimeoutTref = erlang:send_after(TimeDelay, self(), try_bad_sink_nodes),
    State#state{bs_timeout_tref = BsTimeoutTref};
start_bad_sink_timer(State) ->
    State.

%%%=====================================================================================================================
%% Connection Failed
%%%=====================================================================================================================
connection_failed(Addr, State = #state{connection_failed_counts = Dict, bad_sink_nodes = BadSinkNodes}) ->
    case lists:member(Addr, BadSinkNodes) of
        false ->
            NewDict = orddict:update_counter(Addr, 1, Dict),
            State1 = update_number_of_pending_connections(State#state{connection_failed_counts = NewDict}),
            State2 = update_balanced_status(State1),
            update_bad_sink_nodes(State2);
        true ->
            start_bad_sink_timer(State)
    end.

%%%=====================================================================================================================
%% Accept Connection
%%%=====================================================================================================================
accept_connection(Socket, Transport, IPPort, Proto, Props, State) ->
    #state{remote = Remote} = State,
    {BadSinkReconnect, State0} = reset_bad_sink_node(IPPort, State),
    case riak_repl2_rtsource_conn:start_link(Remote) of
        {ok, RtSourcePid} ->
            Ref = erlang:monitor(process, RtSourcePid),
            case riak_repl2_rtsource_conn:connected(RtSourcePid, Ref, Socket, Transport, IPPort, Proto, Props) of
                ok ->
                    State1 = update_state_for_new_connection(BadSinkReconnect, Ref, RtSourcePid, IPPort, State0),
                    {reply, ok, State1};
                Error ->
                    erlang:demonitor(Ref),
                    exit(RtSourcePid, unable_to_connect),
                    catch Transport:close(),
                    lager:warning("rtsource_conn failed to recieve connection ~p", [IPPort]),
                    {reply, Error, State}
            end;
        ER ->
            {reply, ER, State}
    end.

update_state_for_new_connection(false, Ref, RtSourcePid, IPPort, State) ->
    State1 = update_connections_monitors(State, Ref, RtSourcePid, IPPort),
    State2 = update_connection_counts(State1, IPPort),
    State3 = update_number_of_pending_connections(State2),
    State4 = update_balanced_status(State3),
    update_bad_sink_nodes(State4);
update_state_for_new_connection(true, Ref, RtSourcePid, IPPort, State) ->
    State1 = update_connections_monitors(State, Ref, RtSourcePid, IPPort),
    update_connection_counts(State1, IPPort).

%%%=====================================================================================================================
%% Update Connection States
%%%=====================================================================================================================
update_connections_monitors(State, Ref, Pid, Addr) ->
    #state{connections_monitor_pids = Pids, connections_monitor_addrs = Addrs} = State,
    State#state
    {
        connections_monitor_addrs = orddict:store(Ref, Addr, Addrs),
        connections_monitor_pids = orddict:store(Ref, Pid, Pids)
    }.

update_connection_counts(Addr, State) ->
    update_connection_counts(Addr, 1, State).
update_connection_counts(Addr, Diff, State) ->
    #state{connection_counts = ConnectionCounts} = State,
    State#state{connection_counts = orddict:update_counter(Addr, Diff, ConnectionCounts)}.

update_number_of_pending_connections(State = #state{number_of_pending_connects = N}) ->
    X = N -1,
    Balancing = X == 0,
    State#state{number_of_pending_connects = X, balancing = Balancing}.

update_balanced_status(State = #state{connection_counts = A, balanced_connection_counts = B}) ->
    State#state{balanced = A == B}.

update_balanced_connections(State) ->
    #state{remote = Remote, bad_sink_nodes = BadSinks} = State,
    TotalNumberOfConnections = get_number_of_connections(Remote),
    case riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(Remote) -- BadSinks of
        {ok, []} ->
            [];
        {ok, SinkNodes} ->
            NumberOfSinkNodes = length(SinkNodes),
            TotalExtraConnections = TotalNumberOfConnections rem NumberOfSinkNodes,
            AverageNumberOfConnections = (TotalNumberOfConnections - TotalExtraConnections) div NumberOfSinkNodes,

            {BalancedConnectionCounts, _} =
                lists:foldl(
                    fun(Addr, {Acc, Counter}) ->
                        case Counter =< TotalExtraConnections of
                            true ->
                                {orddict:store(Addr, AverageNumberOfConnections +1, Acc), Counter +1};
                            false ->
                                {orddict:store(Addr, AverageNumberOfConnections, Acc), Counter +1}
                        end
                    end, {orddict:new(), 1}, SinkNodes),
            State#state{balanced_connection_counts = BalancedConnectionCounts}
    end.

update_bad_sink_nodes(State = #state{balancing = true}) ->
    State;
update_bad_sink_nodes(State = #state{connection_failed_counts = []}) ->
    State;
update_bad_sink_nodes(State = #state{connection_counts = Conns}) ->
    case orddict:fold(fun find_no_sink_connections/3, [], Conns) of
        [] ->
            State#state{connection_failed_counts = orddict:new()};
        BadSinks ->
            State1 = State#state{bad_sink_nodes = BadSinks, connection_failed_counts = orddict:new()},
            State2 = update_balanced_connections(State1),
            State3 = update_balanced_status(State2),
            State4 = start_rebalance_timer(State3),
            start_bad_sink_timer(State4)
    end.

find_no_sink_connections(Addr, 0, Acc) ->
    [Addr | Acc];
find_no_sink_connections(_, _, Acc) ->
    Acc.


reset_bad_sink_node({Addr, _}, State = #state{bad_sink_nodes = BadSinkNodes}) ->
    case lists:delete(Addr, BadSinkNodes) of
        BadSinkNodes ->
            {false, State};
        NewBadSinkNodes ->
            State1 = State#state{bad_sink_nodes = NewBadSinkNodes},
            State2 = update_balanced_connections(State1),
            State3 = update_balanced_status(State2),
            {true, start_rebalance_timer(State3)}
    end.


%%%=====================================================================================================================
%% Maybe Rebalance
%%%=====================================================================================================================

%% we are balanced, we do not require rebalancing
maybe_do_rebalance(State = #state{balanced = true}) ->
    State;

%% we have hit here, but we are in the middle of the previous rebalance -> so reschedule
maybe_do_rebalance(State = #state{balancing = true}) ->
    start_rebalance_timer(State);

maybe_do_rebalance(State) ->
    rebalance_connections(State).

%%%=====================================================================================================================
%% Rebalance Connections
%%%=====================================================================================================================
rebalance_connections(State) ->
    case should_add_connections(State) of
        {true, Add} ->
            NewState = start_rebalance_timer(State),
            add_connections(Add, NewState#state{balancing = true});
        false ->
            case should_remove_connections(State) of
                {true, Remove} ->
                    remove_connections(Remove, State#state{balancing = true});
                false ->
                    State#state{balancing = false, balanced = true}
            end
    end.

%%%=====================================================================================================================
%% Calculate Connections To Add
%%%=====================================================================================================================
should_add_connections(State) ->
    #state{connection_counts = ConnectionCounts, balanced_connection_counts = BalancedConnectionCounts} = State,
    case BalancedConnectionCounts of
        [] ->
            false;
        BalancedConnections ->
            RebalanceConnectionCounts =
                orddict:merge(fun(_, V1, V2) -> V1 - V2 end, BalancedConnections, ConnectionCounts),

            Add =
                orddict:fold(
                    fun(Key, Value, Acc) ->
                            case Value > 0 of
                                true ->
                                    orddict:store(Key, Value, Acc);
                                false ->
                                    Acc
                            end
                    end, orddict:new(), RebalanceConnectionCounts),
            case Add of
                [] ->
                    false;
                _ ->
                    {true, Add}
            end
    end.

%%%=====================================================================================================================
%% Calculate Connections To Remove
%%%=====================================================================================================================
should_remove_connections(State) ->
    #state{
        connection_counts = ConnectionCounts,
        balanced_connection_counts = BalancedConnectionCounts
    } = State,
    case BalancedConnectionCounts of
        [] ->
            false;
        BalancedConnections ->

            %% calculate number of connections to be established (or terminated if negative)
            RebalanceConnectionCounts =
                orddict:merge(fun(_, V1, V2) -> V1 - V2 end, BalancedConnections, ConnectionCounts),

            Remove =
                orddict:fold(
                    fun(Key, Value, Acc) ->
                        case Value < 0 of
                            true ->
                                orddict:store(Key, Value, Acc);
                            false ->
                                Acc
                        end
                    end, orddict:new(), RebalanceConnectionCounts),
            case Remove of
                [] ->
                    false;
                _ ->
                    {true, Remove}
            end
    end.




%% terminate connections gracefully (use lists:keytake(Addr, 2, Dict))
remove_connections(RemoveDict, State = #state{connections_monitor_addrs = Addrs}) ->
    {NewState, _} = orddict:fold(fun remove_connection/3, {State, RemoveDict}, Addrs),
    NewState.

remove_connection(_Ref, _Addr, {State, []}) ->
    {State, []};
remove_connection(Ref, Addr, {State, RemoveDict}) ->
    #state{connections_monitor_pids = Pids, number_of_pending_disconnects = N} = State,

    case orddict:find(Addr, RemoveDict) of
        false ->
            {State, RemoveDict};
        {ok, Count} ->
            Pid = orddict:fetch(Ref, Pids),
            riak_repl2_rtsource_conn:graceful_shutdown(Pid, source_rebalance),
            NewState = State#state{number_of_pending_disconnects = N +1},
            case Count -1 of
                0 ->
                    NewRemoveDict = orddict:erase(Addr, RemoveDict),
                    {NewState, NewRemoveDict};
                NewCount ->
                    NewRemoveDict = orddict:store(Addr, NewCount, RemoveDict),
                    {NewState, NewRemoveDict}
            end

    end.



%%%=====================================================================================================================
%% Add Connections
%%%=====================================================================================================================
add_connections(AddConnectionDict, State) ->
    orddict:fold(fun add_sink_conns/3, State, AddConnectionDict).

add_sink_conns(Addr, N, State) ->
    connect_to_sink_n_times(Addr, N, State).

%% Add New Connections
connect_to_sink_n_times(_Addr, 0, State) ->
    State;
connect_to_sink_n_times(Addr, N, State) ->
    connect_to_sink_n_times(Addr, N-1, connect_to_sink(Addr, State)).

connect_to_sink(Addr, State) ->
    #state{number_of_pending_connects = N, remote = Remote} = State,
    case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, [Addr]}) of
        {ok, _Ref} ->
            State#state{number_of_pending_connects = N +1};
        _->
            State
    end.
%%%=====================================================================================================================

collect_status_data(ConnectionMonitors, Timeout) ->
    orddict:fold(
        fun(_, Pid, Acc) ->
            case riak_repl2_rtsource_conn:status(Pid, Timeout) of
                [] -> Acc;
                Status -> [Status | Acc]
            end
        end, [], ConnectionMonitors).

-ifdef(TEST).
get_number_of_connections(_) ->
    app_helper:get_env(riak_repl, default_number_of_connections, ?DEFAULT_NO_CONNECTIONS).

-else.

get_number_of_connections(Name) ->
    case riak_core_metadata:get(?RIAK_REPL2_CONFIG_KEY, {number_of_connections, Name}) of
        undefined -> app_helper:get_env(riak_repl, default_number_of_connections, ?DEFAULT_NO_CONNECTIONS);
        NumberOfConnections -> NumberOfConnections
    end.
-endif.
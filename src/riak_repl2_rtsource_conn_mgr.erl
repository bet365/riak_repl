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

    %% connection counts (for rebalancing)
    pending_connection_counts = orddict:new(),          %% number of of pending connections per ip addr
    connection_counts = orddict:new(),                  %% number of established connections per ip addr
    reject_connection_counts = orddict:new(),           %% number of connectios per ipaddr that need to be rejected

    %% sink nodes (used to determine if we need to rebalance)
    sink_nodes = [],

    rb_timeout_tref,                                    %% rebalance timeout timer reference
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
    #state{reject_connection_counts = RejectConnections} = State,
    case orddict:find(Addr, RejectConnections) of
        error ->
            accept_connection(Socket, Transport, Addr, Proto, Props, State);
        {ok, Count} ->
            reject_connection(Socket, Transport, Addr, Count, State)
    end;

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

handle_cast(maybe_rebalance, State=#state{rb_timeout_tref = undefined, sink_nodes = SinkNodes, remote = Remote}) ->
    NewSinkNodes = riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(Remote),
    case NewSinkNodes == SinkNodes of
        true ->
            {noreply, State};
        false ->
            MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 60),
            TimeDelay =  10 + round(MaxDelaySecs * crypto:rand_uniform(0, 1000)),
            RbTimeoutTref = erlang:send_after(TimeDelay, self(), rebalance_now),
            {noreply, State#state{rb_timeout_tref = RbTimeoutTref, sink_nodes = NewSinkNodes}}
    end;

handle_cast(maybe_rebalance, State) ->
    {noreply, State};

handle_cast(Request, State) ->
    lager:warning("unhandled cast: ~p", [Request]),
    {noreply, State}.

%%%=====================================================================================================================
handle_info({'DOWN', MonitorRef, process, Pid, {error, sink_shutdown}}, State) ->
    %% connect to another node (calculate an even distribtion)
    {noreply, State};


handle_info({'DOWN', MonitorRef, process, Pid, {error, source_rebalance}}, State) ->
    %% do not reconnect, this has already been done
    {noreply, State};

%% while we operate with cluster on protocol 3, we will hit here on a sink node shutdown!
%% do we want to issue a reconnect to the node going down?
handle_info({'DOWN', MonitorRef, process, Pid, _Reason}, State) ->
    %% reconnect to the same node in X seconds?
    #state{connections_monitor_pids = ConnectionMonitorPids} = State,
    case orddict:find(MonitorRef, ConnectionMonitorPids) of
        error ->
            lager:error("could not find monitor ref: ~p in ConnectionMonitors (handle_info 'DOWN')", [MonitorRef]),
            {noreply, State};
        {ok, {Pid, Addr}} ->
            %% Here we should issue a reconnect
            maybe_rebalance(self()),

            NewState = State#state{connections_monitor_pids = orddict:erase(MonitorRef, ConnectionMonitorPids)},
            update_connection_counts(NewState, Addr, -1),

            {noreply, State}
    end;

handle_info(rebalance_now, State) ->
    {noreply, rebalance_connections(State#state{rb_timeout_tref = undefined})};

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

%%%=====================================================================================================================
%% Connection Failed
%%%=====================================================================================================================
connection_failed(Addr, State) ->
    #state{reject_connection_counts = RejectConnectionCounts} = State,
    case orddict:find(Addr, RejectConnectionCounts) of
        error ->
            maybe_rebalance(self()),
            update_pending_connections(Addr, State);
        {ok, Count} ->
            update_reject_connections_count(Count, Addr, State)
    end.

%%%=====================================================================================================================
%% Accept Connection
%%%=====================================================================================================================
accept_connection(Socket, Transport, IPPort, Proto, Props, State) ->
    #state{remote = Remote} = State,
    case riak_repl2_rtsource_conn:start_link(Remote) of
        {ok, RtSourcePid} ->
            Ref = erlang:monitor(process, RtSourcePid),
            case riak_repl2_rtsource_conn:connected(RtSourcePid, Ref, Socket, Transport, IPPort, Proto, Props) of
                ok ->
                    State1 = update_connections_monitors(State, {connected, Ref, RtSourcePid, IPPort}),
                    State2 = update_connection_counts(State1, IPPort),
                    State3 = update_pending_connections(State2, IPPort),
                    {reply, ok, State3};
                Error ->
                    erlang:demonitor(Ref),
                    exit(RtSourcePid, unable_to_connect),
                    lager:warning("rtsource_conn failed to recieve connection ~p", [IPPort]),
                    maybe_rebalance(self()),
                    {reply, Error, State}
            end;
        ER ->
            maybe_rebalance(self()),
            {reply, ER, State}
    end.

%%%=====================================================================================================================
%% Reject Connection
%%%=====================================================================================================================
reject_connection(Socket, Transport, Addr, Count, State) ->
    catch Transport:close(Socket),
    update_reject_connections_count(Addr, Count, State).

%%%=====================================================================================================================
%% Update Connection States
%%%=====================================================================================================================
update_connections_monitors(State, {connected, Ref, Pid, Addr}) ->
    #state{connections_monitor_pids = Pids, connections_monitor_addrs = Addrs} = State,
    State#state
    {
        connections_monitor_addrs = orddict:store(Ref, Addr, Addrs),
        connections_monitor_pids = orddict:store(Ref, Pid, Pids)
    }.

%% TODO: make use of update_counter
update_pending_connections(State, Addr) ->
    update_pending_connections(State, Addr, -1).
update_pending_connections(State, Addr, Diff) ->
    #state{pending_connection_counts = PendingConnections} = State,
    NewPendingConnections =
        case orddict:find(Addr, PendingConnections) of
            error ->
                lager:error("we have a new connection that is not in pending connections (accept connection)"),
                PendingConnections;
            {ok, N} ->
                orddict:store(Addr, N +Diff, PendingConnections)
        end,
    State#state{pending_connection_counts = NewPendingConnections}.

%% TODO: make use of update_counter
update_connection_counts(State, Addr) ->
    update_connection_counts(State, Addr, 1).
update_connection_counts(State, Addr, Diff) ->
    #state{connection_counts = ConnectionCoutns} = State,
    case orddict:find(Addr, ConnectionCoutns) of
        error ->
            State#state{connection_counts = orddict:store(Addr, Diff, ConnectionCoutns)};
        {ok, Count} ->
            State#state{connection_counts = orddict:store(Addr, Count +Diff, ConnectionCoutns)}
    end.

update_reject_connections_count(Count, Addr, State) ->
    #state{reject_connection_counts = RejectConnectionCounts} = State,
    NewRejectConnectionCounts =
        case Count - 1 == 0 of
            true ->
                orddict:erase(Addr, RejectConnectionCounts);
            false ->
                orddict:store(Addr, Count -1, RejectConnectionCounts)
        end,
    State#state{reject_connection_counts = NewRejectConnectionCounts}.

%%%=====================================================================================================================
%% Rebalance Connections
%%%=====================================================================================================================
rebalance_connections(State) ->
    #state{
        connection_counts = ConnectionCounts,
        pending_connection_counts = PendingConnectionCounts,
        reject_connection_counts = RejectConnectionCounts
    } = State,
    case calculate_balanced_connections(State) of
        [] ->
            lager:warning("No available sink nodes to connect too (rebalance connections)"),
            maybe_rebalance(self()),
            State;

        BalancedConnections ->

            %% merge pending + rejected connection as these are in flight and can be used as part of the rebalance
            MergedPendingRejectedCounts =
                orddict:merge(fun(_, V1, V2) -> V1 + V2 end, RejectConnectionCounts, PendingConnectionCounts),

            %% calculate connected + pending + rejected connection numbers (as these are all connected or in flight)
            MergedConnectionCounts =
                orddict:merge(fun(_, V1, V2) -> V1 + V2 end, ConnectionCounts, MergedPendingRejectedCounts),

            %% calculate number of connections to be established (or terminated if negative)
            RebalanceConnectionCounts =
                orddict:merge(fun(_, V1, V2) -> V1 - V2 end, BalancedConnections, MergedConnectionCounts),

            %% split up list, to know which to establish, and which to terminate
            {AddConnections, RemoveConnections} =
                orddict:fold(
                    fun
                        (_Key, 0, {AC, RC}) ->
                            {AC, RC};
                        (Key, Value, {AC, RC}) ->
                        case Value > 0 of
                            true ->
                                {orddict:store(Key, Value, AC), RC};
                            false ->
                                {AC, orddict:store(Key, Value, RC)}
                        end
                    end, {orddict:new(), orddict:new()}, RebalanceConnectionCounts),


            %% reset reject_connection_counts and re-calculate them
            State0 = State#state{reject_connection_counts = orddict:new()},

            %% remove connections gracefully
            State1 = orddict:fold(fun add_sink_conns/3, State0, AddConnections),

            %% reject pending connections (as to not termiante too many open valid connections)
            %% the only reason we would have pending connection for a address which we have determined
            %% to be terminated, is if we rebalance soon after a rebalance
            {State2, ConnectionsToBeTerminated} =
                orddict:fold(fun reject_pending_connections/3, {State1, orddict:new()}, RemoveConnections),

            %% Remove the remainder of connections that need to shutdown gracefully
            terminate_connections_gracefully(ConnectionsToBeTerminated, State2)
    end.




%% terminate connections gracefully (use lists:keytake(Addr, 2, Dict))
terminate_connections_gracefully(Dict, State = #state{connections_monitor_addrs = Addrs}) ->
    ok.




%% Reject Pending Connections
reject_pending_connections(Addr, N, {State, Dict}) ->
    #state{
        pending_connection_counts = PendingConnectionCounts,
        reject_connection_counts = RejectConnectionCounts
    } = State,
    case orddict:find(Addr, PendingConnectionCounts) of
        error ->
            State;
        {ok, Pending} ->
            case Pending >= N of
                true ->
                    NewPendingConnectionCounts = orddict:store(Addr, Pending - N, PendingConnectionCounts),
                    NewRejectConnectionCounts = orddict:store(Addr, N, RejectConnectionCounts),
                    NewState =
                        State#state
                        {
                            pending_connection_counts = NewPendingConnectionCounts,
                            reject_connection_counts = NewRejectConnectionCounts
                        },
                    {NewState, Dict};
                false ->
                    ConnectionsToTerminate = N - Pending,
                    NewPendingConnectionCounts = orddict:store(Addr, 0, PendingConnectionCounts),
                    NewRejectConnectionCounts = orddict:store(Addr, Pending, RejectConnectionCounts),
                    NewState =
                        State#state
                        {
                            pending_connection_counts = NewPendingConnectionCounts,
                            reject_connection_counts = NewRejectConnectionCounts
                        },
                    {NewState, orddict:store(Addr, ConnectionsToTerminate, Dict)}

            end
    end.

add_sink_conns(Addr, N, State) ->
    connect_to_sink_n_times(Addr, N, State).

%% Add New Connections
connect_to_sink_n_times(_Addr, 0, State) ->
    State;
connect_to_sink_n_times(Addr, N, State) ->
    connect_to_sink_n_times(Addr, N-1, connect_to_sink(Addr, State)).

connect_to_sink(Addr, State) ->
    #state{pending_connection_counts = PendingConnectionCounts, remote = Remote} = State,
    case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, [Addr]}) of
        {ok, _Ref} ->
            Count =
                case orddict:find(Addr, PendingConnectionCounts) of
                    error -> 0;
                    {ok, Count0} -> Count0
                end,
            NewPendingConnections = orddict:store(Addr, Count +1, PendingConnectionCounts),
            State#state{pending_connection_counts = NewPendingConnections};
        _->
            State
    end.


calculate_balanced_connections(State) ->
    #state{remote = Remote} = State,
    TotalNumberOfConnections = get_number_of_connections(Remote),
    case riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(Remote) of
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
            BalancedConnectionCounts
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
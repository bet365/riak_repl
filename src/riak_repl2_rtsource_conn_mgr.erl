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
    sink_shutdown/2,
    stop/1,
    get_all_status/1,
    get_all_status/2,
    get_rtsource_conn_pids/1
]).

-define(SERVER, ?MODULE).
-define(DEFAULT_NO_CONNECTIONS, 400).
-define(CLIENT_SPEC, {{realtime,[{3,0}, {2,0}, {1,5}]},
    {?TCP_OPTIONS, ?SERVER, self()}}).

-define(TCP_OPTIONS,  [{keepalive, true},
    {nodelay, true},
    {packet, 0},
    {active, false}]).

-record(state, {
    remote = undefined,                         %% remote sink cluster name
    connections_monitors = orddict:new(),       %% monitor references mapped to {pid, addr}
    connections = orddict:new(),                %% addr's mapped to another orddict with {ref, pid}
    pending_connection_counts = orddict:new(),  %% number of of pending connections per ip addr
    connection_counts = orddict:new(),          %% number of established connections per ip addr
    reject_connection_counts = orddict:new(),   %% number of connectios per ipaddr that need to be rejected
    rb_timeout_tref,                            %% rebalance timeout timer reference
    reference_rtq                               %% reference queue pid
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

sink_shutdown(Pid, Addr, Ref) ->
    gen_server:call(Pid, {sink_shutdown, Addr, Ref}, infinity).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init([Remote]) ->
    case whereis(riak_repl2_reference_rtq:name(Remote)) of
        undefined ->
            lager:error("undefined reference rtq for remote: ~p", [Remote]),
            {stop, {error, "undefined reference rtq"}};
        ReferenceQ ->
            State = #state{remote = Remote, reference_rtq = ReferenceQ},
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

handle_call(all_status, _From, State=#state{connections_monitors = ConnectionsMonitors}) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    {reply, lists:flatten(collect_status_data(ConnectionsMonitors, Timeout)), State};

handle_call(get_rtsource_conn_pids, _From, State = #state{connections_monitors = ConnectionMonitors}) ->
    Result = orddict:fold(fun(_, {Pid, _}, Acc) -> [Pid | Acc] end, [], ConnectionMonitors),
    {reply, Result, State};

handle_call({sink_shutdown, Addr, Ref}, {Pid,_Tag}, State) ->
    NewDict =
        case orddict:find(Addr, Dict) of
            error ->
                orddict:store(Addr, 1, Dict);
            {ok, Count} ->
                orddict:store(Addr, Count +1, Dict)
        end,
    {reply, ok, State#state{sink_shutting_down = NewDict}};

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

handle_cast(maybe_rebalance, State=#state{rb_timeout_tref = undefined}) ->
    MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 60),
    TimeDelay =  10 + round(MaxDelaySecs * crypto:rand_uniform(0, 1000)),
    RbTimeoutTref = erlang:send_after(TimeDelay, self(), rebalance_now),
    {noreply, State#state{rb_timeout_tref = RbTimeoutTref}};
handle_cast(maybe_rebalance, State) ->
    {noreply, State};

handle_cast(Request, State) ->
    lager:warning("unhandled cast: ~p", [Request]),
    {noreply, State}.

%%%=====================================================================================================================
handle_info({'DOWN', MonitorRef, process, Pid, _Reason}, State) ->
    #state
    {
        connections_monitors = ConnectionMonitors,
        connections = Connections,
        connection_counts = ConnectionCounts
    } = State,
    NewState =
        case orddict:find(MonitorRef, ConnectionMonitors) of
            error ->
                lager:error("could not find monitor ref: ~p in ConnectionMonitors (handle_info 'DOWN')", [MonitorRef]),
                State;
            {ok, {Pid, Addr}} ->

                NewConnectionMonitors = orddict:erase(MonitorRef, ConnectionMonitors),
                State1 = State#state{connections_monitors = NewConnectionMonitors},

                case orddict:find(Addr, Connections) of
                    error ->
                        %% this would be due to a rebalance!
                        lager:error("could not find Addr: ~p in Connections (handle_info 'DOWN')", Addr),
                        State1;
                    {ok, Addresses} ->

                        NewAddresses = orddict:erase(MonitorRef, Addresses),
                        NewConnections = orddict:store(Addr, NewAddresses, Connections),
                        NewConnectionCounts =
                            case orddict:find(Addr, ConnectionCounts) of
                                error ->
                                    lager:error("could not find count in ConnectionCounts for addr: ~p", [Addr]),
                                    ConnectionCounts;
                                {ok, Count} ->
                                    orddict:store(Addr, Count -1, ConnectionCounts)
                            end,

                        %% here we should issue a rebalance!
                        %% a connection died, and we did not expect it
                        maybe_rebalance(self()),
                        State1#state{connections = NewConnections, connection_counts = NewConnectionCounts}
                end;
            {ok, {Pid2, Addr}} ->
                lager:error("found monitor ref: ~p with different pid:~p, (orginal pid: ~p) "
                            "associated to it for addr: ~p (handle_info 'DOWN')", [MonitorRef, Pid2, Pid, Addr]),
                NewConnectionMonitors = orddict:erase(MonitorRef, ConnectionMonitors),
                State#state{connections_monitors = NewConnectionMonitors}
        end,
    {noreply, NewState};

handle_info(rebalance_now, State) ->
    {noreply, rebalance_connections(State#state{rb_timeout_tref = undefined})};

handle_info(Info, State) ->
    lager:warning("unhandled info: ~p", [Info]),
    {noreply, State}.

%%%=====================================================================================================================

terminate(Reason, _State=#state{remote = Remote, connections_monitors = ConnectionMonitors}) ->
    lager:info("rtrsource conn mgr terminating, Reason: ~p", [Reason]),
    riak_core_connection_mgr:disconnect({rt_repl, Remote}),
    orddict:fold(
        fun(_, {Pid, Ref}, _) ->
            erlang:demonitor(Ref),
            catch riak_repl2_rtsource_conn:stop(Pid),
            ok
        end, ok, ConnectionMonitors),
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
    #state
    {reject_connection_counts = RejectConnectionCounts} = State,
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
                    State1 = update_connections(State, {connected, Ref, RtSourcePid, IPPort}),
                    State2 = update_connections_monitors(State1, {connected, Ref, RtSourcePid, IPPort}),
                    State3 = update_connection_counts(State2, IPPort),
                    State4 = update_pending_connections(State3, IPPort),
                    {reply, ok, State4};
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
update_connections(State, {connected, Ref, Pid, Addr}) ->
    #state{connections = Connections} = State,
    NewConnections =
        case orddict:find(Addr, Connections) of
            error ->
                NewAddrDict = orddict:from_list([{Ref, Pid}]),
                orddict:store(Addr, NewAddrDict, Connections);
            {ok, Dict} ->
                NewAddrDict = orddict:store(Ref, Pid, Dict),
                orddict:store(Addr, NewAddrDict, Connections)
        end,
    State#state{connections = NewConnections}.

update_connections_monitors(State, {connected, Ref, Pid, Addr}) ->
    #state{connections_monitors = ConnectionMonitors} = State,
    NewConnectionMonitors = orddict:store(Ref, {Pid, Addr}, ConnectionMonitors),
    State#state{connections_monitors = NewConnectionMonitors}.

update_pending_connections(State, Addr) ->
    #state{pending_connection_counts = PendingConnections} = State,
    NewPendingConnections =
        case orddict:find(Addr, PendingConnections) of
            error ->
                lager:error("we have a new connection that is not in pending connections (accept connection)"),
                PendingConnections;
            {ok, N} ->
                orddict:store(Addr, N-1, PendingConnections)
        end,
    State#state{pending_connection_counts = NewPendingConnections}.

update_connection_counts(State, Addr) ->
    #state{connection_counts = ConnectionCoutns} = State,
    case orddict:find(Addr, ConnectionCoutns) of
        error ->
            State#state{connection_counts = orddict:store(Addr, 1, ConnectionCoutns)};
        {ok, Count} ->
            State#state{connection_counts = orddict:store(Addr, Count +1, ConnectionCoutns)}
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
        pending_connection_counts = PendingConnectionCounts
    } = State,
    case calculate_balanced_connections(State) of
        [] ->
            lager:warning("No available sink nodes to connect too (rebalance connections)"),
            maybe_rebalance(self()),
            State;
        BalancedConnections ->

            %% calculate total connections conencted/pending
            MergedConnectionCounts =
                orddict:merge(fun(_, V1, V2) -> V1 + V2 end, ConnectionCounts, PendingConnectionCounts),

            %% Calculate connection to be establish/ terminate
            RebalanceConnectionCounts =
                orddict:merge(fun(_, V1, V2) -> V1 - V2 end, BalancedConnections, MergedConnectionCounts),

            %% rebalance the connections to match the rebalance connection counts
            orddict:fold(fun connect_or_terminate/3, State, RebalanceConnectionCounts)
    end.




connect_or_terminate(_Addr, 0, State) ->
    State;
connect_or_terminate(Addr, N, State) when N < 0 ->
    {NewState, NewN} = reject_pending_connections(Addr, N, State),
    terminate_sink_n_times(Addr, NewN, NewState);
connect_or_terminate(Addr, N, State) when N > 0 ->
    connect_to_sink_n_times(Addr, N, State).



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


reject_pending_connections(_Addr, 0, State) ->
    State;
reject_pending_connections(Addr, N, State) ->
    #state{
        pending_connection_counts = PendingConnectionCounts,
        reject_connection_counts = RejectConnectionCounts
    } = State,
    case orddict:find(Addr, PendingConnectionCounts) of
        error ->
            State;
        {ok, V} ->
            case V >= N of
                true ->
                    NewPendingConnectionCounts = orddict:store(Addr, V - N, PendingConnectionCounts),
                    NewRejectConnectionCounts = orddict:store(Addr, N, RejectConnectionCounts),
                    NewState =
                        State#state
                        {
                            pending_connection_counts = NewPendingConnectionCounts,
                            reject_connection_counts = NewRejectConnectionCounts
                        },
                    {NewState, 0};
                false ->
                    NewPendingConnectionCounts = orddict:store(Addr, 0, PendingConnectionCounts),
                    NewRejectConnectionCounts = orddict:store(Addr, N - V, RejectConnectionCounts),
                    NewState =
                        State#state
                        {
                            pending_connection_counts = NewPendingConnectionCounts,
                            reject_connection_counts = NewRejectConnectionCounts
                        },
                    {NewState, N - V}

            end
    end.

terminate_sink_n_times(_Addr, 0, State) ->
    State;
terminate_sink_n_times(Addr, N, State) ->
    #state{connections = Connections} = State,
    case orddict:find(Addr, Connections) of
        error ->
            State;
        {ok, Addresses} ->
            case length(Addresses) >= N of
                true ->
                    {Termiante, Rest} = lists:split(N, Addresses),
                    %% TODO: do we want this to change so its a graceful shutdown for the reference queue
                    %% TODO: as in we will wait for an ack if there is one incoming
                    lists:foreach(fun({_Ref, Pid}) -> exit(Pid, rebalance) end, Termiante),
                    NewConnections = orddict:store(Addr, Rest, Connections),
                    State#state{connections = NewConnections};
                false ->
                    lager:error("we have been asked to kill more connections than exist! (terminate_sink_n_times)"),
                    %% TODO: do we want this to change so its a graceful shutdown for the reference queue
                    %% TODO: as in we will wait for an ack if there is one incoming
                    lists:foreach(fun({_Ref, Pid}) -> exit(Pid, rebalance) end, Addresses),
                    NewConnections = orddict:store(Addr, [], Connections),
                    State#state{connections = NewConnections}
            end



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
        fun(_, {Pid, _}, Acc) ->
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
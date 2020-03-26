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
    status/1,
    get_rtsource_conn_pids/1
]).

-define(SERVER, ?MODULE).
-define(CLIENT_SPEC(Id), {{realtime,[{4,0}, {3,0}, {2,0}, {1,5}]}, {?TCP_OPTIONS, ?SERVER, ?CALLBACK_ARGS(Id)}}).
-define(CALLBACK_ARGS(Id), {self(), Id}).
-define(TCP_OPTIONS,  [{keepalive, true}, {nodelay, true}, {packet, 0}, {active, false}]).
-define(DEFAULT_RT_REBALANCE_DELAY, 10000).
-define(DEFAULT_RT_RETRY_BAD_SINKS, 120000).
-define(DEFAULT_NUMBER_OF_CONNECTIONS_PER_QUEUE, one_per_sink_node).

-record(state, {
    remote = undefined,                                 %% remote sink cluster name
    connections = orddict:new(),                        %% orddict of connections records
    connection_monitor_ids = orddict:new(),             %% orddict for monitorref -> Id
    number_of_connection = 0,
    number_of_pending_connects = 0,
    number_of_pending_disconnects = 0,
    balancing = true,
    balanced = false,
    sink_nodes = [],
    bad_sink_nodes = [],
    rb_timeout_tref,                                   %% rebalance timeout timer reference
    bs_timeout_tref,                                   %% bad sink timer to retry the bad sinks
    ipl_timeout_tref                                   %% empty ip list timer to retry the list of IPs
}).

-record(connections,
{
    connections_monitor_addrs = orddict:new(),         %% monitor references mapped to addr
    connections_monitor_pids = orddict:new(),          %% monitor references mapped to pid
    connection_counts = orddict:new(),                 %% number of established connections per ip addr,
    balanced_connection_counts = orddict:new(),        %% the balanced version of connection_counts (the ideal to hit)
    balanced = false
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RemoteName) ->
    gen_server:start_link(?MODULE, [RemoteName], []).

connected(Socket, Transport, IPPort, Proto, {RTSourceConnMgrPid, Id}, Props) ->
    Transport:controlling_process(Socket, RTSourceConnMgrPid),
    try
    gen_server:call(RTSourceConnMgrPid,
        {connected, Socket, Transport, IPPort, Proto, Props, Id})
    catch
        _:Reason ->
            lager:warning("Unable to contact RT Source Conn Manager (~p). Killing it to force a reconnect", RTSourceConnMgrPid),
            exit(RTSourceConnMgrPid, {unable_to_contact, Reason}),
            ok
    end.

connect_failed(_ClientProto, Reason, {RTSourceConnMgrPid, Id}, Addr) ->
    gen_server:cast(RTSourceConnMgrPid, {connect_failed, Reason, Id, Addr}).

maybe_rebalance(Pid) ->
    gen_server:cast(Pid, maybe_rebalance).

stop(Pid) ->
    gen_server:call(Pid, stop).

status(Pid) ->
    gen_server:call(Pid, status, ?LONG_TIMEOUT).

get_rtsource_conn_pids(Pid) ->
    gen_server:call(Pid, get_rtsource_conn_pids).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init([RemoteName]) ->
    Concurrency = riak_repl_util:get_rtq_concurrency(),
    Connections = lists:foldl(
                    fun(N, Acc) ->
                        orddict:store(N, #connections{}, Acc)
                    end, orddict:new(), lists:seq(1, Concurrency)),
    {ok, SinkNodes} = riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(RemoteName),
    State = #state{connections = Connections, remote = RemoteName, sink_nodes = SinkNodes},
    NewState = rebalance_connections(State),
    {ok, NewState}.

%%%=====================================================================================================================
handle_call({connected, Socket, Transport, Addr, Proto, Props, Id}, _From, State) ->
    accept_connection(Socket, Transport, Addr, Proto, Props, Id, State);

%% TODO: decide on the information we want here
handle_call(status, _From, State) ->
    Stats = get_status(State),
    {reply, Stats, State};

handle_call(get_rtsource_conn_pids, _From, State = #state{connections = Connections}) ->
    Result =
        orddict:fold(
            fun(_, #connections{connections_monitor_pids = Pids}, Acc1) ->
                orddict:fold(
                    fun(_Ref, Pid, Acc2) ->
                        [Pid | Acc2]
                    end, Acc1, Pids)
            end, [], Connections),
    {reply, Result, State};

handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(Request, _From, State) ->
    lager:warning("unhandled call: ~p", [Request]),
    {reply, ok, State}.

%%%=====================================================================================================================
handle_cast({connect_failed, Reason, Id, Addr}, State) ->
    #state{remote = Remote} = State,
    lager:warning("Realtime replication connection to site ~p; address: ~p; failed - ~p\n", [Remote, Addr, Reason]),
    {noreply, connection_failed(Id, Addr, State)};


handle_cast(maybe_rebalance, State) ->
    State1 = set_balanced_connections(State),
    State2 = set_status(State1),
    {noreply, start_rebalance_timer(State2)};

handle_cast(Request, State) ->
    lager:warning("unhandled cast: ~p", [Request]),
    {noreply, State}.

%%%=====================================================================================================================
%%                                         Rtsource Conn Pids DOWN
%%%=====================================================================================================================
handle_info({'DOWN', MonitorRef, process, _Pid, {shutdown, sink_shutdown}}, State) ->
    #state{bad_sink_nodes = BadSinks, connections = Connections, connection_monitor_ids = IdRefs} = State,
    try
        Id = orddict:fetch(MonitorRef, IdRefs),
        Connection = orddict:fetch(Id, Connections),
        #connections{connections_monitor_addrs = Addrs} = Connection,
        Addr = orddict:fetch(MonitorRef, Addrs),
        State1 = remove_connection_monitor(Id, MonitorRef, State),
        State2 = decrease_connection_count(Id, Addr, State1),
        State3 =
            case lists:member(Addr, BadSinks) of
                true ->
                    State2;
                false ->
                    set_balanced_connections(State2#state{bad_sink_nodes = [Addr|BadSinks]})
            end,
        State4 = set_status(State3),
        State5 = start_rebalance_timer(State4),
        {noreply, State5}
    catch
        Type:Error ->
            {stop, {error, Type, Error}, State}
    end;


handle_info({'DOWN', MonitorRef, process, _Pid, {shutdown, source_rebalance}}, State) ->
    #state{connections = Connections, connection_monitor_ids = IdRefs} = State,
    try
        Id = orddict:fetch(MonitorRef, IdRefs),
        Connection = orddict:fetch(Id, Connections),
        #connections{connections_monitor_addrs = Addrs} = Connection,
        Addr = orddict:fetch(MonitorRef, Addrs),
        State1 = remove_connection_monitor(Id, MonitorRef, State),
        State2 = decrease_connection_count(Id, Addr, State1),
        State3 = decrease_number_of_pending_disconnects(State2),
        State4 = set_status(State3),
        {noreply, State4}
    catch
        Type:Error ->
            {stop, {error, Type, Error}, State}
    end;

%% wrong_seq, random connection interrupt, protocol 3 node shutdown
%% above are the reasons for receiving these DOWN messages
%% in these circumstances, we shall issue a re-connect straight away, and start a rebalance timer as well
handle_info({'DOWN', MonitorRef, process, _Pid, _Reason}, State) ->
    #state{connections = Connections, connection_monitor_ids = IdRefs} = State,
    try
        Id = orddict:fetch(MonitorRef, IdRefs),
        Connection = orddict:fetch(Id, Connections),
        #connections{connections_monitor_addrs = Addrs} = Connection,
        Addr = orddict:fetch(MonitorRef, Addrs),
        State1 = remove_connection_monitor(Id, MonitorRef, State),
        State2 = decrease_connection_count(Id, Addr, State1),

        %% issue re-connect for the failed connection
        {State3, _} = connect_to_sink(Addr, Id, State2),
        State4 = set_status(State3),
        State5 = start_rebalance_timer(State4),
        {noreply, State5}
    catch
        Type:Error ->
            {stop, {error, Type, Error}, State}
    end;
%%%=====================================================================================================================


handle_info(rebalance_now, State) ->
    {noreply, maybe_do_rebalance(State#state{rb_timeout_tref = undefined})};

handle_info(try_bad_sink_nodes, State = #state{bad_sink_nodes = []}) ->
    {noreply, State#state{bs_timeout_tref = undefined}};
handle_info(try_bad_sink_nodes, State = #state{bad_sink_nodes = BadSinkNodes, remote = Remote}) ->
    lists:foreach(
        fun(Addr) ->
            _ = riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC(0), {use_only, [Addr]})
        end, BadSinkNodes),
    {noreply, State#state{bs_timeout_tref = undefined}};

handle_info(try_get_ip_list, State) ->
    State0 = State#state{ipl_timeout_tref = undefined},
    State1 = set_balanced_connections(State0),
    State2 = set_status(State1),
    {noreply, maybe_do_rebalance(State2)};


handle_info(Info, State) ->
    lager:warning("unhandled info: ~p", [Info]),
    {noreply, State}.

%%%=====================================================================================================================

terminate(Reason, _State=#state{remote = Remote, connections = Connections}) ->
    lager:info("RTSOURCE CONN MGR has died due to: ~p", [Reason]),
    riak_core_connection_mgr:disconnect({rt_repl, Remote}),
    orddict:fold(
        fun(_, #connections{connections_monitor_pids = Pids}, _) ->
            orddict:fold(
                fun(Ref,Pid,_) ->
                    erlang:demonitor(Ref),
                    catch riak_repl2_rtsource_conn:stop(Pid),
                    ok
                end, ok, Pids)
        end, ok, Connections),
    ok.

%%%=====================================================================================================================

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%%%====================================================================================================================


%%%===================================================================
%%% Start Timers (DONE)
%%%===================================================================

%% ensure that the iplist timeout ref is undefined as well
%% if it is not undefined, it means we have no ip's in the list and are in a loop to get a non-empty list
start_rebalance_timer(State = #state{ipl_timeout_tref = undefined, rb_timeout_tref = undefined}) ->
    TimeDelay = get_rebalance_delay(),
    RbTimeoutTref = erlang:send_after(TimeDelay, self(), rebalance_now),
    State#state{rb_timeout_tref = RbTimeoutTref};
start_rebalance_timer(State) ->
    State.


start_bad_sink_timer(State = #state{bad_sink_nodes = []}) ->
    State;
start_bad_sink_timer(State = #state{bs_timeout_tref = undefined}) ->
    TimeDelay = get_retry_bad_sinks_delay(),
    BsTimeoutTref = erlang:send_after(TimeDelay, self(), try_bad_sink_nodes),
    State#state{bs_timeout_tref = BsTimeoutTref};
start_bad_sink_timer(State) ->
    State.

start_empty_ip_list_timer(State = #state{ipl_timeout_tref = undefined}) ->
    IPLTimeoutTref = erlang:send_after(5000, self(), try_get_ip_list),
    State#state{ipl_timeout_tref = IPLTimeoutTref};
start_empty_ip_list_timer(State) ->
    State.

%%%===================================================================
%% Connection Failed
%%%===================================================================
connection_failed(0, _Addr, State) ->
    start_bad_sink_timer(State);
connection_failed(_Id, _Addr, State) ->
    State1 = decrease_number_of_pending_connects(State),
    State2 = set_status(State1),
    State3 = maybe_set_bad_sink_nodes(State2),
    start_rebalance_timer(State3).

%%%===================================================================
%% Accept Connection
%%%===================================================================
accept_connection(Socket, Transport, Addr, _Proto, _Props, 0, State = #state{bad_sink_nodes = BadSinkNodes}) ->
    catch Transport:close(Socket),
    NewBadSinkNodes = lists:delete(Addr, BadSinkNodes),
    State1 = State#state{bad_sink_nodes = NewBadSinkNodes},
    State2 = set_balanced_connections(State1),
    State3 = set_status(State2),
    start_rebalance_timer(State3);
accept_connection(Socket, Transport, IPPort, Proto, Props, Id, State) ->
    #state{remote = Remote} = State,
    case riak_repl2_rtsource_conn:start(Remote, Id, self()) of
        {ok, RtSourcePid} ->
            Ref = erlang:monitor(process, RtSourcePid),
            case riak_repl2_rtsource_conn:connected(RtSourcePid, Ref, Socket, Transport, IPPort, Proto, Props) of
                ok ->
                    State1 = update_state_for_new_connection(Ref, RtSourcePid, IPPort, Id, State),
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

update_state_for_new_connection(Ref, RtSourcePid, Addr, Id, State) ->
    State1 = add_connection_monitor(Id, Ref, RtSourcePid, Addr, State),
    State2 = increase_connection_count(Id, Addr, State1),
    State3 = decrease_number_of_pending_connects(State2),
    State4 = set_status(State3),
    maybe_set_bad_sink_nodes(State4).



%%%===================================================================
%% Maybe Do The Rebalance
%%%===================================================================
maybe_do_rebalance(State = #state{balancing = true, balanced = false}) ->
    start_rebalance_timer(State);

maybe_do_rebalance(State = #state{ipl_timeout_tref = undefined, balanced = false}) ->
    rebalance_connections(State);

maybe_do_rebalance(State) ->
    State.

%%%===================================================================
%% Rebalance Connections
%%%===================================================================
rebalance_connections(State) ->
    case should_add_connections(State) of
        {true, Add} ->
            State1 = start_rebalance_timer(State),
            add_connections(Add, State1);
        false ->
            case should_remove_connections(State) of
                {true, Remove} ->
                    remove_connections(Remove, State);
                false ->
                    %% If we we have this function we shouldn't get into this state
                    %% as we perform check to see if we require the rebalance before we hit here
                    set_status(State)
            end
    end.

%%%===================================================================
%% Calculate Connections To Add (DONE)
%%%===================================================================
should_add_connections(State) ->
    #state{connections = Connections} = State,
    ConnectionsToAdd =
        orddict:fold(fun should_add_connections_helper/3, orddict:new(), Connections),
    case ConnectionsToAdd of
        [] ->
            false;
        _ ->
            {true, ConnectionsToAdd}
    end.

should_add_connections_helper(Id, Connections, Dict) ->
    #connections
    {
        connection_counts = ConnectionCounts,
        balanced_connection_counts = BalancedConnectionCounts
    } = Connections,
    case BalancedConnectionCounts of
        [] ->
            Dict;
        BalancedConnections ->
            RebalanceConnectionCounts =
                orddict:merge(fun(_, V1, V2) -> V1 - V2 end, BalancedConnections, ConnectionCounts),

            Add =
                orddict:fold(
                    fun(Addr, Count, Acc) ->
                            case Count > 0 of
                                true ->
                                    orddict:store(Addr, Count, Acc);
                                false ->
                                    Acc
                            end
                    end, orddict:new(), RebalanceConnectionCounts),

            case Add of
                [] ->
                    Dict;
                _ ->
                    orddict:store(Id, Add, Dict)
            end
    end.

%%%===================================================================
%% Calculate Connections To Remove (DONE)
%%%===================================================================
should_remove_connections(State) ->
    #state{connections = Connections} = State,
    ConnectionsToRemove =
        orddict:fold(fun should_remove_connections_helper/3, orddict:new(), Connections),
    case ConnectionsToRemove of
        [] ->
            false;
        _ ->
            {true, ConnectionsToRemove}
    end.

should_remove_connections_helper(Id, Connections, Dict) ->
    #connections
    {
        connection_counts = ConnectionCounts,
        balanced_connection_counts = BalancedConnectionCounts
    } = Connections,
    case BalancedConnectionCounts of
        [] ->
            Dict;
        BalancedConnections ->

            %% calculate number of connections to be established (or terminated if negative)
            RebalanceConnectionCounts =
                orddict:merge(fun(_, V1, V2) -> V1 - V2 end, BalancedConnections, ConnectionCounts),

            Remove =
                orddict:fold(
                    fun(Addr, Count, Acc) ->
                        case Count < 0 of
                            true ->
                                orddict:store(Addr, Count, Acc);
                            false ->
                               Acc
                        end
                    end, orddict:new(), RebalanceConnectionCounts),
            case Remove of
                [] ->
                    Dict;
                _ ->
                    orddict:store(Id, Remove, Dict)
            end
    end.



%%%===================================================================
%% Remove Connections Gracefully (DONE)
%%%===================================================================
%% Dict of Dicts
remove_connections(RemoveDicts, State) ->
    State1 = orddict:fold(fun remove_connections_helper/3, State, RemoveDicts),
    State2 = set_balanced_status(State1),
    set_balancing_status(State2).

remove_connections_helper(Id, RemoveDict, State = #state{connections = Connections}) ->
    IdConnections = orddict:fetch(Id, Connections),
    #connections{connections_monitor_addrs = Addrs} = IdConnections,
    {State1, _, _} = orddict:fold(fun do_remove_connection/3, {State, IdConnections, RemoveDict}, Addrs),
    State1.

do_remove_connection(_Ref, _Addr, {State, IdConnections, []}) ->
    {State, IdConnections, []};
do_remove_connection(Ref, Addr, {State, IdConnections, RemoveDict}) ->
    #connections{connections_monitor_pids = Pids} = IdConnections,
    case orddict:find(Addr, RemoveDict) of
        error ->
            {State, IdConnections, RemoveDict};
        {ok, Count} ->
            Pid = orddict:fetch(Ref, Pids),
            riak_repl2_rtsource_conn:graceful_shutdown(Pid, source_rebalance),
            State1 = increase_number_of_pending_disconnects(State),
            case Count +1 of
                0 ->
                    NewRemoveDict = orddict:erase(Addr, RemoveDict),
                    {State1, IdConnections, NewRemoveDict};
                NewCount ->
                    NewRemoveDict = orddict:store(Addr, NewCount, RemoveDict),
                    {State1, IdConnections, NewRemoveDict}
            end

    end.



%%%===================================================================
%% Add Connections (DONE)
%%%===================================================================
%% Dict of Dicts
add_connections(AddDicts, State) ->
    State1 = orddict:fold(fun do_add_connections/3, State, AddDicts),
    set_status(State1).

do_add_connections(Id, AddConnectionDict, State) ->
    {State1, _} = orddict:fold(fun add_sink_conns/3, {State, Id}, AddConnectionDict),
    State1.

add_sink_conns(Addr, N, {State, Id}) ->
    connect_to_sink_n_times(Addr, N, {State, Id}).

%% Add New Connections
connect_to_sink_n_times(_Addr, 0, {State, Id}) ->
    {State, Id};
connect_to_sink_n_times(Addr, N, {State, Id}) ->
    connect_to_sink_n_times(Addr, N-1, connect_to_sink(Addr, Id, State)).

connect_to_sink(Addr, Id, State) ->
    #state{remote = Remote} = State,
    case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC(Id), {use_only, [Addr]}) of
        {ok, _Ref} ->
            {increase_number_of_pending_connects(State), Id};
        _->
            {State, Id}
    end.


%%%=====================================================================================================================
%%% Environment Variables
%%%=====================================================================================================================

%%%===================================================================
%% Get Rebalance Delay
%%%===================================================================
get_rebalance_delay() ->
    case app_helper:get_env(riak_repl, rt_rebalance_delay) of
        N when is_integer(N) and N > 0 -> N * 1000;
        _ -> ?DEFAULT_RT_REBALANCE_DELAY
    end.

%%%===================================================================
%% Get Retry Bad Sink Delay
%%%===================================================================
get_retry_bad_sinks_delay() ->
    case app_helper:get_env(riak_repl, rt_retry_bad_sinks) of
        N when is_integer(N) and N > 0 -> N * 1000;
        _ -> ?DEFAULT_RT_RETRY_BAD_SINKS
    end.

%%%===================================================================
%% Get the number of connections for our remote (DONE)
%%%===================================================================
-ifdef(TEST).

get_number_of_connections_per_queue(_) ->
    app_helper:get_env(riak_repl, default_number_of_connections_per_queue, one_per_sink_node).

-else.

get_number_of_connections_per_queue(Name) ->
    case get_number_of_connections_per_queue_for_remote(Name) of
        undefined -> get_number_of_connections_per_queue_default();
        N -> N
    end.

get_number_of_connections_per_queue_default() ->
    case app_helper:get_env(riak_repl, default_number_of_connections_per_queue) of
        N when is_integer(N) and N > 0 -> N;
        _ -> ?DEFAULT_NUMBER_OF_CONNECTIONS_PER_QUEUE
    end.

get_number_of_connections_per_queue_for_remote(Name) ->
    case riak_core_metadata:get(?RIAK_REPL2_CONFIG_KEY, {number_of_connections_per_queue, Name}) of
        N when is_integer(N) and N > 0 -> N;
        _ -> undefined
    end.

-endif.
%%%===================================================================================================================%%
%%                                              Update State                                                          %%
%%%===================================================================================================================%%

%%%===================================================================
%% Update Bad Sink Nodes (DONE)
%%%===================================================================
%% This is used to check if we have any connections to a sink node after we have completed a rebalance
%% If we do not, we determine that we are unable to connect to the node and set it as a 'bad' node.
%% This allows us to rebalance to the remaining 'good' nodes, while we periodically attempt to reconnect to the 'bad'
%% nodes.
maybe_set_bad_sink_nodes(State = #state{balancing = true}) ->
    State;
maybe_set_bad_sink_nodes(State = #state{connections = Connections}) ->
    TotalConnectionCounts =
        orddict:fold(
            fun(_Id, #connections{connection_counts = D1}, D2) ->
                orddict:merge(fun(_, V1, V2) -> V1 + V2 end, D1, D2)
            end, orddict:new(), Connections),
    set_bad_sink_nodes_helper(State, TotalConnectionCounts).


%% when we are no longer balancing (all connections have made it)
%% check to see if we have any sink nodes with 0 connections and determine them as a 'bad sink node'
set_bad_sink_nodes_helper(State = #state{sink_nodes = SinkNodes}, TotalConnectionCounts) ->
    AliveNodes = [DN || {DN,X} <- TotalConnectionCounts, X > 0],
    DeadNodes = SinkNodes -- AliveNodes,
    case DeadNodes of
        [] ->
            State;
        _ ->
            State1 = State#state{bad_sink_nodes = DeadNodes},
            State2 = set_balanced_connections(State1),
            State3 = set_status(State2),
            start_bad_sink_timer(State3)
    end.

%%%===================================================================
%% Add Connection Monitor (DONE)
%%%===================================================================
add_connection_monitor(Id, Ref, Pid, Addr, State) ->
    #state{connection_monitor_ids = IdRefs, connections = Connections} = State,
    NewIdRefs = orddict:store(Ref, Id, IdRefs),
    Connection = orddict:fetch(Id, Connections),
    #connections{connections_monitor_addrs = Addrs, connections_monitor_pids = Pids} = Connection,
    NewConnection =
        Connection#connections
        {
            connections_monitor_addrs = orddict:store(Ref, Addr, Addrs),
            connections_monitor_pids = orddict:store(Ref, Pid, Pids)
        },
    NewConnections = orddict:store(Id, NewConnection, Connections),
    State#state{connection_monitor_ids = NewIdRefs, connections = NewConnections}.

%%%===================================================================
%% Remove Connection Monitor (DONE)
%%%===================================================================
remove_connection_monitor(Id, Ref, State) ->
    #state{connection_monitor_ids = IdRefs, connections = Connections} = State,
    NewIdRefs = orddict:erase(Ref,IdRefs),
    Connection = orddict:fetch(Id, Connections),
    #connections{connections_monitor_addrs = Addrs, connections_monitor_pids = Pids} = Connection,
    NewConnection =
        Connection#connections
        {
            connections_monitor_addrs = orddict:erase(Ref, Addrs),
            connections_monitor_pids = orddict:erase(Ref, Pids)
        },
    NewConnections = orddict:store(Id, NewConnection, Connections),
    State#state{connection_monitor_ids = NewIdRefs, connections = NewConnections}.


%%===================================================================
%% Increase The Connection Count For An Addresss (DONE)
%%%===================================================================
%% increases the connection count for a given address
increase_connection_count(Id, Addr, State) ->
    #state{connections= Connections, number_of_connection = N} = State,
    IdConnection = orddict:fetch(Id, Connections),
    #connections{connection_counts = ConnectionCounts} = IdConnection,
    NewIdConnection = IdConnection#connections{connection_counts = orddict:update_counter(Addr, 1, ConnectionCounts)},
    NewConnections = orddict:store(Id, NewIdConnection, Connections),
    State#state{connections = NewConnections, number_of_connection = N +1}.

%%===================================================================
%% Decrease The Connection Count For An Addresss (DONE)
%%%===================================================================
%% decreases the connection count for a given address
decrease_connection_count(Id, Addr, State) ->
    #state{connections= Connections, number_of_connection = N} = State,
    IdConnection = orddict:fetch(Id, Connections),
    #connections{connection_counts = ConnectionCounts} = IdConnection,
    NewIdConnection =
        case orddict:find(Addr, ConnectionCounts) of
            error ->
                lager:error("couldn't find connection count for Id: ~p, Addr: ~p", [Id, Addr]),
                IdConnection;
            {ok, C} ->
                case C -1 of
                    0 ->
                        IdConnection#connections{connection_counts = orddict:erase(Addr, ConnectionCounts)};
                    _ ->
                        IdConnection#connections{connection_counts = orddict:update_counter(Addr, -1, ConnectionCounts)}

                end
        end,
    NewConnections = orddict:store(Id, NewIdConnection, Connections),
    State#state{connections = NewConnections, number_of_connection = N -1}.


%%%===================================================================
%% Increase Number Of Pending Disconnects (DONE)
%%%===================================================================
%% This function increases the number of pending disconnects
increase_number_of_pending_disconnects(State = #state{number_of_pending_disconnects = N}) ->
    State#state{number_of_pending_disconnects = N +1}.

%%%===================================================================
%% Decrease Number Of Pending Disconnects (DONE)
%%%===================================================================
%% This function decreases the number of pending disconnects
decrease_number_of_pending_disconnects(State = #state{number_of_pending_disconnects = N}) ->
    State#state{number_of_pending_disconnects = N -1}.


%%%===================================================================
%% Increase Number Of Pending Connections (DONE)
%%%===================================================================
%% This function increases the number of pending connects
increase_number_of_pending_connects(State = #state{number_of_pending_connects = N}) ->
    State#state{number_of_pending_connects = N +1}.

%%%===================================================================
%% Decrease Number Of Pending Connections (DONE)
%%%===================================================================
%% This function decreases the number of pending connects
decrease_number_of_pending_connects(State = #state{number_of_pending_connects = N}) ->
    State#state{number_of_pending_connects = N -1}.



%%%===================================================================
%% Set Status's (DONE)
%%%===================================================================
set_status(State) ->
    State1 = set_balanced_status(State),
    set_balancing_status(State1).

%%%===================================================================
%% Set Balanced Status (DONE)
%%%===================================================================
%% This function checks if the number of connections we have is the same as the number of connections we should have
%% (that is determined by the balanced_connections_counts saved in state)
%% We save that to state, to allow us to skip rebalancing in scenario's when its not needed
set_balanced_status(State = #state{connections = Connections}) ->
    Result =
        orddict:fold(
            fun(_, #connections{connection_counts = A, balanced_connection_counts = B}, Acc) ->
                (A == B) and Acc
            end, true, Connections),
    State#state{balanced = Result}.

%%%===================================================================
%% Set Balancing Status (DONE)
%%%===================================================================
%% This function takes the values of the pending number of disconnects/ connects, if they are both not 0, we set this
%% to true
set_balancing_status(State = #state{number_of_pending_connects = 0, number_of_pending_disconnects = 0}) ->
    State#state{balancing = false};
set_balancing_status(State) ->
    State#state{balancing = true}.

%%%===================================================================
%% Set Balanced Connections (DONE)
%%%===================================================================
%% This grabs the list of sink nodes from the cluster_mgr (in a specific order for our own node)
%% We then use this list in combination with the config for the number of connections to have for this remote
%% to build and list of {Addr, Count} which tells us how many times to connect to a given node
set_balanced_connections(State = #state{connections = Connections, bad_sink_nodes = BadSinkNodes, remote = Remote}) ->
    case riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(Remote) of
        {ok, []} ->
            start_empty_ip_list_timer(State);
        {ok, SinkNodes0} ->
            BadSinkNodesDown = BadSinkNodes -- SinkNodes0,
            NewBadSinkNodes = BadSinkNodes -- BadSinkNodesDown,
            SinkNodes = SinkNodes0 -- NewBadSinkNodes,
            #state{remote = Remote} = State,
            ConnectionsPerQ =
                case get_number_of_connections_per_queue(Remote) of
                    one_per_sink_node ->
                        length(SinkNodes);
                    N ->
                        N
                end,
            State1 = State#state{sink_nodes = SinkNodes, bad_sink_nodes = NewBadSinkNodes},
            {NewConnections, _, _} =
                orddict:fold(
                    fun set_balanced_connections_helper/3, {orddict:new(), ConnectionsPerQ, SinkNodes}, Connections),
            State1#state{connections = NewConnections}
    end.

set_balanced_connections_helper(Id, Connection, {Acc, ConnectionsPerQ, SinkNodes}) ->
    NumberOfSinkNodes = length(SinkNodes),
    TotalExtraConnections = ConnectionsPerQ rem NumberOfSinkNodes,
    AverageNumberOfConnections = (ConnectionsPerQ - TotalExtraConnections) div NumberOfSinkNodes,
    {BalancedConnectionCounts, _} =
        lists:foldl(
            fun(Addr, {Acc1, Counter}) ->
                case Counter =< TotalExtraConnections of
                    true ->
                        {orddict:store(Addr, AverageNumberOfConnections +1, Acc1), Counter +1};
                    false ->
                        {orddict:store(Addr, AverageNumberOfConnections, Acc1), Counter +1}
                end
            end, {orddict:new(), 1}, SinkNodes),
    [H|T] = SinkNodes,
    NewAcc = orddict:store(Id, Connection#connections{balanced_connection_counts = BalancedConnectionCounts}, Acc),
    NewSinkNodes = T ++ [H],
    {NewAcc, ConnectionsPerQ, NewSinkNodes}.


%%%===================================================================
%% Status
%%%===================================================================
%% Old Status
%%    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
%%    orddict:fold(
%%        fun(_, Pid, Acc) ->
%%            case riak_repl2_rtsource_conn:status(Pid, Timeout) of
%%                [] -> Acc;
%%                Status -> [Status | Acc]
%%            end
%%        end, [], ConnectionMonitors),


get_status(State) ->
    #state
    {
        remote = Remote,
        balanced = Balanced,
        balancing = Balancing,
        number_of_connection = NumberOfConnections,
        number_of_pending_connects = NumberOfPendingConnects,
        number_of_pending_disconnects = NumerberOfPendingDisconnects
    } = State,

    Stats =
        [
            {remote, Remote},
            {balanced, Balanced},
            {balancing, Balancing},
            {number_of_connections, NumberOfConnections},
            {number_of_pending_connects, NumberOfPendingConnects},
            {number_of_pending_disconnects, NumerberOfPendingDisconnects},
            {connections, get_connection_counts(State)},
            {latency, get_latency(State)}
        ],
    Stats.


get_connection_counts(#state{connections = Connections}) ->
    Cumlative =
        orddict:fold(
            fun(_Id, #connections{connection_counts = C}, Acc) ->
                orddict:merge(fun(_, V1, V2) -> V1 + V2 end, C, Acc)
            end, orddict:new(), Connections),
    orddict:fold(
        fun({Ip, Port}, Count, Acc) ->
            FormatedAddr = riak_repl_util:format_ip_and_port(Ip, Port),
            orddict:store(FormatedAddr, Count , Acc)
        end, orddict:new(), Cumlative).

get_latency(State = #state{connections = Connections}) ->
    %% Latency information
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    LatencyPerSink =
        orddict:fold(
            fun(_, #connections{connections_monitor_pids = Pids}, Acc) ->
                orddict:fold(
                    fun(_Ref, Pid, Acc2) ->
                        case riak_repl2_rtsource_conn:get_latency(Pid, Timeout) of
                            error -> Acc2;
                            {Addr, LatencyDistribtion} -> merge_latency(Addr, LatencyDistribtion, Acc2)
                        end
                    end, Acc, Pids)
            end, orddict:new(), Connections),
    generate_latency_from_distribtuion(LatencyPerSink, State).

merge_latency(Addr, LatencyDistribtion, Acc) ->
    case orddict:find(Addr, Acc) of
        error ->
            orddict:store(Addr, LatencyDistribtion, Acc);
        {ok, OGLatencyDistribtion} ->
            Latency3 = merge_latency(OGLatencyDistribtion, LatencyDistribtion),
            orddict:store(Addr, Latency3, Acc)
    end.

merge_latency(L1, L2) ->
    #distribution_collector
    {number_data_points = N1, aggregate_values = AV1, aggregate_values_sqrd = AVS1, max = Max1} = L1,
    #distribution_collector
    {number_data_points = N2, aggregate_values = AV2, aggregate_values_sqrd = AVS2, max = Max2} = L2,
    Max =  case Max1 >= Max2 of
               true ->
                   Max1;
               false ->
                   Max2
           end,
    #distribution_collector
    {number_data_points = N1 + N2, aggregate_values = AV1 + AV2, aggregate_values_sqrd = AVS1 + AVS2, max = Max}.


generate_latency_from_distribtuion(LatencyPerSink, #state{remote = Remote}) ->
    RemoteLatencyDistribution =
        orddict:fold(
            fun(_, LatencyDistribution, Acc) ->
                merge_latency(Acc, LatencyDistribution)
            end, #distribution_collector{}, LatencyPerSink),
    AllLatencyDistributions = orddict:store(Remote, RemoteLatencyDistribution, LatencyPerSink),
    orddict:map(fun(_Key, Distribution) -> calculate_latency(Distribution) end, AllLatencyDistributions).

calculate_latency(#distribution_collector{number_data_points = 0}) ->
    [{mean, 0}, {percentile_95, 0}, {percentile_99, 0}, {percentile_100, 0}];
calculate_latency(Dist) ->
    #distribution_collector
    {number_data_points = N, aggregate_values = AV, aggregate_values_sqrd = AVS, max = Max} = Dist,
    Mean = AV / N,
    Var = (AVS + (N*Mean*Mean) - (2*Mean*AV)) / N,
    Std = math:sqrt(Var),
    P95 = Mean + 1.645*Std,
    P99 = Mean + 2.326*Std,
    [{mean, round(Mean)}, {percentile_95, round(P95)}, {percentile_99, round(P99)}, {percentile_100, round(Max)}].
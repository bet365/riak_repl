-module(riak_repl2_rtsource_conn_mgr).
-behaviour(gen_server).
-include("riak_repl.hrl").

%% API
-export([start_link/2]).

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
-define(CLIENT_SPEC(Id), {{realtime,[{4,0}, {3,0}, {2,0}, {1,5}]}, {?TCP_OPTIONS, ?SERVER, ?CALLBACK_ARGS(Id)}}).
-define(CALLBACK_ARGS(Id), {self(), Id}).
-define(TCP_OPTIONS,  [{keepalive, true}, {nodelay, true}, {packet, 0}, {active, false}]).

-record(state, {
    remote = undefined,                                 %% remote sink cluster name
    connections = orddict:new(),                        %% orddict of connections records
    connection_monitor_ids = orddict:new(),             %% orddict for monitorref -> Id
    balanced_connection_counts = orddict:new(),         %% the balanced version of connection_counts (the ideal to hit)
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
    connection_failed_counts = orddict:new(),          %% for stats
    balanced = false
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RemoteName, Id) ->
    gen_server:start_link(?MODULE, [RemoteName, Id], []).

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


init([RemoteName]) ->
    Concurrency = app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)),
    Connections = lists:foldl(
                    fun(N, Acc) ->
                        orddict:store(N, #connections{}, Acc)
                    end, orddict:new(), lists:seq(1, Concurrency)),
    {ok, SinkNodes} = riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(RemoteName),
    State = #state{connections = Connections, remote = RemoteName, sink_nodes = SinkNodes},
    NewState = rebalance_connections(State),
    {ok, NewState}.

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


handle_cast(maybe_rebalance, State) ->
    State1 = set_balanced_connections(State),
    State2 = set_status(State1),
    {noreply, start_rebalance_timer(State2)};

handle_cast(Request, State) ->
    lager:warning("unhandled cast: ~p", [Request]),
    {noreply, State}.

%%%=====================================================================================================================
handle_info({'DOWN', MonitorRef, process, _Pid, {shutdown, sink_shutdown}}, State) ->
    #state{bad_sink_nodes = BadSinks, connections_monitor_addrs = Addrs} = State,
    case orddict:find(MonitorRef, Addrs) of
        {ok, {Addr, _Id}} ->
            State1 = remove_connection_monitor(MonitorRef, State),
            State2 = decrease_connection_count(Addr, State1),
            State3 =
                case lists:member(Addr, BadSinks) of
                    true ->
                        State2;
                    false ->
                        set_balanced_connections(State2#state{bad_sink_nodes = [Addr|BadSinks]})
                end,
            State4 = set_status(State3),
            State5 = start_rebalance_timer(State4),
            {noreply, State5};
        error ->
            lager:error("could not find monitor ref in connection monitors addrs"),
            {noreply, State}
    end;


handle_info({'DOWN', MonitorRef, process, _Pid, {shutdown, source_rebalance}}, State) ->
    #state{connections_monitor_addrs = Addrs} = State,
    case orddict:find(MonitorRef, Addrs) of
        {ok, {Addr, _Id}} ->
            State1 = remove_connection_monitor(MonitorRef, State),
            State2 = decrease_connection_count(Addr, State1),
            State3 = decrease_number_of_pending_disconnects(State2),
            State4 = set_status(State3),
            {noreply, State4};
        error ->
            lager:error("could not find monitor ref in connection monitors addrs"),
            {noreply, State}
    end;

%% TODO: shutdown for wrong_seq etc ...
%% while we operate with cluster on protocol 3, we will hit here on a sink node shutdown!
%% do we want to issue a reconnect to the node going down?
handle_info({'DOWN', MonitorRef, process, _Pid, _Reason}, State) ->
    #state{connections_monitor_addrs = Addrs} = State,
    case orddict:find(MonitorRef, Addrs) of
        {ok, {Addr, _Id}} ->
            State1 = remove_connection_monitor(MonitorRef, State),
            State2 = decrease_connection_count(Addr, State1),
            State3 = set_status(State2),
            State4 = start_rebalance_timer(State3),
            {noreply, State4};
        error ->
            lager:error("could not find monitor ref in connection monitors addrs"),
            {noreply, State}
    end;

handle_info(rebalance_now, State) ->
    {noreply, maybe_do_rebalance(State#state{rb_timeout_tref = undefined})};

handle_info(try_bad_sink_nodes, State = #state{bad_sink_nodes = []}) ->
    {noreply, State#state{bs_timeout_tref = undefined}};
handle_info(try_bad_sink_nodes, State = #state{bad_sink_nodes = BadSinkNodes, remote = Remote}) ->
    lists:foreach(
        fun(Addr) ->
            _ = riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, [Addr]})
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
%% Get All Status's From rtsource_conn
%%%===================================================================
collect_status_data(ConnectionMonitors, Timeout) ->
    orddict:fold(
        fun(_, Pid, Acc) ->
            case riak_repl2_rtsource_conn:status(Pid, Timeout) of
                [] -> Acc;
                Status -> [Status | Acc]
            end
        end, [], ConnectionMonitors).

%%%===================================================================
%%% Start Timers
%%%===================================================================

%% ensure that the iplist timeout ref is undefined as well
%% if it is not undefined, it means we have no ip's in the list and are in a loop to get a non-empty list
start_rebalance_timer(State = #state{ipl_timeout_tref = undefined, rb_timeout_tref = undefined}) ->
    MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 60),
    TimeDelay =  10000 + round(MaxDelaySecs * crypto:rand_uniform(0, 1000)),
    RbTimeoutTref = erlang:send_after(TimeDelay, self(), rebalance_now),
    State#state{rb_timeout_tref = RbTimeoutTref};
start_rebalance_timer(State) ->
    State.


start_bad_sink_timer(State = #state{bad_sink_nodes = []}) ->
    State;
start_bad_sink_timer(State = #state{bs_timeout_tref = undefined}) ->
    TimeDelay = app_helper:get_env(riak_repl, rt_retry_bad_sinks, 120) * 1000,
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
connection_failed(Addr, State = #state{connection_failed_counts = Dict, bad_sink_nodes = BadSinkNodes}) ->
    case lists:member(Addr, BadSinkNodes) of
        false ->
            NewDict = orddict:update_counter(Addr, 1, Dict),
            State1 = decrease_number_of_pending_connects(State#state{connection_failed_counts = NewDict}),
            State2 = set_status(State1),
            set_bad_sink_nodes(State2);
        true ->
            start_bad_sink_timer(State)
    end.

%%%===================================================================
%% Accept Connection
%%%===================================================================
accept_connection(Socket, Transport, IPPort, Proto, Props, State) ->
    #state{remote = Remote, id = Id} = State,
    {BadSinkReconnect, State0} = maybe_reset_bad_sink_node(IPPort, State),
    case riak_repl2_rtsource_conn:start(Remote, Id) of
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

update_state_for_new_connection(false, Ref, RtSourcePid, Addr, State) ->
    State1 = add_connection_monitor(State, Ref, RtSourcePid, Addr),
    State2 = increase_connection_count(Addr, State1),
    State3 = decrease_number_of_pending_connects(State2),
    State4 = set_status(State3),
    set_bad_sink_nodes(State4);
update_state_for_new_connection(true, Ref, RtSourcePid, IPPort, State) ->
    State1 = add_connection_monitor(State, Ref, RtSourcePid, IPPort),
    increase_connection_count(State1, IPPort).


%%%===================================================================
%% Maybe Do The Rebalance (if we are balanced/ balancing, do not do the rebalance!)
%%%===================================================================
maybe_do_rebalance(State = #state{ipl_timeout_tref = undefined, balancing = false, balanced = false}) ->
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
%% Calculate Connections To Add
%%%===================================================================
should_add_connections(State) ->
    #state{connections = Connections, balanced_connection_counts = BalancedConnectionCounts} = State,
    {ConnectionsToAdd, BalancedConnectionCounts} =
        orddict:fold(fun should_add_connections_helper/3,
            {orddict:new(), BalancedConnectionCounts}, Connections),
    case ConnectionsToAdd of
        [] ->
            false;
        _ ->
            {true, ConnectionsToAdd}
    end.

should_add_connections_helper(Id, Connections, {Dict, BalancedConnectionCounts}) ->
    #connections{connection_counts = ConnectionCounts} = Connections,
    case BalancedConnectionCounts of
        [] ->
            false;
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
                    {Dict, BalancedConnectionCounts};
                _ ->
                    {orddict:store(Id, Add, Dict), BalancedConnectionCounts}
            end
    end.

%%%===================================================================
%% Calculate Connections To Remove
%%%===================================================================
should_remove_connections(State) ->
    #state{connections = Connections, balanced_connection_counts = BalancedConnectionCounts} = State,
    {ConnectionsToRemove, BalancedConnectionCounts} =
        orddict:fold(fun should_remove_connections_helper/3,
            {orddict:new(), BalancedConnectionCounts}, Connections),
    case ConnectionsToRemove of
        [] ->
            false;
        _ ->
            {true, ConnectionsToRemove}
    end.

should_remove_connections_helper(Id, Connections, {Dict, BalancedConnectionCounts}) ->
    #connections{connection_counts = ConnectionCounts} = Connections,
    case BalancedConnectionCounts of
        [] ->
            false;
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
                    {Dict, BalancedConnectionCounts};
                _ ->
                    {orddict:store(Id, Remove, Dict), BalancedConnectionCounts}
            end
    end.



%%%===================================================================
%% Remove Connections Gracefully
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
%% Add Connections
%%%===================================================================
%% Dict of Dicts
add_connections(AddDicts, State) ->
    State1 = orddict:fold(fun do_add_connections/3, State, AddDicts),
    State2 = set_balancing_status(State1),
    set_balanced_status(State2).

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

%%%===================================================================
%% Get the number of connections for our remote
%%%===================================================================
round_up_connections(N) ->
    Concurrency = app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)),
    case N rem Concurrency of
        0 ->
            N div Concurrency;
        _ ->
            (N+Concurrency) div Concurrency
    end.

get_number_of_connections() ->
    case app_helper:get_env(riak_repl, default_number_of_connections) of
        N when is_integer(N) ->
            round_up_connections(N);
        _ ->
            round_up_connections(?DEFAULT_NO_CONNECTIONS)
    end.
-ifdef(TEST).
get_number_of_connections(_) ->
    get_number_of_connections().
-else.
get_number_of_connections(Name) ->
    case riak_core_metadata:get(?RIAK_REPL2_CONFIG_KEY, {number_of_connections, Name}) of
        N when is_integer(N) ->
            round_up_connections(N);
        _ ->
            get_number_of_connections()
    end.
-endif.
%%%===================================================================================================================%%
%%                                              Update State                                                          %%
%%%===================================================================================================================%%

%%%===================================================================
%% Update Bad Sink Nodes
%%%===================================================================
%% This is used to check if we have any connections to a sink node after we have completed a rebalance
%% If we do not, we determine that we are unable to connect to the node and set it as a 'bad' node.
%% This allows us to rebalance to the remaining 'good' nodes, while we periodically attempt to reconnect to the 'bad'
%% nodes.
set_bad_sink_nodes(State = #state{balancing = true}) ->
    State;
set_bad_sink_nodes(State = #state{connection_failed_counts = []}) ->
    State;
set_bad_sink_nodes(State = #state{connection_counts = Conns}) ->
    case orddict:fold(fun find_no_sink_connections/3, [], Conns) of
        [] ->
            State#state{connection_failed_counts = orddict:new()};
        BadSinks ->
            State1 = State#state{bad_sink_nodes = BadSinks, connection_failed_counts = orddict:new()},
            State2 = set_balanced_connections(State1),
            State3 = set_status(State2),
            State4 = start_rebalance_timer(State3),
            start_bad_sink_timer(State4)
    end.

find_no_sink_connections(Addr, 0, Acc) ->
    [Addr | Acc];
find_no_sink_connections(_, _, Acc) ->
    Acc.

%%%===================================================================
%% Reset Bad Sink Nodes
%%%===================================================================
%% This is called everytime we accept a connection
%% If we accept a connection, and its in the list of bad sink nodes, we should reset that node back to being a 'good'
%% node and fire off a rebalance
maybe_reset_bad_sink_node(Addr, State = #state{bad_sink_nodes = BadSinkNodes}) ->
    case lists:delete(Addr, BadSinkNodes) of
        BadSinkNodes ->
            {false, State};
        NewBadSinkNodes ->
            State1 = State#state{bad_sink_nodes = NewBadSinkNodes},
            State2 = set_balanced_connections(State1),
            State3 = set_status(State2),
            {true, start_rebalance_timer(State3)}
    end.

%%%===================================================================
%% Add Connection Monitor
%%%===================================================================
add_connection_monitor(State, Ref, Pid, Addr) ->
    #state{connections_monitor_pids = Pids, connections_monitor_addrs = Addrs} = State,
    State#state
    {
        connections_monitor_addrs = orddict:store(Ref, Addr, Addrs),
        connections_monitor_pids = orddict:store(Ref, Pid, Pids)
    }.

%%%===================================================================
%% Remove Connection Monitor
%%%===================================================================
remove_connection_monitor(Ref, State) ->
    #state{connections_monitor_pids = Pids, connections_monitor_addrs = Addrs} = State,
    State#state
    {
        connections_monitor_addrs = orddict:erase(Ref, Addrs),
        connections_monitor_pids = orddict:erase(Ref, Pids)
    }.


%%===================================================================
%% Increase The Connection Count For An Addresss
%%%===================================================================
%% increases the connection count for a given address
increase_connection_count(Addr, State) ->
    #state{connection_counts = ConnectionCounts} = State,
    State#state{connection_counts = orddict:update_counter(Addr, 1, ConnectionCounts)}.

%%===================================================================
%% Decrease The Connection Count For An Addresss
%%%===================================================================
%% decreases the connection count for a given address
decrease_connection_count(Addr, State) ->
    #state{connection_counts = ConnectionCounts} = State,
    State#state{connection_counts = orddict:update_counter(Addr, -1, ConnectionCounts)}.


%%%===================================================================
%% Increase Number Of Pending Disconnects
%%%===================================================================
%% This function increases the number of pending disconnects
increase_number_of_pending_disconnects(State = #state{number_of_pending_disconnects = N}) ->
    State#state{number_of_pending_disconnects = N +1}.

%%%===================================================================
%% Decrease Number Of Pending Disconnects
%%%===================================================================
%% This function decreases the number of pending disconnects
decrease_number_of_pending_disconnects(State = #state{number_of_pending_disconnects = N}) ->
    State#state{number_of_pending_disconnects = N -1}.


%%%===================================================================
%% Increase Number Of Pending Connections
%%%===================================================================
%% This function increases the number of pending connects
increase_number_of_pending_connects(State = #state{number_of_pending_connects = N}) ->
    State#state{number_of_pending_connects = N +1}.

%%%===================================================================
%% Decrease Number Of Pending Connections
%%%===================================================================
%% This function decreases the number of pending connects
decrease_number_of_pending_connects(State = #state{number_of_pending_connects = N}) ->
    State#state{number_of_pending_connects = N -1}.



%%%===================================================================
%% Set Status's
%%%===================================================================
set_status(State) ->
    State1 = set_balanced_status(State),
    set_balancing_status(State1).

%%%===================================================================
%% Set Balanced Status
%%%===================================================================
%% This function checks if the number of connections we have is the same as the number of connections we should have
%% (that is determined by the balanced_connections_counts saved in state)
%% We save that to state, to allow us to skip rebalancing in scenario's when its not needed
set_balanced_status(State = #state{connection_counts = A, balanced_connection_counts = B}) ->
    State#state{balanced = A == B}.

%%%===================================================================
%% Set Balancing Status
%%%===================================================================
%% This function takes the values of the pending number of disconnects/ connects, if they are both not 0, we set this
%% to true
set_balancing_status(State = #state{number_of_pending_connects = 0, number_of_pending_disconnects = 0}) ->
    State#state{balancing = false};
set_balancing_status(State) ->
    State#state{balancing = true}.

%%%===================================================================
%% Set Balanced Connections
%%%===================================================================
%% This grabs the list of sink nodes from the cluster_mgr (in a specific order for our own node)
%% We then use this list in combination with the config for the number of connections to have for this remote
%% to build and list of {Addr, Count} which tells us how many times to connect to a given node
set_balanced_connections(State) ->
    #state{remote = Remote, bad_sink_nodes = BadSinkNodes} = State,
    TotalNumberOfConnections = get_number_of_connections(Remote),
    case riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(Remote) of
        {ok, []} ->
            start_empty_ip_list_timer(State);
        {ok, SinkNodes0} ->
            BadSinkNodesDown = BadSinkNodes -- SinkNodes0,
            NewBadSinkNodes = BadSinkNodes -- BadSinkNodesDown,
            SinkNodes = SinkNodes0 -- NewBadSinkNodes,
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
            State#state
            {
                balanced_connection_counts = BalancedConnectionCounts,
                sink_nodes = SinkNodes,
                bad_sink_nodes = NewBadSinkNodes
            }
    end.
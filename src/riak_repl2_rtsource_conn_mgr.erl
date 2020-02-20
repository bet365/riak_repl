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
    connect_failed/3,
    maybe_rebalance/1,
    stop/1,

    get_all_status/1,
    get_all_status/2,
    get_endpoints/1,
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
    connecting = orddict:new(),                 %% number of of pending connections per ip addr
    connection_counts = orddict:new(),          %% number of established connections per ip addr
    balanced_connection_counts = orddict:new(), %% same as above but what the counts should be under a correct balance
    connection_monitors = orddict:new(),        %% monitor references mapped to {pid, addr}
    rb_timeout_tref,                            %% rebalance timeout timer reference
    reference_rtq
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Remote) ->
    gen_server:start_link(?MODULE, [Remote], []).

connected(Socket, Transport, IPPort, Proto, RTSourceConnMgrPid, _Props) ->
    Transport:controlling_process(Socket, RTSourceConnMgrPid),
    try
    gen_server:call(RTSourceConnMgrPid,
        {connected, Socket, Transport, IPPort, Proto, _Props})
    catch
        _:Reason ->
            lager:warning("Unable to contact RT Source Conn Manager (~p). Killing it to force a reconnect", RTSourceConnMgrPid),
            exit(RTSourceConnMgrPid, {unable_to_contact, Reason}),
            ok
    end.

connect_failed(_ClientProto, Reason, RTSourceConnMgrPid) ->
    gen_server:cast(RTSourceConnMgrPid, {connect_failed, Reason}).

maybe_rebalance(Pid) ->
    gen_server:cast(Pid, maybe_rebalance).

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
%%    MaxDelaySecs = app_helper:get_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 120),
%%    M = fun(X) -> 10 + round(X * crypto:rand_uniform(0, 1000)) end,
    case whereis(riak_repl2_reference_rtq:name(Remote)) of
        undefined ->
            lager:error("undefined reference rtq for remote: ~p", [Remote]),
            {stop, {error, "undefined reference rtq"}};
        ReferenceQ ->
            erlang:monitor(process, ReferenceQ),
            {State, TriggerRebalance} = connect_to_sink(Remote),
            case TriggerRebalance of
                true ->
                    gen_server:cast(self(), maybe_rebalance);
                _ ->
                    ok
            end,
            {ok, State}
    end.

%%%=====================================================================================================================
handle_call({connected, Socket, Transport, IPPort, Proto, _Props}, _From,
    State = #state{remote = Remote}) ->

    lager:info("Adding a connection and starting rtsource_conn ~p", [Remote]),
    case riak_repl2_rtsource_conn:start_link(Remote) of
        {ok, RtSourcePid} ->
            case riak_repl2_rtsource_conn:connected(Socket, Transport, IPPort, Proto, RtSourcePid, _Props) of
                ok ->

                    {reply, ok, State};

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

%% TODO: this needs to change to 'DOWN'
handle_info({'EXIT', Pid, Reason}, State = #state{endpoints = E, remote = Remote, version = Version}) ->
    NewState = case lists:keyfind(Pid, 2, dict:to_list(E)) of
                   {Key, Pid} ->
                       NewEndpoints = dict:erase(Key, E),
                       State2 = State#state{endpoints = NewEndpoints},
                       {IPPort, Primary} = Key,

                       case Version of
                           legacy ->
                               ok;
                           v1 ->
                               riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, Remote, node(), IPPort, Primary)
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
    [catch riak_repl2_rtsource_conn:stop(Pid) || {{{_IP, _Port},_Primary},Pid} <- dict:to_list(E)],
    ok.

%%%=====================================================================================================================

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%%%=====================================================================================================================

%%%===================================================================
%%% Internal functions
%%%===================================================================


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






%% TODO: make this a generic rebalance function (use BalanacedConnection -- (ConnectionCounts ++ PendingConnections))
rebalance_connections(State) ->
    #state{connection_counts = ConnectionCounts, remote = Remote} = State,
    case calculate_balanced_connections(State) of
        [] ->
            problem;
        BalancedConnections ->
            PendingConnections =
                orddict:fold(fun connect_to_sink_n_times/3, {orddict:new(), Remote}, BalancedConnections),
            #state
                {
                    remote = Remote,
                    balanced_connection_counts = BalancedConnections,
                    connecting = PendingConnections
                }
    end.

calculate_balanced_connections(State) ->
    #state{remote = Remote} = State,
    TotalNumberOfConnections = get_number_of_connections(Remote),
    case riak_core_cluster_mgr:get_ipaddrs_of_cluster_single(Remote) of
        [] ->
            orddict:new();
        SinkNodes ->
            NumberOfSinkNodes = length(SinkNodes),
            TotalExtraConnections = TotalNumberOfConnections rem NumberOfSinkNodes,
            AverageNumberOfConnections = (TotalNumberOfConnections - TotalNumberOfConnections) div NumberOfSinkNodes,

            {BalancedConnectionCounts, _} =
                lists:foldl(
                    fun(Addr, {Acc, Counter}) ->
                        case Counter =< TotalExtraConnections of
                            true ->
                                {orddict:store(Addr, AverageNumberOfConnections +1, Acc), Counter +1};
                            false ->
                                {orddict:store(Addr, AverageNumberOfConnections, Acc), Counter +1}
                        end
                    end, {orddict:new(), 1}, lists:seq(1, SinkNodes)
                ),
            BalancedConnectionCounts
    end.


connect_to_sink_n_times(_Addr, 0, {PendingConnections, Remote}) ->
    {PendingConnections, Remote};
connect_to_sink_n_times(Addr, N, {PendingConnections, Remote}) ->
    connect_to_sink_n_times(Addr, N-1, connect_to_sink(Addr, PendingConnections, Remote)).

connect_to_sink(Addr, PendingConnections, Remote) ->
    case riak_core_connection_mgr:connect({rt_repl, Remote}, ?CLIENT_SPEC, {use_only, [Addr]}) of
        {ok, _Ref} ->
            Count =
                case orddict:find(Addr, PendingConnections) of
                    error -> 0;
                    C -> C
                end,
            PendingConnections2 = orddict:store(Addr, Count +1, PendingConnections),
            {PendingConnections2, Remote};
        _->
            {PendingConnections, Remote}
    end.

-ifdef(TEST).
get_number_of_connections(_) ->
    app_helper:get_env(riak_repl, number_of_connections, ?DEFAULT_NO_CONNECTIONS).

-else.

get_number_of_connections(Name) ->
    case riak_core_metadata:get(?RIAK_REPL2_RTQ_CONFIG_KEY, {number_of_connections, Name}) of
        undefined -> app_helper:get_env(riak_repl, number_of_connections, ?DEFAULT_NO_CONNECTIONS);
        NumberOfConnections -> NumberOfConnections
    end.
-endif.
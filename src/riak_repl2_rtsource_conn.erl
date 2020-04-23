%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.

%% @doc Realtime replication source connection module
%%
%% Works in tandem with rtsource_helper.  The helper interacts with
%% the RTQ to send queued traffic over the socket.  This rtsource_conn
%% process accepts the remote Acks and clears the RTQ.
%%
%% If both sides support heartbeat message, it is sent from the RT source
%% every `{riak_repl, rt_heartbeat_interval}' which default to 15s.  If
%% a response is not received in {riak_repl, rt_heartbeat_timeout}, also
%% default to 15s then the source connection exits and will be re-established
%% by the supervisor.
%%
%% 1. On startup/interval timer - `rtsource_conn' casts to `rtsource_helper'
%%    to send over the socket.  If TCP buffer is full or `rtsource_helper'
%%    is otherwise hung the `rtsource_conn' process will still continue.
%%    `rtsource_conn' sets up a heartbeat timeout.
%%
%% 2. At rtsink, on receipt of a heartbeat message it sends back
%%    a heartbeat message and stores the timestamp it last received one.
%%    The rtsink does not worry about detecting broken connections
%%    as new ones can be established harmlessly.  Keep it simple.
%%
%% 3. If rtsource receives the heartbeat back, it cancels the timer
%%    and updates the hearbeat round trip time (`hb_rtt') then sets
%%    a new heartbeat_interval timer.
%%
%%    If the heartbeat_timeout fires, the rtsource connection terminates.
%%    The `rtsource_helper:stop' call is now wrapped in a timeout in
%%    case it is hung so we don't get nasty messages about `rtsource_conn'
%%    crashing when it's the helper that is causing the problems.
-module(riak_repl2_rtsource_conn).

-behaviour(gen_server).
-include("riak_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([riak_core_connection_mgr_connect/2, start_test/2]).
-endif.

%% API
-export([
    start/3,
    stop/1,
    get_helper_pid/1,
    status/1,
    status/2,
    get_address/1,
    get_socketname_primary/1,
    connected/7,
    graceful_shutdown/1,
    object_sent/2,
    get_latency/2
]).

-export([get_ref/1]).
-export([get_heartbeat_timeout/1, get_heartbeat_interval/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state,
{
    id,
    remote,    % remote name
    address,   % {IP, Port}
    transport, % transport module
    socket,    % socket to use with transport
    peername,  % cached when socket becomes active
    proto,     % protocol version negotiated
    ver,       % wire format negotiated
    helper_pid,% riak_repl2_rtsource_helper pid
    hb_interval_tref,
    hb_timeout_tref,% heartbeat timeout timer reference
    hb_sent_q,   % queue of heartbeats now() that were sent
    hb_rtt,    % RTT in milliseconds for last completed heartbeat
    cont = <<>>, % continuation from previous TCP buffer

    %% Protocol 4
    latency = #distribution_collector{},
    connection_mgr_pid,
    connection_mgr_ref,
    shutting_down = false,
    sink_shutdown = false,
    expect_seq_v4 = 1,
    object_sent_time,
    ref
}).

start(Remote, Id, ConnMgr) ->
    gen_server:start(?MODULE, [Remote, Id, ConnMgr], []).

stop(Pid) ->
    gen_server:call(Pid, stop, ?LONG_TIMEOUT).

status(Pid) ->
    status(Pid, ?LONG_TIMEOUT).

status(Pid, Timeout) ->
    try
        gen_server:call(Pid, status, Timeout)
    catch
        _:_ ->
            []
    end.

connected(RtSourcePid, Ref, Socket, Transport, IPPort, Proto, _Props) ->
    Transport:controlling_process(Socket, RtSourcePid),
    Transport:setopts(Socket, [{active, true}]),
    try
        gen_server:call(RtSourcePid, {connected, Ref, Socket, Transport, IPPort, Proto}, ?LONG_TIMEOUT)
    catch
        _:Reason ->
            lager:warning("Unable to contact RT source connection process (~p). Reason ~p"
                          "Killing it to force reconnect.",
                [RtSourcePid, Reason]),
            error
    end.

get_helper_pid(RtSourcePid) ->
  gen_server:call(RtSourcePid, get_helper_pid).

get_address(Pid) ->
  gen_server:call(Pid, address, ?LONG_TIMEOUT).

get_socketname_primary(Pid) ->
  gen_server:call(Pid, get_socketname_primary).

object_sent(Pid, Time) ->
    gen_server:call(Pid, {object_sent, Time}, ?LONG_TIMEOUT).

get_latency(Pid, Timeout) ->
    try
        gen_server:call(Pid, latency, Timeout)
    catch
        _:_ ->
            error
    end.

graceful_shutdown(Pid) ->
    try
        gen_server:call(Pid, graceful_shutdown)
    catch
        _:_  ->
        busy
    end.

%% ==============================
%% For Testing
%% ==============================
get_ref(Pid) ->
    gen_server:call(Pid, get_ref, infinity).

% ======================================================================================================================

%% gen_server callbacks

%% Init for start_test
init([Remote, Id]) ->
    {ok, #state{remote = Remote, id = Id}};

%% Initialize
init([Remote, Id, ConnMgr]) ->
    Ref = erlang:monitor(process, ConnMgr),
    {ok, #state{remote = Remote, id = Id, connection_mgr_pid = ConnMgr, connection_mgr_ref = Ref}}.

handle_call(get_ref, _From, State = #state{ref = Ref}) ->
    {reply, Ref, State};

handle_call(stop, _From, State) ->
  {stop, shutdown, ok, State};

handle_call(address, _From, State = #state{address=A}) ->
    {reply, A, State};

handle_call(get_socketname_primary, _From, State=#state{socket = S}) ->
  {ok, Peername} = inet:sockname(S),
  {reply, Peername, State};

handle_call(status, _From, State) ->
    {reply, get_status(State), State};

handle_call(latency, _From, State = #state{latency = Latency, peername = Peername}) ->
    #distribution_collector{timestamp = T1} = Latency,
    ResetAfter = get_consumer_latency_reset(),
    TimeDiff = timer:now_diff(os:timestamp(), T1) / 1000,
    case  TimeDiff >= ResetAfter of
        true ->
            {reply, {Peername, #distribution_collector{}}, State#state{latency = #distribution_collector{}}};
        false ->
            {reply, {Peername, Latency}, State}
    end;

handle_call({object_sent, Time}, _From, State) ->
    {reply, ok, State#state{object_sent_time = Time}};

handle_call({connected, Ref, Socket, Transport, EndPoint, Proto}, _From, State) ->
    #state{remote = Remote, id = Id} = State,
    %% Check the socket is valid, may have been an error
    %% before turning it active (e.g. handoff of riak_core_service_mgr to handler
    case Transport:send(Socket, <<>>) of
        ok ->
            Ver = riak_repl_util:deduce_wire_version_from_proto(Proto),
            {_, ClientVer, _} = Proto,
            {ok, HelperPid} =
                riak_repl2_rtsource_helper:start_link(Remote, Id, Transport, Socket, ClientVer, Ref, self()),
            SocketTag = riak_repl_util:generate_socket_tag("rt_source", Transport, Socket),
            riak_core_tcp_mon:monitor(Socket, {?TCP_MON_RT_APP, source, SocketTag}, Transport),
            State2 =
                State#state
                {
                    transport = Transport,
                    socket = Socket,
                    address = EndPoint,
                    proto = Proto,
                    peername = peername(Transport, Socket),
                    helper_pid = HelperPid,
                    ver = Ver,
                    ref = Ref
                },
            case Proto of
                {realtime, _OurVer, {1, 0}} ->
                    {reply, ok, State2};
                _ ->
                    %% 1.1 and above, start with a heartbeat
                    State3 = State2#state{hb_sent_q = queue:new()},
                    {reply, ok, send_heartbeat(State3)}
            end;
        ER ->
            {reply, ER, State}
    end;

handle_call(graceful_shutdown, _From, State) ->
    NewState = set_shutdown(State),
    {reply, something, NewState};

handle_call(get_helper_pid, _From, State=#state{helper_pid = H}) ->
    {reply, H, State}.

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info({Proto, _S, TcpBin}, State= #state{cont = Cont})
    when Proto == tcp; Proto == ssl ->
    recv(<<Cont/binary, TcpBin/binary>>, State);

handle_info({Closed, _S}, State = #state{remote = Remote, cont = Cont})
    when Closed == tcp_closed; Closed == ssl_closed ->
    case size(Cont) of
        0 ->
            ok;
        NumBytes ->
            riak_repl_stats:rt_source_errors(),
            lager:warning("Realtime connection ~s to ~p closed with partial receive of ~b bytes\n",
                          [peername(State), Remote, NumBytes])
    end,
    perform_shutdown(State);

handle_info({Error, _S, Reason}, State = #state{remote = Remote, cont = Cont})
  when Error == tcp_error; Error == ssl_error ->
    riak_repl_stats:rt_source_errors(),
    lager:warning("Realtime connection ~s to ~p network error ~p - ~b bytes pending\n",
                  [peername(State), Remote, Reason, size(Cont)]),

    perform_shutdown(State);

handle_info(send_heartbeat, State) ->
    {noreply, send_heartbeat(State)};

handle_info({heartbeat_timeout, HBSent}, State ) ->
    #state{hb_sent_q = HBSentQ, hb_timeout_tref = HBTRef, remote = Remote} = State,
    TimeSinceTimeout = timer:now_diff(now(), HBSent) div 1000,

    %% hb_timeout_tref is the authority of whether we should
    %% restart the conection on heartbeat timeout or not.
    case HBTRef of
        undefined ->
            lager:info("Realtime connection ~s to ~p heartbeat time since timeout ~p",
                       [peername(State), Remote, TimeSinceTimeout]),
            {noreply, State};
        _ ->
            HBTimeout = get_heartbeat_timeout(State#state.remote),
            lager:warning("Realtime connection ~s to ~p heartbeat timeout after ~p seconds\n",
                          [peername(State), Remote, HBTimeout]),
            lager:info("hb_sent_q_len after heartbeat_timeout: ~p", [queue:len(HBSentQ)]),

            perform_shutdown(State)
    end;

handle_info({'DOWN', MonitorRef, process, Pid, _Reason},
    State = #state{connection_mgr_ref = MonitorRef, connection_mgr_pid = Pid}) ->
    {stop, conn_mgr_died, State};

handle_info(Msg, State) ->
    lager:warning("Unhandled info:  ~p", [Msg]),
    {noreply, State}.

terminate(Reason, #state{socket = Socket, transport = Transport, address = A}) ->
    catch Transport:close(Socket),
    lager:info("rtsource conn terminated due to ~p, Endpoint: ~p", [Reason, A]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ================================================================================================================== %%
%% Internal Functions
%% ================================================================================================================== %%

cancel_timer(undefined) -> ok;
cancel_timer(TRef)      -> _ = erlang:cancel_timer(TRef).

peername(Transport, Socket) ->
    riak_repl_util:peername(Socket, Transport).

peername(#state{peername = P}) ->
    P.

get_status(State) ->
    #state
    {
        remote = R,
        transport = T,
        socket = S,
        expect_seq_v4 = Sent,
        hb_rtt = HBRTT
    } = State,

    Props =
        case T of
            undefined ->
                [{connected, false}];
            _ ->
                HBStats = [{hb_rtt, HBRTT}],
                SocketStats = riak_core_tcp_mon:socket_status(S),
                [
                    {connected, true},
                    {transport, T},
                    {objects, Sent},
                    {socket, riak_core_tcp_mon:format_socket_stats(SocketStats, [])}
                ] ++ HBStats
        end,
    FormattedPid = riak_repl_util:safe_pid_to_list(self()),
    [{sink, R}, {pid, FormattedPid}] ++ Props.

%% ================================================================================================================== %%
%% Receive functionality from sink
%% ================================================================================================================== %%

recv(TcpBin, State) ->
    handle_incoming_data(riak_repl2_rtframe:decode(TcpBin), State).

%% more data to be received
handle_incoming_data({ok, undefined, Cont}, State) ->
    {noreply, State#state{cont = Cont}};

%% This deals with the incoming heartbeats
handle_incoming_data({ok, heartbeat, Cont}, State) ->
    #state{hb_timeout_tref = HBTRef, hb_sent_q = HBSentQ} = State,
    {{value, HBSent}, HBSentQ2} = queue:out(HBSentQ),
    HBRTT = timer:now_diff(now(), HBSent) div 1000,
    _ = cancel_timer(HBTRef),
    State2 = State#state{hb_sent_q = HBSentQ2, hb_timeout_tref = undefined, hb_rtt = HBRTT},
    recv(Cont, schedule_heartbeat(State2));


handle_incoming_data({ok, node_shutdown, Cont}, State) ->
    #state{helper_pid = Helper, expect_seq_v4 = ESeq} = State,

    %% inform helper so it does not accept anymore data
    {ok, Seq} = riak_repl2_rtsource_helper:shutting_down(Helper),
    NewState = set_sink_shutdown(State),

    case (Seq +1 == ESeq) orelse (Seq == 0) of
        true ->
            %% we have the ack back for the last sequence number sent by the helper (terminate)
            perform_shutdown(NewState);
        false ->
            recv(Cont, NewState)
    end;

%% This is the most upto date protocol to be used if all clusters are up to date with repl
%% Because we are providing in order sequence numbers, it no longer matters on the ProtoMajor version
%% we can just ack the reference queue, and it shall deal with it correctly.

%% in the old code we used to ack differently (via sink_helper:ack_v1), this is no longer needed of protocol > 2
handle_incoming_data({ok, {ack, Seq}, Cont}, State = #state{expect_seq_v4 = Seq, object_sent_time = TimeSent}) ->
    %% update stats
    Now = os:timestamp(),
    Diff = timer:now_diff(Now, TimeSent),
    State1 = update_latency(Diff, State),

    %% update objects sent
    riak_repl_stats:objects_sent(),

    %% Reset heartbeat timer, since we've seen activity from the peer
    State2 = reset_heartbeat_timer(State1),

    %% ack the reference queue
    #state{remote = Remote, id = Id, ref = Ref} = State2,
    ok = riak_repl2_reference_rtq:ack(Remote, Id, Ref, Seq),

    %% check if we are shutting down gracefully
    case State1#state.shutting_down of
        false ->
            recv(Cont, State1#state{expect_seq_v4 = Seq +1});
        _ ->
            perform_shutdown(State1)
    end;

handle_incoming_data({ok, {retrying, Seq}, Cont}, State = #state{expect_seq_v4 = Seq}) ->
    %% Reset heartbeat timer, since we've seen activity from the peer
    State1 = reset_heartbeat_timer(State),
    recv(Cont, State1);

handle_incoming_data({ok, {bt_drop, Seq}, Cont}, State = #state{expect_seq_v4 = Seq}) ->
    riak_repl_stats:objects_sent(),
    %% Reset heartbeat timer, since we've seen activity from the peer
    State1 = reset_heartbeat_timer(State),

    %% ack the reference queue
    #state{remote = Remote, id = Id, ref = Ref} = State,
    %% TODO: report a drop instead of acking! (new functionality to queue)
    ok = riak_repl2_reference_rtq:ack(Remote, Id, Ref, Seq),

    %% check if we are shutting down gracefully
    case State#state.shutting_down of
        false ->
            recv(Cont, State1#state{expect_seq_v4 = Seq +1});
        _ ->
            perform_shutdown(State)
    end;

handle_incoming_data({ok, {Type, Seq}, _}, State) ->
    handle_wrong_seq(Type, Seq, State).


handle_wrong_seq(Type, Seq, State) ->
    #state{expect_seq_v4 = ESeq, peername = Peername, remote = Remote} = State,
    %% wrong seq number
    lager:error("(~p) recevied incorrect sequence from sink: seq: ~p, expected_seq: ~p, peername:~p, remote:~p",
        [Type, Seq, ESeq, Peername, Remote]),

    %% we should be doing a shutdown check here to ensure conn_mgr doesn't hit a race condition
    perform_shutdown(State).


reset_heartbeat_timer(State = #state{hb_timeout_tref = Ref}) ->
    case Ref of
        undefined ->
            State;
        _ ->
            _ = cancel_timer(Ref),
            schedule_heartbeat(State#state{hb_timeout_tref=undefined})
    end.


set_sink_shutdown(State) ->
    State#state{shutting_down = true, sink_shutdown = true}.

set_shutdown(State) ->
    State#state{shutting_down = true}.


perform_shutdown(State = #state{shutting_down = true, sink_shutdown = true}) ->
    {stop, sink_shutdown, State};
perform_shutdown(State = #state{shutting_down = true}) ->
    {stop, shutdown, State};
perform_shutdown(State) ->
    {stop, unexpected_shutdown, State}.

%% ================================================================================================================== %%
%% Heartbeat's
%% ================================================================================================================== %%
%% Heartbeat supported and enabled, tell helper to send the message,
%% and start the timeout.  Managing heartbeat from this process
%% will catch any bug that causes the helper process to hang as
%% well as connection issues - either way we want to re-establish.
send_heartbeat(State) ->
    #state
    {
        hb_sent_q = SentQ,
        helper_pid = HelperPid
    } = State,
    % Using now as need a unique reference for this heartbeat
    % to spot late heartbeat timeout messages
    Now = now(),
    riak_repl2_rtsource_helper:send_heartbeat(HelperPid),
    HBTimeout = get_heartbeat_timeout(State#state.remote),
    TRef = erlang:send_after(HBTimeout, self(), {heartbeat_timeout, Now}),
    State2 = State#state{hb_interval_tref = undefined, hb_timeout_tref = TRef, hb_sent_q = queue:in(Now, SentQ)},
    lager:debug("hb_sent_q_len after sending heartbeat: ~p", [queue:len(SentQ)+1]),
    State2.

%% Schedule the next heartbeat
schedule_heartbeat(State = #state{hb_interval_tref = undefined}) ->
    HBInterval = get_heartbeat_interval(State#state.remote),
    TRef = erlang:send_after(HBInterval, self(), send_heartbeat),
    State#state{hb_interval_tref = TRef};
schedule_heartbeat(State = #state{hb_interval_tref = _TRef}) ->
    State.


get_heartbeat_interval_default() ->
    case app_helper:get_env(riak_repl, default_rt_heartbeat_interval) of
        Time when is_integer(Time) -> Time * 1000;
        _ -> ?RT_HEARTBEAT_DEFAULT_INTERVAL
    end.

get_heartbeat_timeout_default() ->
    case app_helper:get_env(riak_repl, default_rt_heartbeat_timeout) of
        Time when is_integer(Time) -> Time * 1000;
        _ -> ?RT_HEARTBEAT_DEFAULT_TIMEOUT
    end.


-ifdef(TEST).

get_heartbeat_interval(_) ->
    case get_heartbeat_interval_default() of
        undefined ->  ?RT_HEARTBEAT_DEFAULT_INTERVAL;
        N -> N
    end.

get_heartbeat_timeout(_) ->
    case get_heartbeat_timeout_default() of
        undefined -> ?RT_HEARTBEAT_DEFAULT_TIMEOUT;
        N -> N
    end.

-else.

%% ------------------------------------------------------------------------------------------------------------------ %%
%%                                  Heartbeat Interval
%% ------------------------------------------------------------------------------------------------------------------ %%

get_heartbeat_interval(RemoteName) ->
    case get_heartbeat_interval_for_remote(RemoteName) of
        undefined -> get_heartbeat_interval_default();
        N -> N
    end.

get_heartbeat_interval_for_remote(RemoteName) ->
    case riak_core_metadata:get(?RIAK_REPL2_CONFIG_KEY, {rt_heartbeat_interval, RemoteName}) of
        Interval when is_integer(Interval) -> Interval * 1000;
        _ -> undefined
    end.

%% ------------------------------------------------------------------------------------------------------------------ %%
%%                                  Heartbeat Timeout
%% ------------------------------------------------------------------------------------------------------------------ %%

get_heartbeat_timeout(RemoteName) ->
    case get_heartbeat_timeout_for_remote(RemoteName) of
        undefined -> get_heartbeat_timeout_default();
        N -> N
    end.

get_heartbeat_timeout_for_remote(RemoteName) ->
    case riak_core_metadata:get(?RIAK_REPL2_CONFIG_KEY, {rt_heartbeat_timeout, RemoteName}) of
        Interval when is_integer(Interval) -> Interval * 1000;
        _ -> undefined
    end.
-endif.


%% ===================================================================
%% Update Latency
%% ===================================================================
update_latency(Time, State = #state{latency = #distribution_collector{timestamp = Tstamp}}) ->
    ResetAfter = get_consumer_latency_reset(),
    TimeDiff = timer:now_diff(os:timestamp(), Tstamp) / 1000,
    update_latency(Time, State, TimeDiff >= ResetAfter).

update_latency(Time, State, true) ->
    Latency = #distribution_collector
    {
        timestamp = os:timestamp(),
        number_data_points = 1,
        aggregate_values = Time,
        aggregate_values_sqrd = Time*Time,
        max = Time
    },
    State#state{latency = Latency};
update_latency(Time, State = #state{latency = Latency}, false) ->
    #distribution_collector
    {number_data_points = N1, aggregate_values = AV1, aggregate_values_sqrd = AVS1, max = Max1} = Latency,
    %% increase the number of data points
    N2 = N1 +1,
    %% calculate new aggregate values
    AV2 = AV1 + Time,
    %% calculate new aggregate values sqrd
    AVS2 = AVS1 + (Time*Time),
    %% calculate new max
    Max2 = case Time > Max1 of
               true ->
                   Time;
               false ->
                   Max1
           end,
    Latency2 = #distribution_collector
    {number_data_points = N2, aggregate_values = AV2, aggregate_values_sqrd = AVS2, max = Max2},
    State#state{latency = Latency2}.

get_consumer_latency_reset() ->
    case app_helper:get_env(riak_repl, reset_rtsource_conn_latency_stat) of
        N when is_integer(N), N > 0 -> N * 1000;
        _ -> ?RESET_CONSUMER_LATENCY_TIMEOUT
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(TCP_OPTIONS,  [{keepalive, true},
    {nodelay, true},
    {packet, 0},
    {active, false}]).

-define(CLIENT_SPEC, {{realtime,[{4,0}, {3,0}, {2,0}, {1,5}]}, {?TCP_OPTIONS, ?MODULE, self()}}).

start_test(Remote, Id) ->
    gen_server:start(?MODULE, [Remote, Id], []).


riak_repl2_rtsource_conn_test_() ->
    {spawn, [{
        setup,
        fun setup/0,
        fun cleanup/1,
        {timeout, 120, fun cache_peername_test_case/0}
    }]}.

setup() ->
    % now set up the environment for this test
    process_flag(trap_exit, true),
    riak_repl_test_util:start_test_ring(),
    riak_repl_test_util:abstract_gen_tcp(),
    riak_repl_test_util:abstract_stats(),
    riak_repl_test_util:abstract_stateful(),
    ok.

cleanup(_) ->
    application:unset_env(riak_repl, rtq_concurrency),
    rt_source_helpers:kill_fake_sink(),
    riak_repl_test_util:kill_and_wait([riak_repl2_rt, riak_core_tcp_mon]),
    riak_repl2_rtq:stop(1),
    catch riak_repl2_reference_rtq:stop("sink_cluster", 1),
    riak_repl_test_util:stop_test_ring(),
    riak_repl_test_util:maybe_unload_mecks([riak_core_service_mgr, riak_core_connection_mgr, gen_tcp]),
    meck:unload(),
    ok.

%% test for https://github.com/basho/riak_repl/issues/247
%% cache the peername so that when the local socket is closed
%% peername will still be around for logging
cache_peername_test_case() ->
    RemoteName = "sink_cluster",
    {ok, _RTPid} = rt_source_helpers:start_rt(),
    application:set_env(riak_repl, rtq_concurrency, 1),
    {ok, RTQPid} = riak_repl2_rtq:start_link(1),
    {ok, RefQPid} = riak_repl2_reference_rtq:start_link(RemoteName, 1),
    unlink(RTQPid),
    unlink(RefQPid),
    {ok, _TCPMonPid} = rt_source_helpers:start_tcp_mon(),
    {ok, _FsPid, FsPort} = rt_source_helpers:init_fake_sink(),
    Addr = {"127.0.0.1", FsPort},
    ok = setup_connection_for_peername(Addr),
    connect(RemoteName),
    ok.

%% Connect to the 'fake' sink
connect(RemoteName) ->
    stateful:set(version, {realtime, {1,0}, {1,0}}),
    stateful:set(remote, RemoteName),
    _ = riak_core_connection_mgr:connect({rt_repl, RemoteName}, ?CLIENT_SPEC),
    RTQStats = riak_repl2_rtq_sup:status(),
    ExpectedRTQStats =
        [
            {concurrency, 1},
            {percent_bytes_used, 0.0},
            {bytes,0},
            {max_bytes,104857600},
            {remotes, [{"sink_cluster", [{bytes,0}, {max_bytes,104857600}, {drops,0}]}]},
            {trimming_drops, 0},
            {overload_drops,0}
        ],
    ?assertEqual(ExpectedRTQStats, RTQStats).

%% Set up the test
setup_connection_for_peername(Addr) ->
    riak_repl_test_util:reset_meck(riak_core_connection_mgr, [no_link, passthrough]),
    meck:expect(riak_core_connection_mgr, connect,
        fun(_ServiceAndRemote, ClientSpec) ->
            proc_lib:spawn_link(?MODULE, riak_core_connection_mgr_connect, [ClientSpec, Addr]),
            {ok, make_ref()}
        end).
riak_core_connection_mgr_connect(ClientSpec, {RemoteHost, RemotePort} = Addr) ->
    Version = stateful:version(),
    {_Proto, {TcpOpts, Module, _Pid}} = ClientSpec,
    {ok, Socket} = gen_tcp:connect(RemoteHost, RemotePort, [binary | TcpOpts]),
    Ref = make_ref(),
    {ok, Pid} = riak_repl2_rtsource_conn:start_test("sink_cluster", 1),
    ok = Module:connected(Pid, Ref, Socket, gen_tcp, Addr, Version, []),

    % simulate local socket problem
    inet:close(Socket),

    % get the State from the source connection.
    {status,Pid,_,[_,_,_,_,[_,_,{data,[{_,State}]}]]} = sys:get_status(Pid),

    % getting the peername from the socket should produce error string
    ?assertEqual("error:einval", peername(inet, Socket)),

    % while getting the peername from the State should produce the cached string
    % format the string we expect from peername(State) ...
    {ok, HostIP} = inet:getaddr(RemoteHost, inet),
    RemoteText = lists:flatten(io_lib:format("~B.~B.~B.~B:~B", tuple_to_list(HostIP) ++ [RemotePort])),
    % ... and hook the function to check for it
    ?assertEqual(RemoteText, peername(State)),
    catch exit(Pid, test_complete).

-endif.
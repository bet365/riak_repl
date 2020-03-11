%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsink_conn).

%% @doc Realtime replication sink connection module
%%
%% High level responsibility...
%%  consider moving out socket responsibilities to another process
%%  to keep this one responsive (but it would pretty much just do status)
%%

%% API
-include("riak_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([sync_register_service/0,
         start_service/5]).

-export
([
    start_link/2,
    stop/1,
    set_socket/3,
    status/1,
    status/2,
    get_peername/1,
    send_shutdown/1
]).

%% Export for intercept use in testing
-export([send_heartbeat/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% how long to wait to reschedule socket reactivate.
-define(REACTIVATE_SOCK_INT_MILLIS, 10).
-define(DEFAULT_INTERVAL_MILLIS, 10000).
-define(DEFAULT_BUCKET_TYPE, <<"default">>).

-record(state,
{
    remote,           %% Remote site name
    transport,        %% Module for sending
    socket,           %% Socket
    proto,            %% Protocol version negotiated
    peername,         %% peername of socket
    wire_version,     %% wire format agreed with rt source
    helper,           %% Helper PID
    hb_last,          %% os:timestamp last heartbeat message received
    cont = <<>>,      %% Continuation from previous TCP buffer
    bt_drops,         %% drops due to bucket type mis-matches
    bt_interval,      %% how often (in ms) to report bt_drops
    bt_timer,         %% timer reference for interval

    %% protocol >= 3
    max_pending,      %% Maximum number of operations
    active = true,    %% If socket is set active
    deactivated = 0,  %% Count of times deactivated
    source_drops = 0, %% Count of upstream drops
    seq_ref,          %% Sequence reference for completed/acked
    expect_seq = undefined,%% Next expected sequence number
    acked_seq = undefined, %% Last sequence number acknowledged
    completed = [],   %% Completed sequence numbers that need to be sent

    %% protocol =< 4
    write_data_function, %% the function we use for writting the object to riak
    expected_seq_v4 = 1  %% the source will send seq number 1 first (the rest will be in order)
}).

%% ================================================================================================================== %%
%% API
%% ================================================================================================================== %%
%% Register with service manager
sync_register_service() ->
    %% version {3,0} supports typed bucket replication
    %% version {4,0} supports retries
    ProtoPrefs = {realtime,[{4,0}, {3,0}, {2,0}, {1,4}, {1,1}, {1,0}]},
    TcpOptions = [{keepalive, true}, % find out if connection is dead, this end doesn't send
                  {packet, 0},
                  {nodelay, true}],
    HostSpec = {ProtoPrefs, {TcpOptions, ?MODULE, start_service, undefined}},
    riak_core_service_mgr:sync_register_service(HostSpec, {round_robin, undefined}).

%% Callback from service manager
start_service(Socket, Transport, Proto, _Args, Props) ->
    SocketTag = riak_repl_util:generate_socket_tag("rt_sink", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_RT_APP, sink, SocketTag},
                              Transport),
    Remote = proplists:get_value(clustername, Props),
    {ok, Pid} = riak_repl2_rtsink_conn_sup:start_child(Proto, Remote),
    ok = Transport:controlling_process(Socket, Pid),
    ok = set_socket(Pid, Socket, Transport),
    {ok, Pid}.

start_link(Proto, Remote) ->
    gen_server:start_link(?MODULE, [Proto, Remote], []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%% Call after control handed over to socket
set_socket(Pid, Socket, Transport) ->
    gen_server:call(Pid, {set_socket, Socket, Transport}, infinity).

status(Pid) ->
    status(Pid, ?LONG_TIMEOUT).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

get_peername(Pid) ->
  gen_server:call(Pid, get_peername).

send_shutdown(Pid) ->
    gen_server:cast(Pid, send_shutdown).

%% ================================================================================================================== %%
%% gen_server callbacks
%% ================================================================================================================== %%

%% Callbacks
init([OkProto, Remote]) ->
    %% TODO: remove annoying 'ok' from service mgr proto
    {ok, Proto} = OkProto,
    {_, {CommonMajor, _}, {CommonMajor, _}} = Proto,
    Ver = riak_repl_util:deduce_wire_version_from_proto(Proto),
    {ok, Helper} = riak_repl2_rtsink_helper:start_link(self()),
    riak_repl2_rt:register_sink(self()),
    MaxPending = app_helper:get_env(riak_repl, rtsink_max_pending, 100),
    Report = app_helper:get_env(riak_repl, bucket_type_drop_report_interval, ?DEFAULT_INTERVAL_MILLIS),

    WriteDataFunction =
        case CommonMajor of
            4 -> fun do_write_objects_v4/2;
            _ -> fun do_write_objects_v3/2
        end,

    State =
        #state
        {
            remote = Remote,
            proto = Proto,
            max_pending = MaxPending,
            helper = Helper,
            wire_version = Ver,
            bt_drops = dict:new(),
            bt_interval = Report,
            write_data_function = WriteDataFunction
        },
    {ok, State}.

handle_call(status, _From, State) ->
    {reply, get_status(State), State};


handle_call({set_socket, Socket, Transport}, _From, State) ->
    Transport:setopts(Socket, [{active, once}]), % pick up errors in tcp_error msg
    NewState = State#state{socket=Socket, transport=Transport, peername = peername(Transport, Socket)},
    {reply, ok, NewState};

handle_call(get_peername, _From, State=#state{peername = PeerName}) ->
  {reply, PeerName, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.



%% Note pattern match on expected_seq_v4, as we do not increment upon receive, we only increment once placed
%% And now the source only sends us 1 object at a time
handle_cast(ack_v4, State = #state{expected_seq_v4 = Seq}) ->
    send_ack(Seq, State),
    {noreply, State#state{expected_seq_v4 = Seq +1}};

%% new mechanism to inform the source we struggled to place the object and we are retrying
%% on N number of failures we will not retry.
handle_cast(retrying, State = #state{expected_seq_v4 = Seq}) ->
    #state{transport = T, socket = S} = State,
    TcpIOL = riak_repl2_rtframe:encode(retrying, Seq),
    T:send(S, TcpIOL),
    {noreply, State};

%% protocol 4, send node shutdown message to source so they can handle it gracefully
handle_cast(send_shutdown, State = #state{proto = Proto, transport = T, socket = S}) ->
    {_, {CommonMajor, _}, {CommonMajor, _}} = Proto,
    case CommonMajor >= 4 of
        true ->
            TcpIOL = riak_repl2_rtframe:encode(node_shutdown, undefined),
            T:send(S, TcpIOL);
        false ->
            ok
    end,
    {noreply, State};




%% Note pattern patch on Ref
handle_cast({ack_v3, Ref, Seq, Skips}, State = #state{seq_ref = Ref}) ->
    #state{transport = T, socket = S, acked_seq = AckedTo, completed = Completed} = State,
    %% Worker pool has completed the put, check the completed
    %% list and work out where we can ack back to
    %case ack_to(AckedTo, ordsets:add_element(Seq, Completed)) of
    case ack_to_v3(AckedTo, insert_completed_v3(Seq, Skips, Completed)) of
        {AckedTo, Completed2} ->
            {noreply, State#state{completed = Completed2}};
        {AckTo, Completed2}  ->
            TcpIOL = riak_repl2_rtframe:encode(ack, AckTo),
            T:send(S, TcpIOL),
            {noreply, State#state{acked_seq = AckTo, completed = Completed2}}
    end;
handle_cast({ack_v3, Ref, Seq, _Skips}, State) ->
    %% Nothing to send, it's old news.
    lager:debug("Received ack ~p for previous sequence ~p\n", [Seq, Ref]),
    {noreply, State};

handle_cast({drop, BucketType}, #state{bt_timer = undefined, bt_interval = Interval} = State) ->
    {ok, Timer} = timer:send_after(Interval, report_bt_drops),
    {noreply, bt_dropped(BucketType, State#state{bt_timer = Timer})};

handle_cast({drop, BucketType}, State) ->
    {noreply, bt_dropped(BucketType, State)}.

%% tcp/ ssl data
handle_info({P, _S, TcpBin}, State) when P == tcp; P == ssl ->
    #state{cont = Cont} = State,
    Bin = <<Cont/binary, TcpBin/binary>>,
    recv(Bin, State);

%% tcp/ ssl socket closure
handle_info({P, _S}, State) when P == tcp_closed; P == ssl_closed->
    report_socket_close(State),
    {stop, normal, State};

%% tcp/ssl error
handle_info({E, _S, Reason}, State) when E == tcp_error; E == ssl_error ->
    report_socket_error(Reason, State),
    {stop, normal, State};

handle_info(reactivate_socket, State) ->
    #state{remote = Remote, transport = T, socket = S, max_pending = MaxPending} = State,
    case pending(State) > MaxPending of
        true ->
            {noreply, schedule_reactivate_socket_v3(State#state{active = false})};
        _ ->
            lager:debug("Realtime sink recovered - reactivating transport ~p socket ~p\n", [T, S]),
            %% Check the socket is ok
            case T:peername(S) of
                {ok, _} ->
                    T:setopts(S, [{active, once}]), % socket could die, pick it up on tcp_error msgs
                    {noreply, State#state{active = true}};
                {error, Reason} ->
                    riak_repl_stats:rt_sink_errors(),
                    lager:error("Realtime replication sink for ~p had socket error - ~p\n", [Remote, Reason]),
                    {stop, normal, State}
            end
    end;

handle_info(report_bt_drops, State=#state{bt_drops = DropDict}) ->
    Total = dict:fold(
        fun(BucketType, Counter, TotalCount) ->
          lager:error("drops due to missing or mismatched type ~p: ~p", [BucketType, Counter]),
          TotalCount + Counter
        end, 0, DropDict),
    lager:error("total bucket type drop count: ~p", [Total]),
    Report = app_helper:get_env(riak_repl, bucket_type_drop_report_interval, ?DEFAULT_INTERVAL_MILLIS),
    {noreply, State#state{bt_drops = dict:new(), bt_timer = undefined, bt_interval = Report}}.

terminate(_Reason, #state{helper = H, transport = T, socket = S}) ->
    %% Consider trying to do something graceful with poolboy?
    %% (in V4 this is no longer a consideration) - we do not use poolboy
    catch riak_repl2_rtsink_helper:stop(H),
    catch T:close(S),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ================================================================================================================== %%
%% Internal Functions
%% ================================================================================================================== %%
peername(Transport, Socket) ->
    case Transport:peername(Socket) of
        {ok, Res} ->
            Res;
        {error, Reason} ->
            riak_repl_stats:rt_sink_errors(),
            {lists:flatten(io_lib:format("error:~p", [Reason])), 0}
    end.

peername(#state{peername = Peername}) ->
    Peername.

bt_dropped(BucketType, #state{bt_drops = BucketDict} = State) ->
    State#state{bt_drops = dict:update_counter(BucketType, 1, BucketDict)}.


send_heartbeat(Transport, Socket) ->
    Transport:send(Socket, riak_repl2_rtframe:encode(heartbeat, undefined)).

%% Work out how many requests are pending (not writted yet, may not have been acked)
pending(#state{acked_seq = undefined}) ->
    0;
pending(#state{expect_seq = ExpSeq, acked_seq = AckedSeq, completed = Completed}) ->
    ExpSeq - AckedSeq - length(Completed) - 1.

%% ===================================== %%
%% Status
%% ===================================== %%

get_status(State) ->
    #state
    {
        remote = Remote,
        transport = T,
        helper = Helper,
        hb_last = HBLast,
        active = Active,
        deactivated = Deactivated,
        source_drops = SourceDrops,
        expect_seq = ExpSeq,
        acked_seq = AckedSeq
    } = State,

    {PoolboyState, PoolboyQueueLength, PoolboyOverflow, PoolboyMonitorsActive} = poolboy:status(riak_repl2_rtsink_pool),
    Pending = pending(State),
    SocketStats = riak_core_tcp_mon:socket_status(State#state.socket),
    [
        {source, Remote},
        {pid, riak_repl_util:safe_pid_to_list(self())},
        {connected, true},
        {transport, T},
        {socket, riak_core_tcp_mon:format_socket_stats(SocketStats,[])},
        {hb_last, HBLast},
        {helper, riak_repl_util:safe_pid_to_list(Helper)},
        {helper_msgq_len, riak_repl_util:safe_get_msg_q_len(Helper)},
        {active, Active},
        {deactivated, Deactivated},
        {source_drops, SourceDrops},
        {expect_seq, ExpSeq},
        {acked_seq, AckedSeq},
        {pending, Pending},
        {poolboy_state, PoolboyState},
        {poolboy_queue_length, PoolboyQueueLength},
        {poolboy_overflow, PoolboyOverflow},
        {poolboy_monitors_active, PoolboyMonitorsActive}
    ].
%% ================================================================================================================== %%
%% Handle Incoming Data
%% ================================================================================================================== %%
recv(TcpBin, State) ->
    #state{transport = T, socket = S} = State,
    case riak_repl2_rtframe:decode(TcpBin) of
        {ok, undefined, Cont} ->
            case State#state.active of
                true ->
                    T:setopts(S, [{active, once}]);
                _ ->
                    ok
            end,
            {noreply, State#state{cont = Cont}};

        {ok, heartbeat, Cont} ->
            send_heartbeat(T, S),
            recv(Cont, State#state{hb_last = os:timestamp()});

        {ok, {objects_and_meta, _} = Msg, Cont} ->
            write_data(Msg, Cont, State);

        {ok, {objects, _} = Msg, Cont} ->
            write_data(Msg, Cont, State)


    end.

write_data(Msg, Cont, State) ->
    #state{ write_data_function = WriteDataFunction} = State,
    case WriteDataFunction(Msg, State) of
        {ok, NewState} ->
            recv(Cont, NewState);
        {error, Error, NewState} ->
            {stop, Error, NewState}

    end.


report_socket_error(Reason, State = #state{cont = Cont}) ->
    %% peername not calculated from socket, so should be valid
    riak_repl_stats:rt_sink_errors(),
    lager:warning("Realtime connection from ~p network error ~p - ~b bytes pending\n",
        [peername(State), Reason, size(Cont)]).


report_socket_close(State = #state{cont = Cont}) ->
    case size(Cont) of
        0 ->
            ok;
        NumBytes ->
            riak_repl_stats:rt_sink_errors(),
            %% cached_peername not caclulated from socket, so should be valid
            lager:warning("Realtime connection from ~p closed with partial receive of ~b bytes\n",
                [peername(State), NumBytes])
    end.

%% ================================================================================================================== %%
%% Protocol 4
%% ================================================================================================================== %%
do_write_objects_v4({objects_and_meta, {Seq, BinObjs, Meta}}, State = #state{expected_seq_v4 = Seq}) ->
    #state{wire_version = WireVersion, helper = Helper} = State,

    case riak_repl_bucket_type_util:bucket_props_match(Meta) of
        true ->
            %% This function is now responsible for pushing onto the rtq, and acking back to this process
            riak_repl2_rtsink_helper:write_objects_v4(Helper, BinObjs, Meta, self(), WireVersion),
            {ok, State};
        false ->
            %% this has been changed from the previous protocol, we do not add this to our rtq anymore
            %% and we send a message to source to inform it of the drop
            BucketType = riak_repl_bucket_type_util:prop_get(?BT_META_TYPE, ?DEFAULT_BUCKET_TYPE, Meta),
            gen_server:cast(self(), {drop, BucketType}),
            send_bucket_type_drop(Seq, State),
            {ok, State#state{expected_seq_v4 = Seq +1}}
    end;
do_write_objects_v4({objects_and_meta, {Seq, _BinObjs, _Meta}}, State = #state{expected_seq_v4 = Seq2}) ->
    case Seq2 = Seq +1 of
        true ->
            %% send and ack back for this object as we have already placed it into the cluster
            send_ack(Seq, State),
            {ok, State};
        false ->
            %% we have received a seq that is not expected, nor the one before (for a possible retry)
            lager:error("Received wrong sequence number: ~p, Expected sequence number: ~p", [Seq, Seq2]),
            {error, wrong_seq, State}
    end.

%% standard send ack mechanism
send_ack(Seq, #state{transport = T, socket = S}) ->
    TcpIOL = riak_repl2_rtframe:encode(ack, Seq),
    T:send(S, TcpIOL).

%% new mechanism to inform the source that the object has been dropped due to mis-match of bucket-type properties
%% the source will stop sending this bucket-type until its been resolved
send_bucket_type_drop(Seq, #state{transport = T, socket = S}) ->
    TcpIOL = riak_repl2_rtframe:encode(bt_drop, Seq),
    T:send(S, TcpIOL).

%% ================================================================================================================== %%
%% Protocol >= 3 - legacy (this will be deleted in the future)
%% ================================================================================================================== %%

%% ===================================== %%
%% Writing The Object
%% ===================================== %%

%% latest repl version with metadata
do_write_objects_v3({objects_and_meta, {Seq, BinObjs, Meta}}, State = #state{expect_seq = Seq}) ->
    Me = self(),
    #state{helper = Helper, wire_version = Ver, seq_ref = Ref} = State,
    DoneFun =
        fun(ObjectFilteringRules) ->
            Skips = orddict:fetch(skip_count, Meta),
            gen_server:cast(Me, {ack_v3, Ref, Seq, Skips}),
            maybe_push(BinObjs, Meta, ObjectFilteringRules)
        end,

    case riak_repl_bucket_type_util:bucket_props_match(Meta) of
        true ->
            riak_repl2_rtsink_helper:write_objects_v3(Helper, BinObjs, DoneFun, Ver);
        false ->
            BucketType = riak_repl_bucket_type_util:prop_get(?BT_META_TYPE, ?DEFAULT_BUCKET_TYPE, Meta),
            lager:debug("Bucket type:~p is not equal on both the source and sink; not writing object.",
                [BucketType]),
            gen_server:cast(Me, {drop, BucketType}),
            Objects = riak_repl_util:from_wire(Ver, BinObjs),
            ObjectFilteringRules = [riak_repl2_object_filter:get_realtime_blacklist(Obj) || Obj <- Objects],
            DoneFun(ObjectFilteringRules)
    end,
    State1 = set_seq_number_v3(Seq, State),
    State2 = set_socket_check_pending_v3(State1),
    {ok, State2};

%% old repl version > 1.4 (pre metadata)
do_write_objects_v3({objects, {Seq, BinObjs}}, State = #state{expect_seq = Seq}) ->
    Me = self(),
    #state{helper = Helper, wire_version = Ver, seq_ref = Ref} = State,
    DoneFun =
        fun(_ObjectFiltering) ->
            gen_server:cast(Me, {ack_v3, Ref, Seq, 0})
        end,
    riak_repl2_rtsink_helper:write_objects_v3(Helper, BinObjs, DoneFun, Ver),
    State1 = set_seq_number_v3(Seq, State),
    State2 = set_socket_check_pending_v3(State1),
    {ok, State2};

%% did not get expected sequence number
do_write_objects_v3({objects, {Seq, _}} = Msg, State) ->
    State2 = reset_ref_seq_v3(Seq, State),
    do_write_objects_v3(Msg, State2);
do_write_objects_v3({objects_and_meta, {Seq, _, _}} = Msg, State) ->
    State2 = reset_ref_seq_v3(Seq, State),
    do_write_objects_v3(Msg, State2).


set_seq_number_v3(Seq, State = #state{acked_seq = AckedSeq}) ->
    case AckedSeq of
        undefined ->
            %% Handle first received sequence number
            State#state{acked_seq = Seq - 1, expect_seq = Seq + 1};
        _ ->
            State#state{expect_seq = Seq + 1}
    end.

set_socket_check_pending_v3(State = #state{max_pending = MaxPending}) ->
    %% If the socket is too backed up, take a breather
    %% by setting {active, false} then casting a message to ourselves
    %% to enable it once under the limit
    case pending(State) > MaxPending of
        true ->
            schedule_reactivate_socket_v3(State);
        _ ->
            State
    end.


maybe_push(Binary, Meta, ObjectFilteringRules) ->
    case app_helper:get_env(riak_repl, realtime_cascades, always) of
        never ->
            lager:debug("Skipping cascade due to app env setting"),
            ok;
        always ->
            lager:debug("app env either set to always, or in default; doing cascade"),
            List = riak_repl_util:from_wire(Binary),
            Meta2 = orddict:erase(skip_count, Meta),
            Meta3 = add_object_filtering_blacklist_to_meta(Meta2, ObjectFilteringRules),
            riak_repl2_rtq:push(length(List), Binary, Meta3)
    end.

add_object_filtering_blacklist_to_meta(Meta, []) ->
    lager:error("object filtering rules failed to return the default"),
    Meta;
add_object_filtering_blacklist_to_meta(Meta, [Rules]) ->
    orddict:store(?BT_META_BLACKLIST, Rules, Meta);
add_object_filtering_blacklist_to_meta(Meta, [_Rules | _Rest]) ->
    lager:error("object filtering, repl binary has more than one object!"),
    Meta.


%% ===================================== %%
%% Acking The Sequence Number
%% ===================================== %%
%% Work out the highest sequence number that can be acked and return it,
%% completed always has one or more elements on first call.
ack_to_v3(Acked, []) ->
    {Acked, []};
ack_to_v3(Acked, [LessThanAck | _] = Completed) when LessThanAck =< Acked ->
    ack_to_v3(LessThanAck - 1, Completed);
ack_to_v3(Acked, [Seq | Completed2] = Completed) ->
    case Acked + 1 of
        Seq ->
            ack_to_v3(Seq, Completed2);
        _ ->
            {Acked, Completed}
    end.

insert_completed_v3(Seq, Skipped, Completed) ->
    Foldfun = fun(N, Acc) -> ordsets:add_element(N, Acc) end,
    lists:foldl(Foldfun, Completed, lists:seq(Seq - Skipped, Seq)).

reset_ref_seq_v3(Seq, State = #state{source_drops = SourceDrops, expect_seq = ExpSeq}) ->
    NewSeqRef = make_ref(),
    SourceDrops2 =
        case ExpSeq of
            undefined -> % no need to tell user about first time through
                SourceDrops;
            _ ->
                SourceDrops + Seq - ExpSeq
        end,
    State#state
    {
        seq_ref = NewSeqRef,
        expect_seq = Seq,
        acked_seq = Seq - 1,
        completed = [],
        source_drops = SourceDrops2
    }.

%% ===================================== %%
%% Activating The Socket
%% ===================================== %%
schedule_reactivate_socket_v3(State = #state{transport = T, socket = S, active = Active, deactivated = Deactivated}) ->
    case Active of
        true ->
            lager:debug("Realtime sink overloaded - deactivating transport ~p socket ~p\n", [T, S]),
            T:setopts(S, [{active, false}]),
            self() ! reactivate_socket,
            State#state{active = false, deactivated = Deactivated + 1};
        false ->
            %% already deactivated, try again in configured interval, or default
            ReactivateSockInt = get_reactivate_socket_interval_v3(),
            lager:debug("reactivate_socket_interval_millis: ~sms.", [ReactivateSockInt]),

            erlang:send_after(ReactivateSockInt, self(), reactivate_socket),
            State#state{active = {false, scheduled}};
        {false, scheduled} ->
            %% have a check scheduled already
            State
    end.

get_reactivate_socket_interval_v3() ->
    app_helper:get_env(riak_repl, reactivate_socket_interval_millis, ?REACTIVATE_SOCK_INT_MILLIS).






%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(SINK_PORT, 5008).
-define(LOOPBACK_TEST_PEER, {127,0,0,1}).
-define(VER1, {1,0}).
-define(PROTOCOL(NegotiatedVer), {realtime, NegotiatedVer, NegotiatedVer}).
-define(PROTOCOL_V1, ?PROTOCOL(?VER1)).
-define(REACTIVATE_SOCK_INT_MILLIS_TEST_VAL, 20).
-define(PORT_RANGE, 999999).

-compile(export_all).

riak_repl2_rtsink_conn_test_() ->
    {spawn,
     [
      {setup,
        fun setup/0,
        fun cleanup/1,
        [
         fun cache_peername_test_case/0,
         fun reactivate_socket_interval_test_case/0
        ]
      }]}.

setup() ->
    riak_repl_test_util:start_test_ring(),
    riak_repl_test_util:abstract_gen_tcp(),
    riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    {ok, _RT} = riak_repl2_rt:start_link(),
    riak_repl_test_util:kill_and_wait(riak_repl2_rtq),
    {ok, _} = riak_repl2_rtq:start_link(),
    application:set_env(riak_repl, realtime_connection_rebalance_max_delay_secs, 1000),

    catch(meck:unload(riak_core_connection_mgr)),
    meck:new(riak_core_connection_mgr, [passthrough]),
    meck:expect(riak_core_connection_mgr, disconnect,
        fun(_Remote) ->
            ok
        end),

    catch(meck:unload(riak_repl2_rtsource_conn_data_mgr)),
    meck:new(riak_repl2_rtsource_conn_data_mgr, [passthrough]),
    meck:expect(riak_repl2_rtsource_conn_data_mgr, read, fun(active_nodes) -> [node()]
                                                         end),
    meck:expect(riak_repl2_rtsource_conn_data_mgr, read,
        fun(realtime_connections, _Remote) ->
            dict:new()
        end
    ),
    meck:expect(riak_repl2_rtsource_conn_data_mgr, write, 4,
        fun(_, _, _, _) ->
            ok
        end
    ),

    catch(meck:unload(riak_core_cluster_mgr)),
    meck:new(riak_core_cluster_mgr, [passthrough]),
    meck:expect(riak_core_cluster_mgr, get_unshuffled_ipaddrs_of_cluster, fun(_Remote) -> [] end ),
    meck:expect(riak_core_cluster_mgr, get_ipaddrs_of_cluster, fun(_) -> {ok,[]} end ),
    meck:expect(riak_core_cluster_mgr, get_ipaddrs_of_cluster, fun(_, split) -> {ok, {[],[]}} end ),
    meck:expect(riak_core_cluster_mgr, get_ipaddrs_of_cluster, fun(_, _) -> {ok,[]} end ),

    catch(meck:unload(riak_core_capability)),
    meck:new(riak_core_capability, [passthrough]),
    meck:expect(riak_core_capability, get, 2, fun(_, _) -> v1 end),

    catch(meck:unload(riak_repl_util)),
    meck:new(riak_repl_util, [passthrough]),
    meck:expect(riak_repl_util, generate_socket_tag, fun(Prefix, _Transport, _Socket) ->
        random:seed(now()),
        Portnum = random:uniform(?PORT_RANGE),
        lists:flatten(io_lib:format("~s_~p -> ~p:~p",[
            Prefix,
            Portnum,
            ?LOOPBACK_TEST_PEER,
            ?SINK_PORT]))
                                                     end),

    catch(meck:unload(riak_core_tcp_mon)),
    meck:new(riak_core_tcp_mon, [passthrough]),
    meck:expect(riak_core_tcp_mon, monitor, 3, fun(Socket, _Tag, Transport) ->
        {reply, ok,  #state{transport=Transport, socket=Socket}}
                                               end),

    folsom:start(),

    ok.

cleanup(_Ctx) ->
    process_flag(trap_exit, false),
    riak_repl_test_util:kill_and_wait(riak_core_tcp_mon),
    riak_repl_test_util:kill_and_wait(riak_repl2_rtq),
    riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    riak_repl_test_util:kill_and_wait(riak_repl2_rtsource_conn_mgr),
    riak_repl_test_util:stop_test_ring(),
    riak_repl_test_util:maybe_unload_mecks(
      [riak_core_service_mgr,
        riak_repl2_rtsource_conn_data_mgr,
        riak_core_connection_mgr,
       riak_repl_util,
       riak_core_tcp_mon,
       gen_tcp]),
    meck:unload(),
    ok.

%% test for https://github.com/basho/riak_repl/issues/247
%% cache the peername so that when the local socket is closed
%% peername will still be around for logging
cache_peername_test_case() ->

    TellMe = self(),

    catch(meck:unload(riak_core_service_mgr)),
    meck:new(riak_core_service_mgr, [passthrough]),
    meck:expect(riak_core_service_mgr, sync_register_service, fun(HostSpec, _Strategy) ->
        {_Proto, {TcpOpts, _Module, _StartCB, _CBArg}} = HostSpec,
        {ok, Listen} = gen_tcp:listen(?SINK_PORT, [binary, {reuseaddr, true} | TcpOpts]),
        TellMe ! sink_listening,
        {ok, Socket} = gen_tcp:accept(Listen),
        {ok, Pid} = riak_repl2_rtsink_conn:start_link({ok, ?PROTOCOL(?VER1)}, "source_cluster"),

        ok = gen_tcp:controlling_process(Socket, Pid),
        ok = riak_repl2_rtsink_conn:set_socket(Pid, Socket, gen_tcp),

        % Socket is set, close it to simulate error
        inet:close(Socket),

        % grab the State from the rtsink_conn process
        {status,Pid,_,[_,_,_,_,[_,_,{data,[{_,State}]}]]} = sys:get_status(Pid),

        % check to make sure peername is cached, not calculated from (now closed) Socket
        ?assertMatch({?LOOPBACK_TEST_PEER, _Port}, peername(State)),

        TellMe ! {sink_started, Pid}
    end),

    {ok, _SinkPid} = start_sink(),
    {ok, {_Source, _Sink}} = start_source(?VER1).

% test case for https://github.com/basho/riak_repl/issues/252
reactivate_socket_interval_test_case() ->
    ?assertEqual(?REACTIVATE_SOCK_INT_MILLIS, get_reactivate_socket_interval()),

    application:set_env(riak_repl, reactivate_socket_interval_millis, ?REACTIVATE_SOCK_INT_MILLIS_TEST_VAL),
    ?assertEqual(?REACTIVATE_SOCK_INT_MILLIS_TEST_VAL, get_reactivate_socket_interval()).

listen_sink() ->
    riak_repl2_rtsink_conn:sync_register_service().

start_source() ->
    start_source(?VER1).

start_source(NegotiatedVer) ->
    meck:expect(riak_core_connection_mgr, connect, fun(_ServiceAndRemote, ClientSpec, _Strategy) ->
        spawn_link(fun() ->
            {_Proto, {TcpOpts, Module, Pid}} = ClientSpec,
            {ok, Socket} = gen_tcp:connect("localhost", ?SINK_PORT, [binary | TcpOpts]),
            ok = Module:connected(Socket, gen_tcp, {"localhost", ?SINK_PORT},
              ?PROTOCOL(NegotiatedVer), Pid, [], true)
        end),
        {ok, make_ref()}
    end),


    {ok, SourcePid} = riak_repl2_rtsource_conn_mgr:start_link("sink_cluster"),
    receive
        {sink_started, SinkPid} ->
            {ok, {SourcePid, SinkPid}}
    after 1000 ->
        {error, timeout}
    end.

start_sink() ->

    Pid = proc_lib:spawn_link(?MODULE, listen_sink, []),
    receive
        sink_listening ->
            {ok, Pid}
    after 10000 ->
            {error, timeout}
    end.
unload_mecks() ->
    riak_repl_test_util:maybe_unload_mecks([
        stateful, riak_core_ring_manager, riak_core_ring,
        riak_repl2_rtsink_helper, gen_tcp, fake_source, riak_repl2_rtq,
        riak_core_capability, riak_repl2_rtsource_conn_data_mgr]).


-endif.
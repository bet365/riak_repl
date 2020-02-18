%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsource_helper).

%% @doc Realtime replication source helper
%%
%% High level responsibility...

-behaviour(gen_server).
%% API
-export(
[
    start_link/5,
    stop/1,
    v1_ack/2,
    status/1,
    status/2,
    send_heartbeat/1,
    send_object/2
]).

-include("riak_repl.hrl").

-define(SERVER, ?MODULE).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-record(state, {remote,     % remote site name
                transport,  % erlang module to use for transport
                socket,     % socket to pass to transport
                proto,      % protocol version negotiated
                sent_seq,   % last sequence sent
                v1_offset = 0,
                v1_seq_map = [],
                objects = 0, % number of objects sent - really number of pulls as could be multiobj
                ack_ref
}).

start_link(Remote, Transport, Socket, Version, AckRef) ->
    gen_server:start_link(?MODULE, [Remote, Transport, Socket, Version, AckRef], []).

stop(Pid) ->
    gen_server:call(Pid, stop, ?LONG_TIMEOUT).

%% @doc v1 sinks require fully sequential sequence numbers sent. The outgoing
%% Seq's are munged, and thus must be munged back when the sink replies.
v1_ack(Pid, Seq) ->
    gen_server:cast(Pid, {v1_ack, Seq}).

status(Pid) ->
    status(Pid, app_helper:get_env(riak_repl, riak_repl2_rtsource_helper_status_to, ?LONG_TIMEOUT)).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

send_heartbeat(Pid) ->
    %% Cast the heartbeat, do not want to block the rtsource process
    %% as it is responsible for checking heartbeat
    gen_server:cast(Pid, send_heartbeat).

send_object(Pid, Obj) ->
    gen_server:call(Pid, {send_object, Obj}, ?LONG_TIMEOUT).


init([Remote, Transport, Socket, Version, AckRef]) ->
    State = #state{remote = Remote, transport = Transport, proto = Version,
        socket = Socket, ack_ref = AckRef},
    riak_repl2_reference_rtq:register(Remote, AckRef),
    {ok, State}.

handle_call({send_object, {Seq, NumObjects, _BinObjs, _Meta} = Entry}, From,
    State = #state{transport = T, socket = S, objects = Objects}) ->
    State2 = maybe_send(T, S, Entry, From, State),
    {noreply, State2#state{sent_seq = Seq, objects = Objects + NumObjects}};

handle_call(stop, _From, State) ->
    {stop, {shutdown, routine}, ok, State};

handle_call(status, _From, State =
    #state{sent_seq = SentSeq, objects = Objects}) ->
    {reply, [{sent_seq, SentSeq},
        {objects, Objects}], State}.

handle_cast(send_heartbeat, State = #state{transport = T, socket = S}) ->
    spawn(fun() ->
        HBIOL = riak_repl2_rtframe:encode(heartbeat, undefined),
        T:send(S, HBIOL)
          end),
    {noreply, State};

handle_cast({v1_ack, Seq}, State = #state{v1_seq_map = Map}) ->
    case orddict:find(Seq, Map) of
        error ->
            ok;
        {ok, RealSeq} ->
            riak_repl2_rtq:ack(State#state.remote, RealSeq)
    end,
    Map2 = orddict:erase(Seq, Map),
    {noreply, State#state{v1_seq_map = Map2}};

handle_cast(Msg, _State) ->
    lager:info("Realtime source helper received unexpected cast - ~p", [Msg]).


handle_info(Msg, State) ->
    lager:info("Realtime source helper received unexpected message - ~p", [Msg]),
    {noreply, State}.

terminate(Reason, _State) ->
    lager:info("rtsource conn helper terminated due to ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




maybe_send(Transport, Socket, QEntry, From, State) ->
    #state{sent_seq = Seq, remote = Remote} = State,
    Seq2 = Seq +1,
    QEntry2 = setelement(1, QEntry, Seq2),
    {Seq2, _NumObjects, _BinObjs, Meta} = QEntry2,
    case State#state.proto of
        {Major, _Minor} when Major >= 3 ->
            encode_and_send(QEntry2, Remote, Transport, Socket, State);
        _ ->
            case riak_repl_bucket_type_util:is_bucket_typed(Meta) of
                false ->
                    %% unblock the rtq as fast as possible
                    gen_server:reply(From, {ok, Seq2}),
                    encode_and_send(QEntry2, Remote, Transport, Socket, State);
                true ->
                    %% unblock the rtq as fast as possible
                    gen_server:reply(From, bucket_type_not_supported_by_remote),
                    lager:debug("Negotiated protocol version:~p does not support typed buckets, not sending"),
                    State
            end
    end.

encode_and_send(QEntry, Remote, Transport, Socket, State) ->
    QEntry2 = merge_forwards_and_routed_meta(QEntry, Remote),
    {Seq, _, _, _} = QEntry2,
    {Encoded, State2} = encode(QEntry2, State),
    lager:debug("Forwarding to ~p with new data: ~p derived from ~p", [State#state.remote, QEntry2, QEntry]),
    Transport:send(Socket, Encoded),
    State2#state{sent_seq = Seq}.


encode({Seq, _NumObjs, BinObjs, Meta}, State = #state{proto = Ver}) when Ver < {2,0} ->
    Skips = orddict:fetch(skip_count, Meta),
    Offset = State#state.v1_offset + Skips,
    Seq2 = Seq - Offset,
    V1Map = orddict:store(Seq2, Seq, State#state.v1_seq_map),
    BinObjs2 = riak_repl_util:maybe_downconvert_binary_objs(BinObjs, w0),
    Encoded = riak_repl2_rtframe:encode(objects, {Seq2, BinObjs2}),
    State2 = State#state{v1_offset = Offset, v1_seq_map = V1Map},
    {Encoded, State2};
encode({Seq, _NumbOjbs, BinObjs, Meta}, State = #state{proto = Ver}) when Ver >= {2,0} ->
    {riak_repl2_rtframe:encode(objects_and_meta, {Seq, BinObjs, Meta}), State}.


meta_get(Key, Default, Meta) ->
    case orddict:find(Key, Meta) of
        error -> Default;
        {ok, Value} -> Value
    end.

merge_forwards_and_routed_meta({_, _, _, Meta} = QEntry, Remote) ->
    LocalForwards = meta_get(local_forwards, [Remote], Meta),
    Routed = meta_get(routed_clusters, [], Meta),
    Self = riak_core_connection:symbolic_clustername(),
    Meta2 = orddict:erase(local_forwards, Meta),
    Routed2 = lists:usort(Routed ++ LocalForwards ++ [Self]),
    Meta3 = orddict:store(routed_clusters, Routed2, Meta2),
    Meta4 = orddict:erase(?BT_META_BLACKLIST, Meta3),
    setelement(4, QEntry, Meta4).


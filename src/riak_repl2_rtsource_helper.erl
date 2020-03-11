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
    start_link/6,
    stop/1,
    status/1,
    status/2,
    send_heartbeat/1,
    send_object/2,
    shutting_down/1
]).

-include("riak_repl.hrl").

-define(SERVER, ?MODULE).
-define(SHORT_TIMEOUT, 1000).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-record(state, {remote,     % remote site name
                transport,  % erlang module to use for transport
                socket,     % socket to pass to transport
                proto,      % protocol version negotiated
                sent_seq = 0,   % last sequence sent
                objects = 0, % number of objects sent - really number of pulls as could be multiobj
                ref,
                rtsource_conn_pid,
                shutting_down = false
}).

start_link(Remote, Transport, Socket, Version, RTQRef, RtsourceConnPid) ->
    gen_server:start_link(?MODULE, [Remote, Transport, Socket, Version, RTQRef, RtsourceConnPid], []).

stop(Pid) ->
    gen_server:call(Pid, stop, ?LONG_TIMEOUT).

status(Pid) ->
    status(Pid, app_helper:get_env(riak_repl, riak_repl2_rtsource_helper_status_to, ?LONG_TIMEOUT)).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

send_heartbeat(Pid) ->
    %% Cast the heartbeat, do not want to block the rtsource process
    %% as it is responsible for checking heartbeat
    gen_server:cast(Pid, send_heartbeat).

send_object(Pid, Obj) ->
    gen_server:call(Pid, {send_object, Obj}, ?SHORT_TIMEOUT).

shutting_down(Pid) ->
    gen_server:call(Pid, shutting_down, infinity).


init([Remote, Transport, Socket, Version, Ref, RtsourceConnPid]) ->
    State = #state{remote = Remote, transport = Transport, proto = Version,
        socket = Socket, ref = Ref, rtsource_conn_pid = RtsourceConnPid},
    riak_repl2_reference_rtq:register(Remote, Ref),
    {ok, State}.

handle_call({send_object, Entry}, From,
    State = #state{transport = T, socket = S}) ->
    maybe_send(T, S, Entry, From, State);

handle_call(shutting_down, _From, State = #state{sent_seq = Seq}) ->
    {reply, {ok, Seq}, State#state{shutting_down = true}};

handle_call(stop, _From, State) ->
    {stop, {shutdown, routine}, ok, State};

handle_call(status, _From, State =
    #state{sent_seq = SentSeq, objects = Objects}) ->
    {reply, [{sent_seq, SentSeq}, {objects, Objects}], State}.

%% TODO: does this need to be spawned?
handle_cast(send_heartbeat, State = #state{transport = T, socket = S}) ->
    spawn(fun() -> HBIOL = riak_repl2_rtframe:encode(heartbeat, undefined), T:send(S, HBIOL) end),
    {noreply, State};

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




maybe_send(_Transport, _Socket, _QEntry, From, State = #state{shutting_down = true}) ->
    gen_server:reply(From, shutting_down),
    %% now we are out of the reference_rtq we can shutdown gracefully
    State;
maybe_send(Transport, Socket, QEntry, From, State) ->
    #state
    {
        sent_seq = Seq,
        remote = Remote,
        proto = {Major, _},
        rtsource_conn_pid = _RtsourceConnPid
    } = State,
    Seq2 = Seq +1,
    QEntry2 = setelement(1, QEntry, Seq2),
    {Seq2, _NumObjects, _BinObjs, Meta} = QEntry2,
    case Major of
        4 ->
            %% unblock the rtq as fast as possible
            gen_server:reply(From, {ok, Seq2}),
            %% send rtsource_conn message to know to expect the seq number
%%            RtsourceConnPid ! object_sent,
            %% send object to sink
            {noreply, encode_and_send(QEntry2, Remote, Transport, Socket, State)};
        3 ->
            %% unblock the reference rtq as fast as possible
            gen_server:reply(From, {ok, Seq2}),
            {noreply, encode_and_send(QEntry2, Remote, Transport, Socket, State)};
        _ ->
            case riak_repl_bucket_type_util:is_bucket_typed(Meta) of
                false ->
                    %% unblock the reference rtq as fast as possible
                    gen_server:reply(From, {ok, Seq2}),
                    {noreply, encode_and_send(QEntry2, Remote, Transport, Socket, State)};
                true ->
                    %% unblock the reference rtq as fast as possible
                    gen_server:reply(From, bucket_type_not_supported_by_remote),
                    lager:debug("Negotiated protocol version:~p does not support typed buckets, not sending"),
                    {noreply, State}
            end
    end.

encode_and_send(QEntry, Remote, Transport, Socket, State = #state{objects = Objects}) ->
    QEntry2 = merge_forwards_and_routed_meta(QEntry, Remote),
    {Encoded, State2} = encode(QEntry2, State),
    Transport:send(Socket, Encoded),
    {Seq, NumObjects, _, _} = QEntry2,
    State2#state{sent_seq = Seq, objects = Objects + NumObjects}.


encode({Seq, _NumObjs, BinObjs, _Meta}, State = #state{proto = Ver}) when Ver < {2,0} ->
    BinObjs2 = riak_repl_util:maybe_downconvert_binary_objs(BinObjs, w0),
    Encoded = riak_repl2_rtframe:encode(objects, {Seq, BinObjs2}),
    {Encoded, State};

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


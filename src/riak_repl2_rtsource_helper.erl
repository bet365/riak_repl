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
    start_link/7,
    stop/1,
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

-record(state,
{
    id,
    remote,     % remote site name
    transport,  % erlang module to use for transport
    socket,     % socket to pass to transport
    proto,      % protocol version negotiated
    sent_seq = 0,   % last sequence sent
    ref,
    rtsource_conn_pid,
    shutting_down = false
}).

start_link(Remote, Id, Transport, Socket, Version, RTQRef, RTPid) ->
    gen_server:start_link(?MODULE, [Remote, Id, Transport, Socket, Version, RTQRef, RTPid], []).

stop(Pid) ->
    gen_server:call(Pid, stop, ?LONG_TIMEOUT).

send_heartbeat(Pid) ->
    %% Cast the heartbeat, do not want to block the rtsource process
    %% as it is responsible for checking heartbeat
    gen_server:cast(Pid, send_heartbeat).

send_object(Pid, Obj) ->
    gen_server:call(Pid, {send_object, Obj}, ?SHORT_TIMEOUT).

shutting_down(Pid) ->
    gen_server:call(Pid, shutting_down, infinity).


init([Remote, Id, Transport, Socket, Version, Ref, RTPid]) ->
    State = #state{id = Id, remote = Remote, transport = Transport, proto = Version,
        socket = Socket, ref = Ref, rtsource_conn_pid = RTPid},
    riak_repl2_reference_rtq:register(Remote, Id, Ref),
    {ok, State}.

handle_call({send_object, Entry}, From, State) ->
    maybe_send(Entry, From, State);

handle_call(shutting_down, _From, State = #state{sent_seq = Seq}) ->
    {reply, {ok, Seq}, State#state{shutting_down = true}};

handle_call(stop, _From, State) ->
    {stop, {shutdown, routine}, ok, State}.

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


maybe_send(_QEntry, From, State = #state{shutting_down = true}) ->
    gen_server:reply(From, shutting_down),
    %% now we are out of the reference_rtq we can shutdown gracefully
    State;
maybe_send(QEntry, From, State) ->
    #state
    {
        sent_seq = Seq,
        remote = Remote,
        proto = {Major, _}
    } = State,
    Seq2 = Seq +1,
    QEntry2 = setelement(1, QEntry, Seq2),
    {Seq2, _NumObjects, _BinObjs, Meta} = QEntry2,
    case Major > 2 of
        true ->
            %% unblock the rtq as fast as possible
            gen_server:reply(From, {ok, Seq2}),
            encode_and_send(Seq2, QEntry2, Remote,State);
        false ->
            case riak_repl_bucket_type_util:is_bucket_typed(Meta) of
                false ->
                    %% unblock the reference rtq as fast as possible
                    gen_server:reply(From, {ok, Seq2}),
                    encode_and_send(Seq2, QEntry2, Remote, State);
                true ->
                    %% unblock the reference rtq as fast as possible
                    gen_server:reply(From, bucket_type_not_supported_by_remote),
                    lager:debug("Negotiated protocol version:~p does not support typed buckets, not sending"),
                    {noreply, State}
            end
    end.

encode_and_send(Seq2, QEntry, Remote, State = #state{transport = T, socket = S, rtsource_conn_pid = RTPid}) ->
    QEntry2 = merge_forwards_and_routed_meta(QEntry, Remote),
    {Encoded, State2} = encode(QEntry2, State),
    State3 = State2#state{sent_seq = Seq2},

    %% crash if this fails, we will just re-connect via conn_mgr
    ok = riak_repl2_rtsource_conn:object_sent(RTPid, os:timestamp()),

    case T:send(S, Encoded) of
        ok ->
            {noreply, State3};
        {error, Reason} ->
            {stop, {error, Reason}, State3}
    end.


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

%% Riak Replication Realtime Migration manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_repl_migration).

-behaviour(gen_server).

%% API
-export([start_link/0,migrate_queue/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {elapsed_sleep,
               caller}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

migrate_queue() ->
    gen_server:call(?SERVER, migrate_queue, infinity).


init([]) ->
    lager:info("Riak replication migration server started"),
    {ok, #state{elapsed_sleep=0}}.

handle_call(migrate_queue, From, State) ->
    %% give the reference queue's a chance to clear themselves down
    DefaultTimeout = app_helper:get_env(riak_repl, queue_migration_timeout, 5),
    Time = DefaultTimeout * 1000,
    erlang:send(self(), {check_queue, Time}),
    {noreply, State#state{caller = From, elapsed_sleep = 0}}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({check_queue, Time}, State = #state{elapsed_sleep = ElapsedSleep}) ->
    case riak_repl2_rtq_sup:is_empty() of
        true ->
            gen_server:reply(State#state.caller, ok),
            lager:info("Queue empty, no replication queue migration required"),
            {noreply, State};
        false ->
            case ElapsedSleep >= Time of
                true ->
                    lager:info("Realtime queue has not completely drained"),
                    %% issue shutdown to all reference queues (we are about to migrate them)
                    riak_repl2_reference_rtq_sup:shutdown(),
                    erlang:send(self(), migrate_queue),
                    {noreply, State};
                false ->
                    lager:info("Waiting for realtime repl queue to drain"),
                    erlang:send_after(1000, self(), {check_queue, Time}),
                    {noreply, State#state{elapsed_sleep = ElapsedSleep + 1000}}
            end

    end;

handle_info(migrate_queue, State) ->
    _ = case riak_repl_util:get_peer_repl_nodes() of
            [] ->
                lager:error("No nodes available to migrate replication data"),
                riak_repl_stats:rt_source_errors(),
                error;
            [Peer|_Rest] ->
                WireVer = riak_repl_util:peer_wire_format(Peer),
                lager:info("Migrating replication queue data to ~p with wire version ~p", [Peer, WireVer]),
                drain_queue(Peer, WireVer),
                lager:info("Done migrating replication queue"),
                ok
        end,
    gen_server:reply(State#state.caller, ok),
    {noreply, State}.





%% Drain the realtime queue and push all objects into a Peer node's input RT queue.
%% If the handoff node does not understand the new repl wire format, then we need
%% to downconvert the items into the old "w0" format, otherwise the other node will
%% send an unsupported object format to its eventual sink node. This is painful to
%% trace back to here.
drain_queue(Peer, PeerWireVer) ->
    Concurrency = app_helper:get_env(riak_repl, rtq_concurrency, erlang:system_info(schedulers)),
    drain_queue(Peer, PeerWireVer, Concurrency).

drain_queue(_Peer, _PeerWireVer, 0) ->
    ok;
drain_queue(Peer, PeerWireVer, Id) ->
    case riak_repl2_rtq:drain_queue(Id) of
        empty ->
            drain_queue(PeerWireVer, PeerWireVer, Id -1);
        QEntry ->
            migrate_single_object(QEntry, PeerWireVer, Peer),
            drain_queue(Peer, PeerWireVer)
    end.


migrate_single_object({_Seq, NumItem, W1BinObjs, Meta, Completed}, PeerWireVer, Peer) ->
        try
            BinObjs = riak_repl_util:maybe_downconvert_binary_objs(W1BinObjs, PeerWireVer),
            CastObj =
                case PeerWireVer of
                    w0 ->
                      {push, NumItem, BinObjs};
                    w1 ->
                          case riak_core_capability:get({riak_repl, rtq_completed_list}, false) of
                              false ->
                                  %% remove data from meta
                                  {push, NumItem, BinObjs, Meta};
                              true ->
                                  {push, NumItem, BinObjs, Meta, Completed}
                          end
                  end,
            gen_server:cast({riak_repl2_rtq, Peer}, CastObj)
        catch
            _:_ ->
                % probably too much spam in the logs for this warning
                %lager:warning("Dropped object during replication queue migration"),
                % is this the correct stat?
                riak_repl_stats:objects_dropped_no_clients(),
                riak_repl_stats:rt_source_errors()
        end.




terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

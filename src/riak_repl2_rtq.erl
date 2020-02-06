%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.

%% @doc Queue module for realtime replication.
%%
%% The queue strives to reliably pass on realtime replication, with the
%% aim of reducing the need to fullsync.  Every item in the queue is
%% given a sequence number when pushed.  Consumers register with the
%% queue, then pull passing in a function to receive items (executed
%% on the queue process - it can cast/! as it desires).
%%
%% Once the consumer has delievered the item, it must ack the queue
%% with the sequence number.  If multiple deliveries have taken
%% place an ack of the highest seq number acknowledge all previous.
%%
%% The queue is currently stored in a private ETS table.  Once
%% all consumers are done with an item it is removed from the table.
-module(riak_repl2_rtq).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export(
[
    start_link/0,
    start_link/1,
    register/1,

    unregister/1,
    push/3,
    push/2,
    ack/2,
    status/0,
    is_empty/1,
    all_queues_empty/0,
    shutdown/0,
    stop/0,
    is_running/0
]).

% private api
-export([report_drops/1]).
-export([start_test/0]).

-define(overload_ets, rtq_overload_ets).
-define(SERVER, ?MODULE).
-define(DEFAULT_OVERLOAD, 2000).
-define(DEFAULT_RECOVER, 1000).
-define(DEFAULT_RTQ_LATENCY_SLIDING_WINDOW, 300).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {qtab = ets:new(?MODULE, [protected, ordered_set]), % ETS table
                qseq = 0,  % Last sequence number handed out
                default_max_bytes = undefined, % maximum ETS table memory usage in bytes
                overload = ?DEFAULT_OVERLOAD :: pos_integer(), % if the message q exceeds this, the rtq is overloaded
                recover = ?DEFAULT_RECOVER :: pos_integer(), % if the rtq is in overload mode, it does not recover until =<
                overloaded = false :: boolean(),
                overload_drops = 0 :: non_neg_integer(),
                remotes = [],
                shutting_down=false,
                qsize_bytes = 0,
                word_size=erlang:system_info(wordsize),
                all_remote_names = []
               }).

-record(remote,
{
    pid,
    name,
    total_skipped = 0,
    total_filtered = 0,
    total_acked = 0,
    total_drops = 0,
    rsize_bytes = 0,
    max_ack = 0
}).

-type name() :: term().
-type seq() :: non_neg_integer().

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    Overload = app_helper:get_env(riak_repl, rtq_overload_threshold, ?DEFAULT_OVERLOAD),
    Recover = app_helper:get_env(riak_repl, rtq_overload_recover, ?DEFAULT_RECOVER),
    Opts = [{overload_threshold, Overload}, {overload_recover, Recover}],
    start_link(Opts).
start_link(Options) ->
    case ets:info(?overload_ets) of
        undefined ->
            ?overload_ets = ets:new(?overload_ets, [named_table, public, {read_concurrency, true}]),
            ets:insert(?overload_ets, {overloaded, false});
        _ ->
            ok
    end,
    gen_server:start_link({local, ?SERVER}, ?MODULE, Options, []).

start_test() ->
    gen_server:start(?MODULE, [], []).

register(Name) ->
    gen_server:call(?SERVER, {register, Name}, infinity).

unregister(Name) ->
    gen_server:call(?SERVER, {unregister, Name}, infinity).

status() ->
    Status = gen_server:call(?SERVER, status, infinity),
    % I'm having the calling process do derived stats because
    % I don't want to block the rtq from processing objects.
    MaxBytes = proplists:get_value(max_bytes, Status),
    CurrentBytes = proplists:get_value(bytes, Status),
    PercentBytes = round( (CurrentBytes / MaxBytes) * 100000 ) / 1000,
    [{percent_bytes_used, PercentBytes} | Status].

shutdown() ->
    gen_server:call(?SERVER, shutting_down, infinity).

stop() ->
    gen_server:call(?SERVER, stop, infinity).

is_running() ->
    gen_server:call(?SERVER, is_running, infinity).

%% TODO, replace with drain_queue
is_empty(Name) ->
    gen_server:call(?SERVER, {is_empty, Name}, infinity).

all_queues_empty() ->
    gen_server:call(?SERVER, all_queues_empty, infinity).
%%%=====================================================================================================================
%%% Casts
%%%=====================================================================================================================
push(NumItems, Bin) ->
    push(NumItems, Bin, []).
push(NumItems, Bin, Meta) ->
    case ets:lookup(?overload_ets, overloaded) of
        [{overloaded, true}] ->
            lager:debug("rtq overloaded"),
            riak_repl2_rtq_overload_counter:drop();
        [{overloaded, false}] ->
            gen_server:cast(?SERVER, {push, NumItems, Bin, Meta})
    end.

%% TODO provide the ability to pass a list of seq numbers
ack(Name, Seqs) ->
    gen_server:cast(?SERVER, {ack, Name, Seqs}).

report_drops(N) ->
    gen_server:cast(?SERVER, {report_drops, N}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Options) ->
    %% Default maximum realtime queue size to 100Mb
    DefaultMaxBytes = app_helper:get_env(riak_repl, rtq_max_bytes, 100*1024*1024),
    Overloaded = proplists:get_value(overload_threshold, Options, ?DEFAULT_OVERLOAD),
    Recover = proplists:get_value(overload_recover, Options, ?DEFAULT_RECOVER),
    {ok, #state{default_max_bytes = DefaultMaxBytes, overload = Overloaded, recover = Recover}}. % lots of initialization done by defaults


handle_call({register, Name}, Pid, State = #state{qtab = QTab, qseq = QSeq}) ->
    NewState = register_remote(Name, Pid, State),
    {reply, {QTab, QSeq}, NewState};


handle_call({unregister, Name}, _From, State) ->
    {Reply, NewState} =  unregister_q(Name, State),
    {reply, Reply, NewState};

%% TODO decide if want some information from rtsource_rtq
handle_call(status, _From, State) ->
    Status = make_status(State),
    {reply, Status, State};

%% this is okay
handle_call(shutting_down, _From, State = #state{shutting_down=false}) ->
    %% this will allow the realtime repl hook to determine if it should send
    %% to another host
    _ = riak_repl2_rtq_proxy:start(),
    {reply, ok, State#state{shutting_down = true}};

%% this is okay
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

%% this is okay
handle_call(is_running, _From, State = #state{shutting_down = ShuttingDown}) ->
    {reply, not ShuttingDown, State};


%%%=====================================================================================================================
%% TODO, replace with drain_queue functionality for repl_migration
handle_call({is_empty, Name}, _From, State = #state{remotes = Remotes}) ->
    Result = is_queue_empty(Name, Remotes),
    {reply, Result, State};

handle_call(all_queues_empty, _From, State = #state{remotes = Remotes}) ->
    Result = lists:all(fun (#remote{name = Name}) -> is_queue_empty(Name, Remotes) end, Remotes),
    {reply, Result, State};

%% TODO decide if this code stays (it is legacy) [they are needed for backward compatibility]
% either old code or old node has sent us a old push, upvert it.
handle_call({push, NumItems, Bin}, From, State) ->
    handle_call({push, NumItems, Bin, []}, From, State);
handle_call({push, NumItems, Bin, Meta}, _From, State) ->
    State2 = maybe_flip_overload(State),
    {reply, ok, push(NumItems, Bin, Meta, State2)}.
%%%=====================================================================================================================

% ye previous cast. rtq_proxy may send us an old pattern.
handle_cast({push, NumItems, Bin}, State) ->
    handle_cast({push, NumItems, Bin, []}, State);
handle_cast({push, _NumItems, _Bin, _Meta}, State=#state{remotes=[]}) ->
    {noreply, State};
handle_cast({push, NumItems, Bin, Meta}, State) ->
    State2 = maybe_flip_overload(State),
    {noreply, push(NumItems, Bin, Meta, State2)};

handle_cast({ack, Name, Seqs}, State) ->
       {noreply, ack_seqs(Name, Seqs, State)};

handle_cast({report_drops, N}, State) ->
    QSeq = State#state.qseq + N,
    Drops = State#state.overload_drops + N,
    State2 = State#state{qseq = QSeq, overload_drops = Drops},
    State3 = maybe_flip_overload(State2),
    {noreply, State3}.

%% @private
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(Reason, State=#state{cs = Cs}) ->
  lager:info("rtq terminating due to: ~p State: ~p", [Reason, State]),
    %% when started from tests, we may not be registered
    catch(erlang:unregister(?SERVER)),
    flush_pending_pushes(),
    DList = [get_all_delivery_funs(C) || C <- Cs],
%%    _ = [deliver_error(DeliverFun, {terminate, Reason}) || DeliverFun <- lists:flatten(DList)],
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%% ================================================================================================================== %%

%% Internal Functions For Gen Server Calls

%% ================================================================================================================== %%
%% Register
%% TODO: what happens if a remote name change occurs?
%% ================================================================================================================== %%
register_remote(Name, Pid, State = #state{remotes = Remotes, all_remote_names = AllRemoteNames}) ->
    UpdatedRemotes =
        case lists:keytake(Name, #remote.name, Remotes) of
            {value, R = #remote{name = Name, pid = Pid}, Remotes2} ->
                %% rtsource_rtq has re-registered (under the same pid)
                Remotes;
            {value, R = #remote{name = Name, pid = Pid2}, Remotes2} ->
                %% rtosurce_rtq hash re-registered (new pid, it must have died)
                [R#remote{pid = Pid2} | Remotes2];
            false ->
                %% New registration, start from the beginning
                [#remote{name = Name, pid = Pid} | Remotes]
        end,
    UpdatedAllRemoteNames =
        case lists:member(Name, AllRemoteNames) of
            true -> AllRemoteNames;
            false -> [Name | AllRemoteNames]
        end,
    State#state{remotes = UpdatedRemotes, all_remote_names = UpdatedAllRemoteNames}.


%% ================================================================================================================== %%
%% Unregister
%% ================================================================================================================== %%
unregister_q(Name, State = #state{remotes = Remotes, all_remote_names = AllRemoteNames, qtab = QTab}) ->
    NewAllRemoteNames = AllRemoteNames -- [Name],
    case lists:keytake(Name, #remote.name, Remotes) of
        {value, Remote, Remotes2} ->
            MinSeq = ets:first(QTab),
            NewState = unregister_cleanup(Remote, MinSeq, State#state{remotes = Remotes2, all_remote_names = NewAllRemoteNames}),
            {ok, NewState};
        false ->
            {{error, not_registered}, State}
    end.

%% we have to iterate the entire queue and check if we need to delete any objects
unregister_cleanup('$end_of_table', _Remote, State) ->
    State;
unregister_cleanup(Seq, Remote, State = #state{qtab = QTab, all_remote_names = AllRemoteNames}) ->
    case ets:lookup(QTab, Seq) of
        [{_, _, Bin, _, Completed}] ->
            ShrinkSize = ets_obj_size(Bin, State),
            case AllRemoteNames -- Completed of
                [] ->
                    ets:delete(QTab, Seq),
                    NewState = update_q_size(State, -ShrinkSize),
                    unregister_cleanup(ets:next(QTab, Seq), Remote, NewState);
                _ ->
                    unregister_cleanup(ets:next(QTab, Seq), Remote, State)
            end;
        _ ->
            unregister_cleanup(ets:next(QTab, Seq), Remote, State)
    end.


%% ================================================================================================================== %%
%% Status
%% ================================================================================================================== %%
make_status(State = #state{qtab = QTab, qseq = QSeq, remotes = Remotes}) ->
    MaxBytes = get_queue_max_bytes(State),
    RemoteStats =
        lists:foldl(
            fun(Remote, Acc) ->

                #remote{name = Name, total_skipped = Skipped, total_filtered = Filtered, total_acked = Acked,
                    total_drops = Drops, rsize_bytes = RSize, max_ack = MaxAck} = Remote,

                Stats = [{bytes, RSize}, {max_bytes, get_remote_max_bytes(Name)}, {pending, QSeq - MaxAck},
                    {unacked, QSeq - Skipped - Filtered - Acked - Drops}, {skipped, Skipped}, {filtered, Filtered},
                    {acked, Acked}, {drops, Drops}],

                [{Name, Stats} | Acc]
            end, [], Remotes),

    [{bytes, qbytes(QTab, State)},
    {max_bytes, MaxBytes},
    {remotes, RemoteStats},
    {overload_drops, State#state.overload_drops}].

%% ================================================================================================================== %%

%% Internal Functions For Gen Server Casts

%% ================================================================================================================== %%
%% Push
%% TODO: decide if we ever send messages to remote pids (rtsource_rtq processes)
%% ================================================================================================================== %%
push(NumItems, Bin, Meta, State) ->
    #state
    {
        qtab = QTab,
        qseq = QSeq,
        remotes = Remotes,
        all_remote_names = AllRemoteNames,
        shutting_down = false
    } = State,

    QSeq2 = QSeq + 1,
    QEntry = {QSeq2, NumItems, Bin, Meta, Completed},

    {AllowedConsumerNames, FilteredConsumerNames} = allowed_filtered_consumers(Cs, Meta),
    QEntry2 = set_local_forwards_meta(AllowedConsumerNames, QEntry),


    %% we have to send to all consumers to update there state!
    DeliverAndCs2 = [maybe_deliver_item(C, QEntry3) || C <- Cs],
    DeliverAndCs3 = update_consumer_delivery_funs(DeliverAndCs2),
    {DeliverResults, Cs3} = lists:unzip(DeliverAndCs3),

    %% This has changed for 'filtered' to mimic the behaviour of 'skipped'.
    %% We do not want to add an object that all consumers will filter or skip to the queue
    AllSkippedFilteredAcked = lists:all(fun
                                            (skipped) -> true;
                                            (filtered) -> true;
                                            (acked) -> true;
                                            (_) -> false
                                        end, DeliverResults),


    Routed = meta_get(routed_clusters, [], Meta),
    Acked = meta_get(acked_clusters, [], Meta),
    ConsumersToUpdate = AllowedConsumerNames -- (Routed ++ Acked),
    State2 = State#state{cs = Cs3, qseq = QSeq2},
    State3 =
        case AllSkippedFilteredAcked of
            true ->
                State2;
            false ->
                ets:insert(QTab, QEntry3),
                Size = ets_obj_size(Bin, State2),
                NewState = update_q_size(State2, Size),
                update_consumer_q_sizes(NewState, Size, ConsumersToUpdate)
        end,
    trim_q(State3);
push(NumItems, Bin, Meta, State = #state{shutting_down = true}) ->
    riak_repl2_rtq_proxy:push(NumItems, Bin, Meta),
    State.
%% ================================================================================================================== %%
%% Push Helper Functions
%% ================================================================================================================== %%


allowed_filtered_consumers(Cs, Meta) ->
    AllConsumers = [Consumer#c.name || Consumer <- Cs],
    lists:foldl(
        fun(Consumer, {Allowed, Filtered}) ->
            Name = Consumer#c.name,
            case riak_repl2_object_filter:realtime_filter(Name, Meta) of
                true ->
                    {Allowed, [Name | Filtered]};
                false ->
                    {[Name | Allowed], Filtered}
            end end, {[], []}, AllConsumers).

%% Trim the queue if necessary
trim_q(State = #state{qtab = QTab}) ->
    State1 = trim_consumers_q(State, ets:first(QTab)),
    trim_global_q(get_queue_max_bytes(State1), State1).


consumer_needs_trim(undefined, _) ->
    false;
consumer_needs_trim(MaxBytes, #c{consumer_qbytes = CBytes}) ->
    CBytes > MaxBytes.

trim_consumers_q(State = #state{cs = Cs}, Seq) ->
    AllClusters = [C#c.name || C <- Cs],
    TrimmedCs = [C || C <- Cs, not consumer_needs_trim(get_consumer_max_bytes(C), C)],
    case Cs -- TrimmedCs of
        [] ->
            State;
        NotTrimmedCs ->
            trim_consumers_q_entries(State, AllClusters, TrimmedCs, NotTrimmedCs, Seq)
    end.

trim_consumers_q_entries(State, _AllClusters, TrimmedCs, [], '$end_of_table') ->
    State#state{cs = TrimmedCs};
trim_consumers_q_entries(State, _AllClusters, TrimmedCs, [], _Seq) ->
    State#state{cs = TrimmedCs};
trim_consumers_q_entries(State, _AllClusters, TrimmedCs, NotTrimmedCs, '$end_of_table') ->
    lager:warning("rtq trim consumer q, end of table with consumers needing trimming ~p", [NotTrimmedCs]),
    State#state{cs = TrimmedCs ++ NotTrimmedCs};
trim_consumers_q_entries(State = #state{qtab = QTab}, AllClusters, TrimmedCs, NotTrimmedCs, Seq) ->
    [{_, _, Bin, Meta} = QEntry] = ets:lookup(QTab, Seq),
    ShrinkSize = ets_obj_size(Bin, State),

    Routed = meta_get(routed_clusters, [], Meta),
    Filtered = meta_get(filtered_clusters, [], Meta),
    Acked = meta_get(acked_clusters, [], Meta),
    TooAckClusters = AllClusters -- (Routed ++ Filtered ++ Acked),

    NotTrimmedCNames = [C#c.name || C <- NotTrimmedCs],
    ConsumersToBeTrimmed = [C || C <- NotTrimmedCs, lists:member(C#c.name, TooAckClusters)],
    ConsumersNotToBeTrimmed = NotTrimmedCs -- ConsumersToBeTrimmed,
    {NewState, NewConsumers} =
        case TooAckClusters -- NotTrimmedCNames of
            %% The consumers to be trimmed will cause the object to no longer be used by any consumer
            %% delete the object in this case
            [] ->
                State1 = update_q_size(State, -ShrinkSize),
                ets:delete(QTab, Seq),
                {State1, [trim_single_consumer_q_entry(C, ShrinkSize, Seq) || C <- ConsumersToBeTrimmed]};

            %% The object is only relevant to a subset of all consumers
            %% only remove and update the correct consumers
            _ ->
                %% we need to update the object in ets to add these consumers to the ack'd list!
                QEntry2 = set_acked_clusters_meta(Acked ++ [C#c.name || C <- ConsumersToBeTrimmed], QEntry),
                ets:insert(QTab, QEntry2),
                {State, [trim_single_consumer_q_entry(C, ShrinkSize, Seq) || C <- ConsumersToBeTrimmed] ++ ConsumersNotToBeTrimmed}
        end,
    trim_consumers_q(NewState#state{cs = TrimmedCs ++ NewConsumers}, ets:next(QTab, Seq)).


trim_single_consumer_q_entry(C = #c{cseq = CSeq}, ShrinkSize, TrimSeq) ->
    C1 = update_cq_size(C, -ShrinkSize),
    case CSeq < TrimSeq of
        true ->
            %% count the drop and increase the cseq and aseq to the new trimseq value
            C1#c{drops = C#c.drops + 1, c_drops = C#c.c_drops + 1, cseq = TrimSeq, aseq = TrimSeq};
        _ ->
            C1
    end.



trim_global_q(undefined, State) ->
    State;
trim_global_q(MaxBytes, State = #state{qtab = QTab, qseq = QSeq}) ->
    case qbytes(QTab, State) > MaxBytes of
        true ->
            {Cs2, NewState} = trim_global_q_entries(QTab, MaxBytes, State#state.cs,
                State),

            %% Adjust the last sequence handed out number
            %% so that the next pull will retrieve the new minseq
            %% number.  If that increases a consumers cseq,
            %% reset the aseq too.  The drops have already been
            %% accounted for.
            NewCSeq = case ets:first(QTab) of
                          '$end_of_table' ->
                              QSeq; % if empty, make sure pull waits
                          MinSeq ->
                              MinSeq - 1
                      end,
            Cs3 = [case CSeq < NewCSeq of
                       true ->
                           C#c{cseq = NewCSeq, aseq = NewCSeq};
                       _ ->
                           C
                   end || C = #c{cseq = CSeq} <- Cs2],
            NewState#state{cs = Cs3};
        false -> % Q size is less than MaxBytes words
            State
    end.

trim_global_q_entries(QTab, MaxBytes, Cs, State) ->
    {Cs2, State2, Entries, Objects} = trim_global_q_entries(QTab, MaxBytes, Cs, State, 0, 0),
    if
        Entries + Objects > 0 ->
            lager:debug("Dropped ~p objects in ~p entries due to reaching maximum queue size of ~p bytes", [Objects, Entries, MaxBytes]);
        true ->
            ok
    end,
    {Cs2, State2}.

trim_global_q_entries(QTab, MaxBytes, Cs, State, Entries, Objects) ->
    case ets:first(QTab) of
        '$end_of_table' ->
            {Cs, State, Entries, Objects};
        TrimSeq ->
            [{_, NumObjects, Bin, Meta}] = ets:lookup(QTab, TrimSeq),
            ShrinkSize = ets_obj_size(Bin, State),
            NewState1 = update_q_size(State, -ShrinkSize),

            Routed = meta_get(routed_clusters, [], Meta),
            Filtered = meta_get(filtered_clusters, [], Meta),
            Acked = meta_get(acked_clusters, [], Meta),
            TooAckClusters = [C#c.name || C <- Cs] -- (Routed ++ Filtered ++ Acked),
            NewState2 = update_consumer_q_sizes(NewState1, -ShrinkSize, TooAckClusters),
            ets:delete(QTab, TrimSeq),
            Cs2 = [case CSeq < TrimSeq of
                       true ->
                           %% If the last sent qentry is before the trimseq
                           %% it will never be sent, so count it as a drop.
                           C#c{drops = C#c.drops + 1, q_drops = C#c.q_drops +1};
                       _ ->
                           C
                   end || C = #c{cseq = CSeq} <- Cs],
            %% Rinse and repeat until meet the target or the queue is empty
            case qbytes(QTab, NewState2) > MaxBytes of
                true ->
                    trim_global_q_entries(QTab, MaxBytes, Cs2, NewState2, Entries + 1, Objects + NumObjects);
                _ ->
                    {Cs2, NewState2, Entries + 1, Objects + NumObjects}
            end
    end.

%% ================================================================================================================== %%
%% Acking the queue. Either adds to a remote to the 'Completed' list, or deletes the object.
%% ================================================================================================================== %%
ack_seqs(Name, Seqs, State = #state{remotes = Remotes}) ->
    case lists:keytake(Name, #remote.name, Remotes) of
        false ->
            State;
        {value, Remote, Remotes2} ->
            {UpdatedRemote, UpdatedState} =
                lists:foldl(
                    fun(Seq, {AccRemote, AccState}) ->
                        ack_seq(Seq, AccRemote, AccState)
                    end, {Remote, State}, Seqs),
            UpdatedState#state{remotes = [UpdatedRemote |Remotes2]}
    end.

ack_seq(Seq, Remote, State) ->
    #state{qtab = QTab, qsize_bytes = QSize, all_remote_names = AllRemoteNames} = State,
    case ets:lookup(QTab, Seq) of
        [] ->
            %% TODO:
            %% the queue has been trimmed due to reaching its maximum size
            %% but this has been sent and acked! - so we can reduce the dropped counter
            {Remote, State};
        [{_, _, Bin, _, Completed}] ->
            NewCompleted = [Remote#remote.name | Completed],
            ShrinkSize = ets_obj_size(Bin, State),
            NewRemote = update_remote_size(Remote, -ShrinkSize),
            case AllRemoteNames -- NewCompleted of
                [] ->
                    ets:delete(QTab, Seq),
                    NewState = update_q_size(State, -ShrinkSize),
                    {NewRemote, NewState};
                _ ->
                    ets:update_element(QTab, Seq, {5, NewCompleted}),
                    {NewRemote, State}
            end;
        UnExpectedObj ->
            lager:warning("Unexpected object in RTQ, ~p", [UnExpectedObj]),
            {Remote, QSize}
    end.
%% ================================================================================================================== %%

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% ================================================================================================================== %%
%% Maybe flip overload used to ensure that the rtq mailbox does not exceed a configured size.
%% ================================================================================================================== %%
maybe_flip_overload(State) ->
    #state{overloaded = Overloaded, overload = Overload, recover = Recover} = State,
    {message_queue_len, MsgQLen} = erlang:process_info(self(), message_queue_len),
    if
        Overloaded andalso MsgQLen =< Recover ->
            lager:info("Recovered from overloaded condition"),
            ets:insert(?overload_ets, {overloaded, false}),
            State#state{overloaded = false};
        (not Overloaded) andalso MsgQLen > Overload ->
            lager:warning("Realtime queue mailbox size of ~p is greater than ~p indicating overload; objects will be dropped until size is less than or equal to ~p", [MsgQLen, Overload, Recover]),
            % flip the rt_dirty flag on
            riak_repl_stats:rt_source_errors(),
            ets:insert(?overload_ets, {overloaded, true}),
            State#state{overloaded = true};
        true ->
            State
    end.

flush_pending_pushes() ->
    receive
        {'$gen_cast', {push, NumItems, Bin}} ->
            riak_repl2_rtq_proxy:push(NumItems, Bin),
            flush_pending_pushes()
    after
        1000 ->
            ok
    end.




meta_get(Key, Default, Meta) ->
    case orddict:find(Key, Meta) of
        error -> Default;
        {ok, Value} -> Value
    end.



set_local_forwards_meta(LocalForwards, QEntry) ->
    set_meta(QEntry, local_forwards, LocalForwards).

set_filtered_clusters_meta(FilteredClusters, QEntry) ->
    set_meta(QEntry, filtered_clusters, FilteredClusters).

set_acked_clusters_meta(AckedClusters, QEntry) ->
    set_meta(QEntry, acked_clusters, AckedClusters).

set_meta({Seq, NumItems, Bin, Meta}, Key, Value) ->
    Meta2 = orddict:store(Key, Value, Meta),
    {Seq, NumItems, Bin, Meta2}.




ets_obj_size(Obj, _State=#state{word_size = WordSize}) when is_binary(Obj) ->
  ets_obj_size(Obj, WordSize);
ets_obj_size(Obj, WordSize) when is_binary(Obj) ->
  BSize = erlang:byte_size(Obj),
  case BSize > 64 of
        true -> BSize - (6 * WordSize);
        false -> BSize
  end;
ets_obj_size(Obj, _) ->
  erlang:size(Obj).

update_q_size(State = #state{qsize_bytes = CurrentQSize}, Diff) ->
  State#state{qsize_bytes = CurrentQSize + Diff}.

update_consumer_q_sizes(State = #state{cs = Cs}, Diff, AllowedNames) ->
    Cs2 = lists:map(
        fun(C) ->
            case lists:member(C#c.name, AllowedNames) of
                true ->
                    update_cq_size(C, Diff);
                false ->
                    C
            end
        end, Cs),
    State#state{cs = Cs2}.


update_remote_size(Remote = #remote{rsize_bytes = RBytes}, Diff) ->
    Remote#remote{rsize_bytes = RBytes + Diff}.






get_all_delivery_funs(C) ->
    case C#c.deliver of
        undefined ->
            C#c.delivery_funs;
        Deliver ->
            C#c.delivery_funs ++ [Deliver]
    end.



-ifdef(TEST).
qbytes(_QTab, #state{qsize_bytes = QSizeBytes}) ->
    %% when EQC testing, don't account for ETS overhead
    QSizeBytes.

get_queue_max_bytes(#state{default_max_bytes = Default}) ->
    Default.

get_remote_max_bytes(_) ->
    undefined.

-else.
qbytes(QTab, #state{qsize_bytes = QSizeBytes, word_size=WordSize}) ->
    Words = ets:info(QTab, memory),
    (Words * WordSize) + QSizeBytes.

get_queue_max_bytes(#state{default_max_bytes = Default}) ->
    case riak_core_metadata:get(?RIAK_REPL2_RTQ_CONFIG_KEY, queue_max_bytes) of
        undefined -> Default;
        MaxBytes -> MaxBytes
    end.

get_remote_max_bytes(#remote{name = Name}) ->
    riak_core_metadata:get(?RIAK_REPL2_RTQ_CONFIG_KEY, {consumer_max_bytes, Name}).
-endif.

is_queue_empty(Name, Cs) ->
    case lists:keytake(Name, #c.name, Cs) of
        {value,  #c{consumer_qbytes = CBytes}, _Cs2} ->
            case CBytes == 0 of
                true -> false;
                false -> true
            end;
        false -> lager:error("Unknown queue")
    end.


%% Find the first sequence number
minseq(QTab, QSeq) ->
    case ets:first(QTab) of
        '$end_of_table' ->
            QSeq;
        MinSeq ->
            MinSeq - 1
    end.


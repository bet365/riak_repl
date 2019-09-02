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
-module(riak_repl2_rtcq).

-behaviour(gen_server).
%% API
-export([start_link/0,
    start_link/1,
    start_test/0,
    register/1,
    unregister/1,
    set_max_bytes/1,
    push/3,
    push/2,
    pull/2,
    pull_sync/2,
    ack/2,
    ack_sync/2,
    status/0,
    dumpq/0,
    summarize/0,
    evict/1,
    evict/2,
    is_empty/0,
    all_queues_empty/0,
    shutdown/0,
    stop/0,
    is_running/0]).
% private api
-export([report_drops/1]).

-define(overload_ets, rtq_overload_ets).
-define(SERVER, ?MODULE).
-define(DEFAULT_OVERLOAD, 2000).
-define(DEFAULT_RECOVER, 1000).
-define(DEFAULT_RTQ_LATENCY_SLIDING_WINDOW, 300).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-record(state, {
    qtab = ets:new(?MODULE, [protected, ordered_set]),
    qseq = 0,
    max_bytes = undefined,
    overload = ?DEFAULT_OVERLOAD :: pos_integer(),
    recover = ?DEFAULT_RECOVER :: pos_integer(),
    overloaded = false :: boolean(),
    overload_drops = 0 :: non_neg_integer(),
    consumer = #consumer{},
    shutting_down=false,
    qsize_bytes = 0,
    word_size=erlang:system_info(wordsize)
}).

% Consumers
-record(consumer, {
    name,      % consumer name
    aseq = 0,  % last sequence acked
    cseq = 0,  % last sequence sent
    skips = 0,
    drops = 0, % number of dropped queue entries (not items)
    errs = 0,  % delivery errors
    deliver,  % deliver function if pending, otherwise undefined
    delivery_funs = [],
    delivered = false,  % used by the skip count.
    % skip_count is used to help the sink side determine if an item has
    % been dropped since the last delivery. The sink side can't
    % determine if there's been drops accurately if the source says there
    % were skips before it's sent even one item.
    filtered = 0,
    last_seen  % a timestamp of when we received the last ack to measure latency
}).


start_link(Name) ->
    Overload = app_helper:get_env(riak_repl, rtq_overload_threshold, ?DEFAULT_OVERLOAD),
    Recover = app_helper:get_env(riak_repl, rtq_overload_recover, ?DEFAULT_RECOVER),
    Opts = [{overload_threshold, Overload}, {overload_recover, Recover}],
    start_link(Name, Opts).


start_link(Name, Options) ->
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

is_empty() ->
    gen_server:call(?SERVER, is_empty, infinity).

all_queues_empty() ->
    gen_server:call(?SERVER, all_queues_empty, infinity).

set_max_bytes(MaxBytes) ->
    gen_server:call(?SERVER, {set_max_bytes, MaxBytes}, infinity).

push(NumItems, Bin, Meta) ->
    case should_drop() of
        true ->
            lager:debug("rtq overloaded"),
            riak_repl2_rtq_overload_counter:drop();
        false ->
            gen_server:cast(?SERVER, {push, NumItems, Bin, Meta})
    end.

should_drop() ->
    [{overloaded, Val}] = ets:lookup(?overload_ets, overloaded),
    Val.

push(NumItems, Bin) ->
    push(NumItems, Bin, []).

pull(Name, DeliverFun) ->
    gen_server:cast(?SERVER, {pull, DeliverFun}).

pull_sync(Name, DeliverFun) ->
    gen_server:call(?SERVER, {pull_with_ack, DeliverFun}, infinity).

ack(Name, Seq) ->
    gen_server:cast(?SERVER, {ack, Name, Seq, os:timestamp()}).


ack_sync(Name, Seq) ->
    gen_server:call(?SERVER, {ack_sync, Name, Seq, os:timestamp()}, infinity).

status() ->
    Status = gen_server:call(?SERVER, status, infinity),
    MaxBytes = proplists:get_value(max_bytes, Status),
    CurrentBytes = proplists:get_value(bytes, Status),
    PercentBytes = round( (CurrentBytes / MaxBytes) * 100000 ) / 1000,
    [{percent_bytes_used, PercentBytes} | Status].

dumpq() ->
    gen_server:call(?SERVER, dumpq, infinity).

summarize() ->
    gen_server:call(?SERVER, summarize, infinity).

evict(Seq) ->
    gen_server:call(?SERVER, {evict, Seq}, infinity).

evict(Seq, Key) ->
    gen_server:call(?SERVER, {evict, Seq, Key}, infinity).

shutdown() ->
    gen_server:call(?SERVER, shutting_down, infinity).

stop() ->
    gen_server:call(?SERVER, stop, infinity).

is_running() ->
    gen_server:call(?SERVER, is_running, infinity).

report_drops(N) ->
    gen_server:cast(?SERVER, {report_drops, N}).

init(Name, Options) ->
    MaxBytes = app_helper:get_env(riak_repl, rtq_max_bytes, 100*1024*1024),
    Overloaded = proplists:get_value(overload_threshold, Options, ?DEFAULT_OVERLOAD),
    Recover = proplists:get_value(overload_recover, Options, ?DEFAULT_RECOVER),

    RTQSlidingWindow = app_helper:get_env(riak_repl, rtq_latency_window,?DEFAULT_RTQ_LATENCY_SLIDING_WINDOW),
    case folsom_metrics:metric_exists({latency, Name}) of
        true -> skip;
        false ->
            folsom_metrics:new_histogram({latency, Name}, slide, RTQSlidingWindow)
    end,

    {ok, #state{max_bytes = MaxBytes, overload = Overloaded, recover = Recover}}.


handle_call(status, _From, State = #state{qtab = QTab, max_bytes = MaxBytes, qseq = QSeq,
    consumer = #consumer{
        name = Name,
        aseq = ASeq, cseq = CSeq,
        drops = Drops, errs = Errs,
        skips = Skips, filtered = Filters}}) ->

    MetricName = {latency, Name},
    Latency = case folsom_metrics:metric_exists(MetricName) of
                  true ->
                      folsom_metrics:get_histogram_statistics(MetricName);
                  false ->
                      []
              end,

    Status =
        [
            {bytes, qbytes(QTab, State)},
            {max_bytes, MaxBytes},
            {pending, QSeq - CSeq},
            {unacked, CSeq - ASeq - Skips},
            {drops, Drops},
            {errs, Errs},
            {filtered, Filters},
            {overload_drops, State#state.overload_drops},
            {latency, Latency}
        ],
    {reply, Status, State};

handle_call(shutting_down, _From, State = #state{shutting_down=false}) ->
    %%TODO: this behaviour will now change, and be dealt with in rtfq
    {reply, ok, State#state{shutting_down = true}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(is_running, _From,
    State = #state{shutting_down = ShuttingDown}) ->
    {reply, not ShuttingDown, State};

handle_call(is_empty, _From, State = #state{qseq = QSeq, consumer = Consumer}) ->
    Result = is_queue_empty(Consumer, QSeq),
    {reply, Result, State};

handle_call(all_queues_empty, _From, State = #state{qseq = QSeq, consumer = Consumer}) ->
    Result = is_queue_empty(Consumer, QSeq),
    {reply, Result, State};


%%handle_call({register, Name}, _From, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->
%%    MinSeq = minseq(QTab, QSeq),
%%    case lists:keytake(Name, #c.name, Cs) of
%%        {value, C = #c{aseq = PrevASeq, drops = PrevDrops}, Cs2} ->
%%            %% Work out if anything should be considered dropped if
%%            %% unacknowledged.
%%            Drops = max(0, MinSeq - PrevASeq - 1),
%%
%%            %% Re-registering, send from the last acked sequence
%%            CSeq = case C#c.aseq < MinSeq of
%%                       true -> MinSeq;
%%                       false -> C#c.aseq
%%                   end,
%%            UpdCs = [C#c{cseq = CSeq, drops = PrevDrops + Drops,
%%                deliver = undefined} | Cs2];
%%        false ->
%%            %% New registration, start from the beginning
%%            CSeq = MinSeq,
%%            UpdCs = [#c{name = Name, aseq = CSeq, cseq = CSeq} | Cs]
%%    end,
%%    {reply, {ok, CSeq}, State#state{cs = UpdCs}};
%%handle_call({unregister, Name}, _From, State) ->
%%    case unregister_q(Name, State) of
%%        {ok, NewState} ->
%%            catch folsom_metrics:delete_metric({latency, Name}),
%%            {reply, ok, NewState};
%%        {{error, not_registered}, State} ->
%%            {reply, {error, not_registered}, State}
%%    end;

handle_call({set_max_bytes, MaxBytes}, _From, State) ->
    {reply, ok, trim_q(State#state{max_bytes = MaxBytes})};
handle_call(dumpq, _From, State = #state{qtab = QTab}) ->
    {reply, ets:tab2list(QTab), State};

handle_call(summarize, _From, State = #state{qtab = QTab}) ->
    Fun = fun({Seq, _NumItems, Bin, _Meta}, Acc) ->
        Obj = riak_repl_util:from_wire(Bin),
        {Key, Size} = summarize_object(Obj),
        Acc ++ [{Seq, Key, Size}]
          end,
    {reply, ets:foldl(Fun, [], QTab), State};

handle_call({evict, Seq}, _From, State = #state{qtab = QTab}) ->
    ets:delete(QTab, Seq),
    {reply, ok, State};
handle_call({evict, Seq, Key}, _From, State = #state{qtab = QTab}) ->
    case ets:lookup(QTab, Seq) of
        [{Seq, _, Bin, _}] ->
            Obj = riak_repl_util:from_wire(Bin),
            case Key =:= riak_object:key(Obj) of
                true ->
                    ets:delete(QTab, Seq),
                    {reply, ok, State};
                false ->
                    {reply, {wrong_key, Seq, Key}, State}
            end;
        _ ->
            {reply, {not_found, Seq}, State}
    end;

handle_call({pull_with_ack, DeliverFun}, _From, State) ->
    {reply, ok, pull(DeliverFun, State)};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% for old eqc testing I think. (remove once confirmed all testing works)
handle_call({push, NumItems, Bin}, From, State) ->
    handle_call({push, NumItems, Bin, []}, From, State);
handle_call({push, NumItems, Bin, Meta}, _From, State) ->
    State2 = maybe_flip_overload(State),
    {reply, ok, push(NumItems, Bin, Meta, State2)};
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call({ack_sync, Name, Seq, Ts}, _From, State) ->
    {reply, ok, ack_seq(Name, Seq, Ts, State)}.

% ye previous cast. rtq_proxy may send us an old pattern.
handle_cast({push, NumItems, Bin}, State) ->
    handle_cast({push, NumItems, Bin, []}, State);

handle_cast({push, NumItems, Bin, Meta}, State) ->
    State2 = maybe_flip_overload(State),
    {noreply, push(NumItems, Bin, Meta, State2)};

%% @private
handle_cast({report_drops, N}, State) ->
    QSeq = State#state.qseq + N,
    Drops = State#state.overload_drops + N,
    State2 = State#state{qseq = QSeq, overload_drops = Drops},
    State3 = maybe_flip_overload(State2),
    {noreply, State3};

handle_cast({pull, DeliverFun}, State) ->
    {noreply, do_pull(DeliverFun, State)};

handle_cast({ack, Name, Seq, Ts}, State) ->
    {noreply, ack_seq(Name, Seq, Ts, State)}.

record_consumer_latency(Name, OldLastSeen, SeqNumber, NewTimestamp) ->
    case OldLastSeen of
        {SeqNumber, OldTimestamp} ->
            folsom_metrics:notify({{latency, Name}, abs(timer:now_diff(NewTimestamp, OldTimestamp))});
        _ ->
            % Don't log for a non-matching seq number
            skip
    end.

ack_seq(Name, Seq, NewTs, State = #state{qtab = QTab, qseq = QSeq, consumer = Consumer}) ->

    %% record consumer latency statistic
    LastSeen = Consumer#consumer.last_seen,
    record_consumer_latency(Name, LastSeen, Seq, NewTs),

    %% find min sequnce number
    MinSeq = min(Seq, QSeq),

    %% Remove any entries from the ETS table before MinSeq
    NewState = cleanup(QTab, MinSeq, State),

    %% Update consumers ack'd sequence number
    NewState#state{consumer = Consumer#consumer{aseq = Seq}}.

%% @private
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(Reason, State=#state{consumer = Consumer}) ->
    lager:info("rtq terminating due to: ~p State: ~p", [Reason, State]),
    %% when started from tests, we may not be registered
    catch(erlang:unregister(?SERVER)),
    flush_pending_pushes(),
    DList = [get_all_delivery_funs(Consumer)],
    _ = [deliver_error(DeliverFun, {terminate, Reason}) || DeliverFun <- lists:flatten(DList)],
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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


%%unregister_q(Name, State = #state{qtab = QTab, cs = Cs}) ->
%%    case lists:keytake(Name, #c.name, Cs) of
%%        {value, C, Cs2} ->
%%            %% Remove C from Cs, let any pending process know
%%            %% and clean up the queue
%%            case get_all_delivery_funs(C) of
%%                [] ->
%%                    ok;
%%                DeliveryFuns ->
%%                    _ = [deliver_error(DeliverFun, {error, unregistered}) || DeliverFun <- DeliveryFuns]
%%            end,
%%            MinSeq = case Cs2 of
%%                         [] ->
%%                             State#state.qseq; % no consumers, remove it all
%%                         _ ->
%%                             lists:min([Seq || #c{aseq = Seq} <- Cs2])
%%                     end,
%%            NewState0 = cleanup(QTab, MinSeq, State),
%%            {ok, NewState0#state{cs = Cs2}};
%%        false ->
%%            {{error, not_registered}, State}
%%    end.

recast_saved_deliver_funs(DeliverStatus, C = #consumer{delivery_funs = DeliveryFuns, name = Name}) ->
    case DeliverStatus of
        delivered ->
            case DeliveryFuns of
                [] ->
                    {DeliverStatus, C};
                _ ->
                    _ = [gen_server:cast(?SERVER, {pull, Name, DeliverFun}) || DeliverFun <- DeliveryFuns],
                    {DeliverStatus, C#consumer{delivery_funs = []}}
            end;
        _ ->
            {DeliverStatus, C}
    end.

push(NumItems, Bin, Meta, State = #state{qtab = QTab,
    qseq = QSeq,
    consumer = Consumer,
    shutting_down = false}) ->

    QSeq2 = QSeq + 1,
    QEntry = {QSeq2, NumItems, Bin, Meta},
    {DeliverStatus0, UpdatedConsumer0} = maybe_deliver_item(Consumer, QEntry),
    {DeliverStatus, UpdatedConsumer} = recast_saved_deliver_funs(DeliverStatus0, UpdatedConsumer0),

    NewState = case DeliverStatus of
                   skipped ->
                       State;
                   _ ->
                       ets:insert(QTab, QEntry),
                       Size = ets_obj_size(Bin, State),
                       update_q_size(State, Size)

               end,
    trim_q(NewState#state{qseq = QSeq2, consumer = UpdatedConsumer});

push(NumItems, Bin, Meta, State = #state{shutting_down = true}) ->
    riak_repl2_rtq_proxy:push(NumItems, Bin, Meta),
    State.

do_pull(DeliverFun, State = #state{qtab = QTab, qseq = QSeq, consumer = Consumer}) ->
    UpdatedConsumer = maybe_pull(QTab, QSeq, Consumer, DeliverFun),
    State#state{consumer = UpdatedConsumer}.


maybe_pull(QTab, QSeq,
    Consumer = #consumer{cseq = CSeq, skips = Skips},
    DeliverFun) ->

    CSeq2 = CSeq + 1,
    case CSeq2 =< QSeq of
        true -> % something reday
            case ets:lookup(QTab, CSeq2) of
                [] -> % entry removed, due to previously being unroutable
                    C2 = Consumer#consumer{skips = Skips+1, cseq = CSeq2},
                    maybe_pull(QTab, QSeq, C2, DeliverFun);
                [QEntry] ->
                    case Consumer#consumer.deliver of
                        undefined ->
                            % if the item can't be delivered due to cascading rt,
                            % just keep trying.
                            case maybe_deliver_item(Consumer#consumer{deliver = DeliverFun}, QEntry) of
                                {skipped, C2} ->
                                    C3 = C2#consumer{
                                        deliver = Consumer#consumer.deliver,
                                        delivery_funs = Consumer#consumer.delivery_funs
                                    },
                                    maybe_pull(QTab, QSeq, C3, DeliverFun);
                                {_WorkedOrNoFun, C2} ->
                                    C2
                            end;

                        %% we have a saved function due to being at the head of the queue, just add the function and let the push
                        %% functionality push the items out to the helpers using the saved deliverfuns
                        _ ->
                            save_consumer(Consumer, DeliverFun)
                    end
            end;
        false ->
            %% consumer is up to date with head, keep deliver function
            %% until something pushed
            save_consumer(Consumer, DeliverFun)
    end.



maybe_deliver_item(C = #c{deliver = undefined}, QEntry) ->
    {_Seq, _NumItem, _Bin, Meta} = QEntry,

    Name = C#c.name,
    Routed = case orddict:find(routed_clusters, Meta) of
                 error -> [];
                 {ok, V} -> V
             end,

    Filtered = riak_repl2_object_filter:realtime_filter(Name, Meta),
    Cause = case {lists:member(Name, Routed), Filtered} of
                {true, _} ->
                    skipped;

                {_, true} ->
                    filtered;

                {_, false} ->
                    no_fun
            end,
    {Cause, C};
maybe_deliver_item(C, QEntry) ->
    {Seq, _NumItem, _Bin, Meta} = QEntry,
    #c{name = Name} = C,
    Routed = case orddict:find(routed_clusters, Meta) of
                 error -> [];
                 {ok, V} -> V
             end,

    Filtered = riak_repl2_object_filter:realtime_filter(C#c.name, Meta),
    case {lists:member(Name, Routed), Filtered} of
        {true, false} when C#c.delivered ->
            Skipped = C#c.skips + 1,
            {skipped, C#c{skips = Skipped, cseq = Seq}};

        {true, false} ->
            {skipped, C#c{cseq = Seq, aseq = Seq}};

        {false, false} ->
            {delivered, deliver_item(C, C#c.deliver, QEntry)};

        {_, true} ->
            {filtered, C#c{cseq = Seq, aseq = Seq, filtered = C#c.filtered + 1, delivered = true, skips=0}}
    end.

deliver_item(C, DeliverFun, {Seq, _NumItem, _Bin, _Meta} = QEntry) ->
    try
        Seq = C#c.cseq + 1, % bit of paranoia, remove after EQC
        QEntry2 = set_skip_meta(QEntry, Seq, C),
        ok = DeliverFun(QEntry2),
        case Seq rem app_helper:get_env(riak_repl, rtq_latency_interval, 1000) of
            0 ->
                C#c{cseq = Seq, deliver = undefined, delivered = true, skips = 0, last_seen = {Seq, os:timestamp()}};
            _ ->
                C#c{cseq = Seq, deliver = undefined, delivered = true, skips = 0}
        end
    catch
        Type:Error ->
            lager:warning("did not deliver object back to rtsource_helper, Reason: {~p,~p}", [Type, Error]),
            lager:info("Seq: ~p   -> CSeq: ~p", [Seq, C#c.cseq]),
            lager:info("consumer: ~p" ,[C]),
            riak_repl_stats:rt_source_errors(),
            %% do not advance head so it will be delivered again
            C#c{errs = C#c.errs + 1, deliver = undefined}
    end.

%% Deliver an error if a delivery function is registered.
deliver_error(DeliverFun, Reason) when is_function(DeliverFun)->
    catch DeliverFun({error, Reason}),
    ok;
deliver_error(_NotAFun, _Reason) ->
    ok.

% if nothing has been delivered, the sink assumes nothing was skipped
% fulfill that expectation.
set_skip_meta(QEntry, _Seq, _C = #c{delivered = false}) ->
    set_meta(QEntry, skip_count, 0);
set_skip_meta(QEntry, _Seq, _C = #c{skips = S}) ->
    set_meta(QEntry, skip_count, S).

set_local_forwards_meta(LocalForwards, QEntry) ->
    set_meta(QEntry, local_forwards, LocalForwards).

set_meta({_Seq, _NumItems, _Bin, Meta} = QEntry, Key, Value) ->
    Meta2 = orddict:store(Key, Value, Meta),
    setelement(4, QEntry, Meta2).

%% Cleanup until the start of the table
cleanup(_QTab, '$end_of_table', State) ->
    State;
cleanup(QTab, Seq, State) ->
    case ets:lookup(QTab, Seq) of
        [] -> cleanup(QTab, ets:prev(QTab, Seq), State);
        [{_, _, Bin, _Meta}] ->
            ShrinkSize = ets_obj_size(Bin, State),
            NewState = update_q_size(State, -ShrinkSize),
            ets:delete(QTab, Seq),
            cleanup(QTab, ets:prev(QTab, Seq), NewState);
        _ ->
            lager:warning("Unexpected object in RTQ")
    end.

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

%% Trim the queue if necessary
trim_q(State = #state{max_bytes = undefined}) ->
    State;
trim_q(State = #state{qtab = QTab, qseq = QSeq, max_bytes = MaxBytes}) ->
    case qbytes(QTab, State) > MaxBytes of
        true ->
            {Cs2, NewState} = trim_q_entries(QTab, MaxBytes, State#state.cs,
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

trim_q_entries(QTab, MaxBytes, Cs, State) ->
    {Cs2, State2, Entries, Objects} = trim_q_entries(QTab, MaxBytes, Cs, State, 0, 0),
    if
        Entries + Objects > 0 ->
            lager:debug("Dropped ~p objects in ~p entries due to reaching maximum queue size of ~p bytes", [Objects, Entries, MaxBytes]);
        true ->
            ok
    end,
    {Cs2, State2}.

trim_q_entries(QTab, MaxBytes, Cs, State, Entries, Objects) ->
    case ets:first(QTab) of
        '$end_of_table' ->
            {Cs, State, Entries, Objects};
        TrimSeq ->
            [{_, NumObjects, Bin, _Meta}] = ets:lookup(QTab, TrimSeq),
            ShrinkSize = ets_obj_size(Bin, State),
            NewState = update_q_size(State, -ShrinkSize),
            ets:delete(QTab, TrimSeq),
            Cs2 = [case CSeq < TrimSeq of
                       true ->
                           %% If the last sent qentry is before the trimseq
                           %% it will never be sent, so count it as a drop.
                           C#c{drops = C#c.drops + 1};
                       _ ->
                           C
                   end || C = #c{cseq = CSeq} <- Cs],
            %% Rinse and repeat until meet the target or the queue is empty
            case qbytes(QTab, NewState) > MaxBytes of
                true ->
                    trim_q_entries(QTab, MaxBytes, Cs2, NewState, Entries + 1, Objects + NumObjects);
                _ ->
                    {Cs2, NewState, Entries + 1, Objects + NumObjects}
            end
    end.

save_consumer(C, DeliverFun) ->
    case C#c.deliver of
        undefined ->
            C#c{deliver = DeliverFun};
        _ ->
            DeliveryFuns = C#c.delivery_funs ++ [DeliverFun],
            C#c{delivery_funs = DeliveryFuns}
    end.

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
-else.
qbytes(QTab, #state{qsize_bytes = QSizeBytes, word_size=WordSize}) ->
    Words = ets:info(QTab, memory),
    (Words * WordSize) + QSizeBytes.
-endif.

is_queue_empty(#consumer{cseq = CSeq}, QSeq) ->
    CSeq2 = CSeq + 1,
    case CSeq2 =< QSeq of
        true -> false;
        false -> true
    end


%% Find the first sequence number
minseq(QTab, QSeq) ->
    case ets:first(QTab) of
        '$end_of_table' ->
            QSeq;
        MinSeq ->
            MinSeq - 1
    end.

summarize_object(Obj) ->
    ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
    {riak_object:key(Obj), riak_object:approximate_size(ObjFmt, Obj)}.
%%%-------------------------------------------------------------------
%%% @author nordinesaabouni
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Feb 2020 12:51
%%%-------------------------------------------------------------------
-module(old_rtq).
-author("nordinesaabouni").

%% API
-export([
    dumpq/0,
    summarize/0,
    evict/1,
    evict/2,
    ack_sync/2,
    pull/2,
    pull_sync/2,
]).



%%%===================================================================
%%% Not needed
%%%===================================================================

%% ================================================================================================================== %%
summarize() ->
    gen_server:call(?SERVER, summarize, infinity).

handle_call(summarize, _From, State = #state{qtab = QTab}) ->
    Fun = fun({Seq, _NumItems, Bin, _Meta}, Acc) ->
        Obj = riak_repl_util:from_wire(Bin),
        {Key, Size} = summarize_object(Obj),
        Acc ++ [{Seq, Key, Size}]
          end,
    {reply, ets:foldl(Fun, [], QTab), State};

summarize_object(Obj) ->
    ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
    {riak_object:key(Obj), riak_object:approximate_size(ObjFmt, Obj)}.
%% ================================================================================================================== %%
dumpq() ->
    gen_server:call(?SERVER, dumpq, infinity).

handle_call(dumpq, _From, State = #state{qtab = QTab}) ->
    {reply, ets:tab2list(QTab), State};
%% ================================================================================================================== %%
evict(Seq) ->
    gen_server:call(?SERVER, {evict, Seq}, infinity).

evict(Seq, Key) ->
    gen_server:call(?SERVER, {evict, Seq, Key}, infinity).

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
%% ================================================================================================================== %%
pull_sync(Name, DeliverFun) ->
    gen_server:call(?SERVER, {pull_with_ack, Name, DeliverFun}, infinity).


handle_call({pull_with_ack, Name, DeliverFun}, _From, State) ->
    {reply, ok, pull(Name, DeliverFun, State)};
%% ================================================================================================================== %%
pull(Name, DeliverFun) ->
    gen_server:cast(?SERVER, {pull, Name, DeliverFun}).

handle_cast({pull, Name, DeliverFun}, State) ->
    {noreply, pull(Name, DeliverFun, State)};

pull(Name, DeliverFun, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->
    CsNames = [C#c.name || C <- Cs],
    UpdCs = case lists:keytake(Name, #c.name, Cs) of
                {value, C, Cs2} ->
                    [maybe_pull(QTab, QSeq, C, CsNames, DeliverFun) | Cs2];
                false ->
                    lager:error("Consumer ~p pulled from RTQ, but was not registered", [Name]),
                    _ = deliver_error(DeliverFun, not_registered),
                    Cs
            end,
    State#state{cs = UpdCs}.




%% Deliver an error if a delivery function is registered.
deliver_error(DeliverFun, Reason) when is_function(DeliverFun)->
    catch DeliverFun({error, Reason}),
    ok;
deliver_error(_NotAFun, _Reason) ->
    ok.

maybe_pull(QTab, QSeq, C = #c{cseq = CSeq}, CsNames, DeliverFun) ->
    CSeq2 = CSeq + 1,
    case CSeq2 =< QSeq of
        true -> % something reday
            case ets:lookup(QTab, CSeq2) of
                [] -> % entry removed, due to previously being unroutable
                    C2 = C#c{skips = C#c.skips + 1, cseq = CSeq2},
                    maybe_pull(QTab, QSeq, C2, CsNames, DeliverFun);
                [QEntry] ->
                    {CSeq2, _NumItems, _Bin, _Meta} = QEntry,
                    case C#c.deliver of
                        undefined ->
                            % if the item can't be delivered due to cascading rt, or filtering,
                            % just keep trying.
                            {Res, C2} = maybe_deliver_item(C#c{deliver = DeliverFun}, QEntry),
                            case (Res == skipped) or (Res == filtered) or (Res == acked) of
                                true ->
                                    C3 = C2#c{deliver = C#c.deliver, delivery_funs = C#c.delivery_funs},
                                    maybe_pull(QTab, QSeq, C3, CsNames, DeliverFun);
                                false ->
                                    C2
                            end;
                        %% we have a saved function due to being at the head of the queue, just add the function and let the push
                        %% functionality push the items out to the helpers using the saved deliverfuns
                        _ ->
                            save_consumer(C, DeliverFun)
                    end
            end;
        false ->
            %% consumer is up to date with head, keep deliver function
            %% until something pushed
            save_consumer(C, DeliverFun)
    end.



maybe_deliver_item(#c{name = Name} = C, {_Seq, _NumItems, _Bin, Meta} = QEntry) ->
    Routed = meta_get(routed_clusters, [], Meta),
    Filtered = meta_get(filtered_clusters, [], Meta),
    Acked = meta_get(acked_clusters, [], Meta),

    IsRouted = lists:member(Name, Routed),
    IsFiltered = lists:member(Name, Filtered),
    IsAcked = lists:member(Name, Acked),

    maybe_deliver_item(C, QEntry, IsRouted, IsFiltered, IsAcked).

%% IsRouted = true
maybe_deliver_item(#c{deliver = undefined} = C, _QEntry, true, _, _) ->
    {skipped, C};
maybe_deliver_item(#c{delivered = Delivered} = C, {Seq, _NumItems, _Bin, _Meta}, true, _, _) ->
    case Delivered of
        true ->
            Skipped = C#c.skips + 1,
            {skipped, C#c{skips = Skipped, cseq = Seq}};
        false ->
            {skipped, C#c{cseq = Seq}}
    end;

%% IsFiltered = true
maybe_deliver_item(#c{deliver = undefined} = C, _QEntry, _, true, _) ->
    {filtered, C};
maybe_deliver_item(C, {Seq, _NumItems, _Bin, _Meta}, _, true, _) ->
    {filtered, C#c{cseq = Seq, filtered = C#c.filtered + 1, delivered = true, skips=0}};

%% IsAcked = true (so this would have been sent via repl_proxy / repl_migration)
maybe_deliver_item(#c{deliver = undefined} = C, _QEntry, _, _, true) ->
    {acked, C};
maybe_deliver_item(C, {Seq, _NumItems, _Bin, _Meta}, _, _, true) ->
    {acked, C#c{cseq = Seq}};

%% NotAcked, NotFiltered, NotRouted (Send)
maybe_deliver_item(#c{deliver = undefined} = C, _QEntry, _, _, _) ->
    {no_fun, C};
maybe_deliver_item(C, QEntry, _, _, _) ->
    {delivered, deliver_item(C, C#c.deliver, QEntry)}.


deliver_item(C, DeliverFun, {Seq, _NumItems, _Bin, _Meta} = QEntry) ->
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

% if nothing has been delivered, the sink assumes nothing was skipped
% fulfill that expectation.
set_skip_meta(QEntry, _Seq, _C = #c{delivered = false}) ->
    set_meta(QEntry, skip_count, 0);
set_skip_meta(QEntry, _Seq, _C = #c{skips = S}) ->
    set_meta(QEntry, skip_count, S).

set_meta({Seq, NumItems, Bin, Meta}, Key, Value) ->
    Meta2 = orddict:store(Key, Value, Meta),
    {Seq, NumItems, Bin, Meta2}.

%% ================================================================================================================== %%
ack_sync(Name, Seq) ->
    gen_server:call(?SERVER, {ack_sync, Name, Seq, os:timestamp()}, infinity).

handle_call({ack_sync, Name, Seq, Ts}, _From, State) ->
    {reply, ok, ack_seq(Name, Seq, Ts, State)}.

%% ================================================================================================================== %%

handle_call(status, _From, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->

    MaxBytes = get_queue_max_bytes(State),

    Consumers =
        [{Name, [{consumer_qbytes, CBytes},
            {consumer_max_qbytes, get_consumer_max_bytes(C)},
            {pending, QSeq - CSeq},  % items to be send
            {unacked, CSeq - ASeq - Skips},  % sent items requiring ack
            {c_drops, CDrops},
            {q_drops, QDrops},
            {drops, Drops},          % number of dropped entries due to max bytes
            {errs, Errs},            % number of non-ok returns from deliver fun
            {filtered, Filtered}]}    % number of objects filtered

            || #c{name = Name, aseq = ASeq, cseq = CSeq, skips = Skips, q_drops = QDrops, c_drops = CDrops, drops = Drops,
            filtered = Filtered, errs = Errs, consumer_qbytes = CBytes} = C <- Cs],

    GetTrimmedFolsomMetrics =
        fun(MetricName) ->
            lists:foldl(fun
                            ({min, Value}, Acc) -> [{latency_min, Value} | Acc];
                            ({max, Value}, Acc) -> [{latency_max, Value} | Acc];
                            ({percentile, Value}, Acc) -> [{latency_percentile, Value} | Acc];
                            (_, Acc) -> Acc
                        end, [], folsom_metrics:get_histogram_statistics(MetricName))
        end,


    UpdatedConsumerStats = lists:foldl(fun(ConsumerName, Acc) ->
        MetricName = {latency, ConsumerName},
        case folsom_metrics:metric_exists(MetricName) of
            true ->
                case lists:keytake(ConsumerName, 1, Acc) of
                    {value, {ConsumerName, ConsumerStats}, Rest} ->
                        [{ConsumerName, ConsumerStats ++ GetTrimmedFolsomMetrics(MetricName)} | Rest];
                    _ ->
                        Acc
                end;
            false ->
                Acc
        end
                                       end, Consumers, [ C#c.name || C <- Cs ]),

    Status =
        [{bytes, qbytes(QTab, State)},
            {max_bytes, MaxBytes},
            {consumers, UpdatedConsumerStats},
            {overload_drops, State#state.overload_drops}],
    {reply, Status, State};

-ifdef(TEST).
qbytes(_QTab, #state{qsize_bytes = QSizeBytes}) ->
    %% when EQC testing, don't account for ETS overhead
    QSizeBytes.

-else.
get_queue_max_bytes(#state{default_max_bytes = Default}) ->
    case riak_core_metadata:get(?RIAK_REPL2_RTQ_CONFIG_KEY, queue_max_bytes) of
        undefined -> Default;
        MaxBytes -> MaxBytes
    end.

get_consumer_max_bytes(#c{name = Name}) ->
    riak_core_metadata:get(?RIAK_REPL2_RTQ_CONFIG_KEY, {consumer_max_bytes, Name}).
-endif.


%% ================================================================================================================== %%
record_consumer_latency(Name, OldLastSeen, SeqNumber, NewTimestamp) ->
    case OldLastSeen of
        {SeqNumber, OldTimestamp} ->
            folsom_metrics:notify({{latency, Name}, abs(timer:now_diff(NewTimestamp, OldTimestamp))});
        _ ->
            % Don't log for a non-matching seq number
            skip
    end.
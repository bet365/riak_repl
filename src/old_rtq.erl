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
%%TODO might need to put this back for testing purposes
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


%% TODO this has to go back into riak_repl2_rtq! for testing purposes
ack_sync(Name, Seq) ->
    gen_server:call(?SERVER, {ack_sync, Name, Seq, os:timestamp()}, infinity).

handle_call({ack_sync, Name, Seq, Ts}, _From, State) ->
    {reply, ok, ack_seq(Name, Seq, Ts, State)}.

%% ================================================================================================================== %%
record_consumer_latency(Name, OldLastSeen, SeqNumber, NewTimestamp) ->
    case OldLastSeen of
        {SeqNumber, OldTimestamp} ->
            folsom_metrics:notify({{latency, Name}, abs(timer:now_diff(NewTimestamp, OldTimestamp))});
        _ ->
            % Don't log for a non-matching seq number
            skip
    end.

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




%% ================================================================================================================== %%



%% ================================================================================================================== %%
pull_sync(Name, DeliverFun) ->
    gen_server:call(?SERVER, {pull_with_ack, Name, DeliverFun}, infinity).


handle_call({pull_with_ack, Name, DeliverFun}, _From, State) ->
    {reply, ok, pull(Name, DeliverFun, State)};
%% ================================================================================================================== %%


%% ================================================================================================================== %%
record_consumer_latency(Name, OldLastSeen, SeqNumber, NewTimestamp) ->
    case OldLastSeen of
        {SeqNumber, OldTimestamp} ->
            folsom_metrics:notify({{latency, Name}, abs(timer:now_diff(NewTimestamp, OldTimestamp))});
        _ ->
            % Don't log for a non-matching seq number
            skip
    end.

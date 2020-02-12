-module(riak_repl2_rtq_router).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(_Request, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
filter_remotes(AllRemoteNames, PreCompleted, Meta) ->
    Routed = meta_get(routed_clusters, [], Meta),
    Filtered = riak_repl2_object_filter:realtime_blacklist(Meta),
    Completed = lists:usort(PreCompleted ++ Routed ++ Filtered),
    Send = AllRemoteNames -- Completed,
    {Send, Completed}.

meta_get(Key, Default, Meta) ->
    case orddict:find(Key, Meta) of
        error -> Default;
        {ok, Value} -> Value
    end.

set_local_forwards_meta(LocalForwards, Meta) ->
    orddict:store(local_forwards, LocalForwards, Meta).

set_skip_meta(Meta) ->
    orddict:store(skip_count, 0, Meta).

push_to_rtqs([], _Seq, State) ->
    State;
push_to_rtqs([RemoteName | Rest], Seq, State = #state{remotes = Remotes}) ->
    case lists:keyfind(RemoteName, #remote.name, Remotes) of
        {_, Remote} ->
            riak_repl2_rtq:push(Name, Seq);
        _ ->
            ok
    end,
    push_to_remotes(Rest, Seq, State).
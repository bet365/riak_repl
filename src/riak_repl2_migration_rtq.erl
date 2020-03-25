-module(riak_repl2_migration_rtq).
-include("riak_repl.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([status/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% We name this riak_repl2_rtq, to support the old method of sending objects over to another node on migration
-define(SERVER, riak_repl2_rtq).
-record(state, {migrated = 0}).

status() ->
    gen_server:call(?SERVER, status, ?LONG_TIMEOUT).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.


handle_call(status, _From, State = #state{migrated = N}) ->
    Migrated = [{migrated, N}],
    {reply, Migrated, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({push, NumItems, Bin}, State) ->
    handle_cast({push, NumItems, Bin, [], []}, State);
handle_cast({push, NumItems, Bin, Meta}, State) ->
    handle_cast({push, NumItems, Bin, Meta, []}, State);
handle_cast({push, NumItems, Bin, Meta, PreCompleted}, State = #state{migrated = N}) ->
    Concurrency = riak_repl_util:get_rtq_concurrency(),
    Hash = erlang:phash2(Bin, Concurrency) +1,
    riak_repl2_rtq:push(Hash, NumItems, Bin, Meta, PreCompleted),
    {noreply, State#state{migrated = N+1}};

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

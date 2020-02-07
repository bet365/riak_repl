-module(riak_repl2_reference_rtq).
-behaviour(gen_server).

%% API
-export(
[
    start_link/1,
    restart_sending/1
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state,
{
    name,
    consumers,
    remote_tab = ets:new(?MODULE, [protected, ordered_set]),
    rtq_tab,
    qseq

}).

% Consumers
-record(consumer,
{
    pid,
    aseq = 0,
    cseq = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

restart_sending(Pid) ->
    gen_server:cast(Pid, restart_sending).

pull(Pid, ConnPid)->
    gen_server:cast(Pid, {pull, ConnPid}).

%% pull, if at head of queue store the pid
%% when we receive restart_sending, send as many objects to as many pids as possible
%% when we ack, this should act as another pull

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name]) ->
    %% register to riak_repl2_rtq
    {QTab, QSeq} = riak_repl2_rtq:register(Name),
    {ok, #state{name = Name, qtab = QTab, qseq = QSeq}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({pull, Pid}, State) ->
    {noreply, State};

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


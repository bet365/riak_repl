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
    name
}).

% Consumers
-record(consumer,
{
    name,      % consumer name
    aseq = 0,  % last sequence acked
    cseq = 0,  % last sequence sent
    skips = 0,
    filtered = 0,
    q_drops = 0, % number of dropped queue entries (not items)
    c_drops = 0,
    drops = 0,
    errs = 0,  % delivery errors
    deliver,  % deliver function if pending, otherwise undefined
    delivery_funs = [],
    delivered = false,  % used by the skip count.
    % skip_count is used to help the sink side determine if an item has
    % been dropped since the last delivery. The sink side can't
    % determine if there's been drops accurately if the source says there
    % were skips before it's sent even one item.
    last_seen,  % a timestamp of when we received the last ack to measure latency
    consumer_qbytes = 0
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

restart_sending(Pid) ->
    gen_server:cast(Pid, restart_sending).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name]) ->
    %% register to riak_repl2_rtq
    S = self(),
    riak_repl2_rtq:register(Name, S),
    {ok, #state{name = Name}}.

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


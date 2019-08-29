-module(riak_repl2_rtrq).

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

-export([
    register/1,
    unregister/1,
    push/2,
    push/3,
    status/0,
    ack/2,
    shutdown/0,
    stop/0
]).

-define(overload_ets, rtq_overload_ets).
-define(SERVER, ?MODULE).
-define(DEFAULT_OVERLOAD, 2000).
-define(DEFAULT_RECOVER, 1000).
-define(DEFAULT_RTQ_LATENCY_SLIDING_WINDOW, 300).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

register(Name) ->
    gen_server:call(?SERVER, {register, Name}, infinity).

unregister(Name) ->
    gen_server:call(?SERVER, {unregister, Name}, infinity).

push(NumItems, Bin) ->
    push(NumItems, Bin, []).
push(NumItems, Bin, Meta) ->
    [{overloaded, Overloaded}] = ets:lookup(?overload_ets, overloaded),
    case Overloaded of
        true ->
            lager:debug("rtq overloaded"),
            riak_repl2_rtq_overload_counter:drop();
        false ->
            gen_server:cast(?SERVER, {push, NumItems, Bin, Meta})
    end.

status() ->
    Status = gen_server:call(?SERVER, status, infinity),
    % I'm having the calling process do derived stats because
    % I don't want to block the rtq from processing objects.
    MaxBytes = proplists:get_value(max_bytes, Status),
    CurrentBytes = proplists:get_value(bytes, Status),
    PercentBytes = round( (CurrentBytes / MaxBytes) * 100000 ) / 1000,
    [{percent_bytes_used, PercentBytes} | Status].

ack(Name, Seq) ->
    gen_server:cast(?SERVER, {ack, Name, Seq, os:timestamp()}).

shutdown() ->
    gen_server:call(?SERVER, shutting_down, infinity).

stop() ->
    gen_server:call(?SERVER, stop, infinity).


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

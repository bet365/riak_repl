%% @doc Simply takes messages from rtq pushes that are dropped and counts them.
%% At a configurable interval it tells the rtq how many have been dropped since
%% last interval. If nothing has been dropped, no message is sent.
-module(riak_repl2_rtq_overload_counter).
-behavior(gen_server).

-define(DEFAULT_INTERVAL, 5000).

-record(state, {
    % number of drops since last report
    drops_1 = 0 :: non_neg_integer(),
    drops_2 = 0 :: non_neg_integer(),
    % how often (in milliseconds) to report drops to rtq.
    interval :: pos_integer(),
    % timer reference for interval
    timer
}).

-export([start_link/0, start_link/1, stop/0]).
-export([drop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% API

%% @doc Start linked and registered as module name with default options.
start_link() ->
    SendInterval = app_helper:get_env(riak_repl, rtq_drop_report_interval, ?DEFAULT_INTERVAL),
    start_link([{report_interval, SendInterval}]).

%% @doc Start linked and registered as the module name with the given options.
start_link(Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Options, []).

%% @doc Stop the counter gracefully.
stop() ->
    gen_server:cast(?MODULE, stop).

%% @private
drop(X) ->
    gen_server:cast(?MODULE, {drop, X}).

%% gen_server

%% @private
init(Options) ->
    Report = proplists:get_value(report_interval, Options, ?DEFAULT_INTERVAL),
    {ok, #state{interval = Report}}.

handle_call(_Msg, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast({drop, X}, #state{timer = undefined} = State) ->
    {ok, Timer} = timer:send_after(State#state.interval, report_drops),
    {noreply, dropped(X, State#state{timer = Timer})};

handle_cast({drop, X}, State) ->
    {noreply, dropped(X, State)};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(report_drops, State) ->
    lager:debug("reporting drops_1: ~p", [State#state.drops_1]),
    lager:debug("reporting drops_2: ~p", [State#state.drops_2]),
    riak_repl2_rtq:report_drops(State#state.drops_1, 1),
    riak_repl2_rtq:report_drops(State#state.drops_2, 2),
    {noreply, State#state{drops_1 = 0, drops_2 = 0, timer = undefined}};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Why, _State) ->
    ok.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.

%% internal

dropped(X, #state{drops_1 = N1, drops_2 = N2} = State) ->
    case X of
        1 -> State#state{drops_1 = N1+1};
        2 -> State#state{drops_2 = N2+1};
        _ -> State
    end.

-module(fenrir_drift_detector).
-behaviour(gen_server).

%% Format drift detection. Keeps a sliding window of the latest confidences
%% per signature. When the window is full and its mean drops below the
%% threshold, the source format is considered to have drifted → learning must
%% be re-triggered. This is the self-healing mechanism.

-export([start_link/0, start_link/1, record/2, drifting/1, window/1, reset/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

%% Sig -> [Conf]  (the last WindowSize values, most recent first)
-record(state, {tab, size, threshold}).

start_link() -> start_link(#{}).
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

record(Sig, Conf) -> gen_server:call(?MODULE, {record, Sig, Conf}).
drifting(Sig)     -> gen_server:call(?MODULE, {drifting, Sig}).
window(Sig)       -> gen_server:call(?MODULE, {window, Sig}).
reset(Sig)        -> gen_server:call(?MODULE, {reset, Sig}).

init(Opts) ->
    Tab = ets:new(fenrir_drift, [set, private]),
    {ok, #state{tab = Tab,
                size = maps:get(window_size, Opts, 10),
                threshold = maps:get(threshold, Opts, 0.9)}}.

handle_call({record, Sig, Conf}, _From, S) ->
    Win = get_window(S#state.tab, Sig),
    Win2 = lists:sublist([Conf | Win], S#state.size),
    ets:insert(S#state.tab, {Sig, Win2}),
    {reply, ok, S};

handle_call({drifting, Sig}, _From, S) ->
    Win = get_window(S#state.tab, Sig),
    Drift = length(Win) >= S#state.size andalso mean(Win) < S#state.threshold,
    {reply, Drift, S};

handle_call({window, Sig}, _From, S) ->
    {reply, get_window(S#state.tab, Sig), S};

handle_call({reset, Sig}, _From, S) ->
    ets:insert(S#state.tab, {Sig, []}),
    {reply, ok, S}.

handle_cast(_, S) -> {noreply, S}.
terminate(_, _) -> ok.

get_window(Tab, Sig) ->
    case ets:lookup(Tab, Sig) of
        [{Sig, W}] -> W;
        []         -> []
    end.

mean([]) -> 1.0;
mean(L)  -> lists:sum(L) / length(L).

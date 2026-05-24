-module(fenrir_drift_detector).
-behaviour(gen_server).

%% Format drift detection. Keeps a sliding window of the latest confidences
%% per signature. When the window is full and its mean drops below the
%% threshold, the source format is considered to have drifted → learning must
%% be re-triggered. This is the self-healing mechanism.

-export([start_link/0, start_link/1, record/2, drifting/1, window/1, reset/1,
         drifting_signatures/0, would_drift/3]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

%% Sig -> [Conf]  (the last WindowSize values, most recent first)
-record(state, {tab, size, threshold, notify}).

start_link() -> start_link(#{}).
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

record(Sig, Conf) -> gen_server:call(?MODULE, {record, Sig, Conf}).
drifting(Sig)     -> gen_server:call(?MODULE, {drifting, Sig}).
window(Sig)       -> gen_server:call(?MODULE, {window, Sig}).
reset(Sig)        -> gen_server:call(?MODULE, {reset, Sig}).
drifting_signatures() -> gen_server:call(?MODULE, drifting_signatures).

init(Opts) ->
    Tab = ets:new(fenrir_drift, [set, private]),
    {ok, #state{tab = Tab,
                size = maps:get(window_size, Opts, 10),
                threshold = maps:get(threshold, Opts, 0.9),
                notify = maps:get(notify, Opts, undefined)}}.

handle_call({record, Sig, Conf}, _From, S) ->
    Win = get_window(S#state.tab, Sig),
    WasDrifting = is_drift(Win, S),
    Win2 = lists:sublist([Conf | Win], S#state.size),
    ets:insert(S#state.tab, {Sig, Win2}),
    NowDrifting = is_drift(Win2, S),
    case (not WasDrifting) andalso NowDrifting of
        true  -> notify(S#state.notify, {drift, Sig});
        false -> ok
    end,
    {reply, ok, S};

handle_call({drifting, Sig}, _From, S) ->
    {reply, is_drift(get_window(S#state.tab, Sig), S), S};

handle_call(drifting_signatures, _From, S) ->
    Sigs = [Sig || {Sig, Win} <- ets:tab2list(S#state.tab), is_drift(Win, S)],
    {reply, Sigs, S};

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

is_drift(Win, S) ->
    would_drift(Win, S#state.size, S#state.threshold).

%% Pure drift predicate (shared with model checking).
would_drift(Win, Size, Threshold) ->
    length(Win) >= Size andalso mean(Win) < Threshold.

notify(undefined, _Msg) -> ok;
notify(Target, Msg)     -> catch Target ! Msg, ok.

mean([]) -> 1.0;
mean(L)  -> lists:sum(L) / length(L).

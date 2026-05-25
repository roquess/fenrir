-module(fenrir_metrics).
-behaviour(gen_server).

%% Dependency-free observability aggregator. Cumulative counters are pushed via
%% incr/1 (fire-and-forget, a no-op when this server is not started). Live
%% per-signature state is pulled on demand by snapshot/0.

-export([start_link/0, start_link/1, incr/1, counters/0, snapshot/0, reset/0]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-define(ZERO, #{heals => 0, quarantines => 0, drifts => 0, runs => 0}).
-record(st, {counters = ?ZERO, started}).

start_link() -> start_link(#{}).
start_link(_Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Fire-and-forget; gen_server:cast to an unregistered name is already a silent
%% no-op, and the catch is belt-and-suspenders.
incr(Key)  -> catch gen_server:cast(?MODULE, {incr, Key}), ok.
counters() -> gen_server:call(?MODULE, counters).
snapshot() -> gen_server:call(?MODULE, snapshot).
reset()    -> gen_server:call(?MODULE, reset).

init([]) ->
    {ok, #st{started = erlang:monotonic_time(millisecond)}}.

handle_call(counters, _From, S) ->
    {reply, S#st.counters, S};
handle_call(snapshot, _From, S) ->
    Uptime = erlang:monotonic_time(millisecond) - S#st.started,
    {reply, #{uptime_ms => Uptime,
              totals => S#st.counters,
              signatures => signatures_snapshot()}, S};
handle_call(reset, _From, S) ->
    {reply, ok, S#st{counters = ?ZERO,
                     started = erlang:monotonic_time(millisecond)}}.

handle_cast({incr, Key}, S) ->
    C = maps:update_with(Key, fun(V) -> V + 1 end, 1, S#st.counters),
    {noreply, S#st{counters = C}};
handle_cast(_, S) ->
    {noreply, S}.

terminate(_, _) -> ok.

%% Per-signature live aggregation, pulled on demand.
signatures_snapshot() ->
    case catch fenrir_recipe_store:signatures() of
        Sigs when is_list(Sigs) ->
            maps:from_list([{Sig, sig_info(Sig)} || Sig <- Sigs]);
        _ ->
            #{}
    end.

sig_info(Sig) ->
    #{mean    => safe(fun() -> fenrir_confidence_monitor:mean(Sig) end, 1.0),
      dead    => safe(fun() -> length(fenrir_confidence_monitor:dead_letters(Sig)) end, 0),
      status  => safe(fun() -> fenrir_healer:status(Sig) end, healthy),
      version => safe(fun() -> length(fenrir_recipe_store:history(Sig)) end, 0)}.

safe(F, Default) ->
    try F() of R -> R catch _:_ -> Default end.

-module(fenrir_confidence_monitor).
-behaviour(gen_server).

%% Collects confidence per record and per signature. Records below the
%% threshold are routed to dead-letters. Decides when an escalation
%% (re-learning) is needed.

-export([start_link/0, start_link/1, observe/3, mean/1, dead_letters/1,
         needs_escalation/1, reset/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

%% Sig -> {Count, SumConf, DeadLetters (list of lines, arrival order)}
-record(state, {tab, min_conf, min_samples}).

start_link() -> start_link(#{}).
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

observe(Sig, Line, Conf)  -> gen_server:call(?MODULE, {observe, Sig, Line, Conf}).
mean(Sig)                 -> gen_server:call(?MODULE, {mean, Sig}).
dead_letters(Sig)         -> gen_server:call(?MODULE, {dead_letters, Sig}).
needs_escalation(Sig)     -> gen_server:call(?MODULE, {needs_escalation, Sig}).
reset(Sig)                -> gen_server:call(?MODULE, {reset, Sig}).

init(Opts) ->
    Tab = ets:new(fenrir_confidence, [set, private]),
    {ok, #state{tab = Tab,
                min_conf = maps:get(min_conf, Opts, 0.95),
                min_samples = maps:get(min_samples, Opts, 1)}}.

handle_call({observe, Sig, Line, Conf}, _From, S) ->
    {C, Sum, DL} = entry(S#state.tab, Sig),
    DL2 = case Conf < S#state.min_conf of
              true  -> DL ++ [Line];
              false -> DL
          end,
    ets:insert(S#state.tab, {Sig, {C + 1, Sum + Conf, DL2}}),
    {reply, ok, S};

handle_call({mean, Sig}, _From, S) ->
    {C, Sum, _} = entry(S#state.tab, Sig),
    {reply, case C of 0 -> 1.0; _ -> Sum / C end, S};

handle_call({dead_letters, Sig}, _From, S) ->
    {_, _, DL} = entry(S#state.tab, Sig),
    {reply, DL, S};

handle_call({needs_escalation, Sig}, _From, S) ->
    {C, Sum, _} = entry(S#state.tab, Sig),
    Mean = case C of 0 -> 1.0; _ -> Sum / C end,
    Need = C >= S#state.min_samples andalso Mean < S#state.min_conf,
    {reply, Need, S};

handle_call({reset, Sig}, _From, S) ->
    ets:insert(S#state.tab, {Sig, {0, 0.0, []}}),
    {reply, ok, S}.

handle_cast(_, S) -> {noreply, S}.
terminate(_, _) -> ok.

entry(Tab, Sig) ->
    case ets:lookup(Tab, Sig) of
        [{Sig, E}] -> E;
        []         -> {0, 0.0, []}
    end.

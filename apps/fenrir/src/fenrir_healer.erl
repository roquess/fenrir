-module(fenrir_healer).
-behaviour(gen_server).

%% Autonomous self-healing loop. Reacts to format drift (push notification from
%% fenrir_drift_detector + periodic reconciliation tick), re-learns via
%% fenrir_job:escalate, and rolls the recipe forward only if it improves.
%% Quarantines a signature (no auto-retry, emits {needs_attention, Sig}) when
%% re-learning cannot improve. Single process → heals are serialized.

-export([start_link/0, start_link/1, status/1, clear/1, heal/1, reconcile_now/0,
         should_heal/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% status :: #{Sig => drifting | healing | healed | quarantined}
%% (absence of a key means healthy)
-record(state, {nif, notify, reconcile_ms, status = #{}}).

start_link() -> start_link(#{}).
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

status(Sig)     -> gen_server:call(?MODULE, {status, Sig}).
clear(Sig)      -> gen_server:call(?MODULE, {clear, Sig}).
heal(Sig)       -> gen_server:call(?MODULE, {heal, Sig}).
reconcile_now() -> gen_server:call(?MODULE, reconcile_now).

%% Pure re-entry guard: a heal may start only if the signature is not already
%% being healed or quarantined.
should_heal(healing)     -> false;
should_heal(quarantined) -> false;
should_heal(_)           -> true.

init(Opts) ->
    Nif = maps:get(nif, Opts, default_nif()),
    Notify = maps:get(notify, Opts, undefined),
    Recon = maps:get(reconcile_ms, Opts, 5000),
    erlang:send_after(Recon, self(), reconcile),
    {ok, #state{nif = Nif, notify = Notify, reconcile_ms = Recon}}.

handle_call({status, Sig}, _From, S) ->
    {reply, status_of(Sig, S), S};
handle_call({clear, Sig}, _From, S) ->
    {reply, ok, set_status(Sig, healthy, S)};
handle_call({heal, Sig}, _From, S) ->
    {Result, S2} = do_heal(Sig, S),
    {reply, Result, S2};
handle_call(reconcile_now, _From, S) ->
    {reply, ok, reconcile(S)}.

handle_cast(_, S) -> {noreply, S}.

handle_info({drift, Sig}, S) ->
    {_Result, S2} = do_heal(Sig, S),
    {noreply, S2};
handle_info(reconcile, S) ->
    S2 = reconcile(S),
    erlang:send_after(S#state.reconcile_ms, self(), reconcile),
    {noreply, S2};
handle_info(_, S) -> {noreply, S}.

terminate(_, _) -> ok.

%% ---- internals ----

default_nif() -> fenrir:nif().

status_of(Sig, S) -> maps:get(Sig, S#state.status, healthy).

set_status(Sig, healthy, S) ->
    S#state{status = maps:remove(Sig, S#state.status)};
set_status(Sig, St, S) ->
    S#state{status = maps:put(Sig, St, S#state.status)}.

%% Attempt a heal, respecting the re-entry guard.
do_heal(Sig, S) ->
    case should_heal(status_of(Sig, S)) of
        false -> {ignored, S};
        true ->
            S1 = set_status(Sig, healing, S),
            try attempt_heal(Sig, S1#state.nif) of
                healed ->
                    {healed, set_status(Sig, healed, S1)};
                quarantined ->
                    notify(S1#state.notify, {needs_attention, Sig}),
                    logger:warning("fenrir_healer: ~p quarantined", [Sig]),
                    {quarantined, set_status(Sig, quarantined, S1)};
                no_change ->
                    {no_change, set_status(Sig, healthy, S1)}
            catch
                Class:Reason ->
                    logger:warning("fenrir_healer: heal ~p failed ~p:~p",
                                   [Sig, Class, Reason]),
                    {error, set_status(Sig, healthy, S1)}
            end
    end.

%% The heal action — reuses the existing escalation machinery.
attempt_heal(Sig, Nif) ->
    case fenrir_recipe_store:get(Sig) of
        not_found -> no_change;
        {ok, Recipe} ->
            DeadLetters = fenrir_confidence_monitor:dead_letters(Sig),
            case fenrir_job:escalate(Recipe, DeadLetters, Nif) of
                no_change -> no_change;
                {improved, Patched, _Old, _New} ->
                    ok = fenrir_recipe_store:put(Sig, Patched),
                    fenrir_confidence_monitor:reset(Sig),
                    fenrir_drift_detector:reset(Sig),
                    healed;
                {rejected, _R, _Old, _New} ->
                    quarantined
            end
    end.

%% Reconciliation tick: recover settled signatures, heal any missed drift.
reconcile(S) ->
    %% Recovery: clear healed/quarantined signatures no longer drifting.
    S1 = maps:fold(
           fun(Sig, St, Acc) when St =:= healed; St =:= quarantined ->
                   case fenrir_drift_detector:drifting(Sig) of
                       false -> set_status(Sig, healthy, Acc);
                       true  -> Acc
                   end;
              (_Sig, _St, Acc) -> Acc
           end, S, S#state.status),
    %% Heal: drifting signatures not currently healing/quarantined.
    Drifting = fenrir_drift_detector:drifting_signatures(),
    lists:foldl(
      fun(Sig, Acc) ->
          case should_heal(status_of(Sig, Acc)) of
              false -> Acc;
              true  -> {_R, Acc2} = do_heal(Sig, Acc), Acc2
          end
      end, S1, Drifting).

notify(undefined, _Msg) -> ok;
notify(Target, Msg)     -> catch Target ! Msg, ok.

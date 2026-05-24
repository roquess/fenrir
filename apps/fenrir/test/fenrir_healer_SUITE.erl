-module(fenrir_healer_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([should_heal_guard/1, default_status_healthy/1, clear_resets_status/1,
         heal_improves_bumps_recipe/1, unrepairable_is_quarantined/1,
         quarantined_skips_reheal/1, recovery_clears_quarantine/1,
         tick_heals_missed_drift/1, push_path_heals_e2e/1]).
-include_lib("common_test/include/ct.hrl").

all() ->
    [should_heal_guard, default_status_healthy, clear_resets_status,
     heal_improves_bumps_recipe, unrepairable_is_quarantined,
     quarantined_skips_reheal, recovery_clears_quarantine,
     tick_heals_missed_drift, push_path_heals_e2e].

init_per_testcase(_, Config) ->
    {ok, S} = fenrir_recipe_store:start_link(#{disk => false}),
    {ok, C} = fenrir_confidence_monitor:start_link(#{min_conf => 0.95, min_samples => 1}),
    {ok, D} = fenrir_drift_detector:start_link(#{window_size => 2, threshold => 0.9}),
    [{pids, [S, C, D]} | Config].

end_per_testcase(_, _Config) ->
    [catch gen_server:stop(P)
     || P <- [fenrir_healer, fenrir_drift_detector,
              fenrir_confidence_monitor, fenrir_recipe_store]],
    ok.

%% ---- mocks / helpers ----

%% relearn always yields "new" (parsed at 1.0) → escalate improves.
improving_nif() ->
    #{parse_line => fun(Json, _L) ->
                        case Json of <<"new">> -> {<<"{}">>, 1.0};
                                     _         -> {<<"{}">>, 0.5} end
                    end,
      relearn => fun(_Prev, _Corpus) -> <<"new">> end}.

%% relearn never improves (always "old", parsed at 0.5) → escalate rejects.
stuck_nif() ->
    #{parse_line => fun(_Json, _L) -> {<<"{}">>, 0.5} end,
      relearn => fun(_Prev, _Corpus) -> <<"old">> end}.

seed(Sig) ->
    Recipe = #{<<"signature">> => Sig, <<"json">> => <<"old">>,
               <<"sample">> => <<"name;age\nAlice;30\n">>},
    ok = fenrir_recipe_store:put(Sig, Recipe),
    ok = fenrir_confidence_monitor:observe(Sig, <<"Bob;x">>, 0.5),
    ok = fenrir_confidence_monitor:observe(Sig, <<"Eve;y">>, 0.5),
    Sig.

%% ---- tests ----

should_heal_guard(_) ->
    true  = fenrir_healer:should_heal(healthy),
    true  = fenrir_healer:should_heal(drifting),
    true  = fenrir_healer:should_heal(healed),
    false = fenrir_healer:should_heal(healing),
    false = fenrir_healer:should_heal(quarantined).

default_status_healthy(_) ->
    {ok, _} = fenrir_healer:start_link(#{nif => #{}, notify => self()}),
    healthy = fenrir_healer:status(<<"unknown">>).

clear_resets_status(_) ->
    {ok, _} = fenrir_healer:start_link(#{nif => #{}, notify => self()}),
    ok = fenrir_healer:clear(<<"x">>),
    healthy = fenrir_healer:status(<<"x">>).

heal_improves_bumps_recipe(_) ->
    {ok, _} = fenrir_healer:start_link(#{nif => improving_nif(), notify => self()}),
    Sig = seed(<<"sig1">>),
    healed = fenrir_healer:heal(Sig),
    healed = fenrir_healer:status(Sig),
    {ok, R} = fenrir_recipe_store:get(Sig),
    <<"new">> = maps:get(<<"json">>, R).

unrepairable_is_quarantined(_) ->
    {ok, _} = fenrir_healer:start_link(#{nif => stuck_nif(), notify => self()}),
    Sig = seed(<<"sig2">>),
    quarantined = fenrir_healer:heal(Sig),
    quarantined = fenrir_healer:status(Sig),
    receive {needs_attention, <<"sig2">>} -> ok
    after 1000 -> ct:fail(no_needs_attention) end.

quarantined_skips_reheal(_) ->
    {ok, _} = fenrir_healer:start_link(#{nif => stuck_nif(), notify => self()}),
    Sig = seed(<<"sig3">>),
    quarantined = fenrir_healer:heal(Sig),
    ignored = fenrir_healer:heal(Sig).

recovery_clears_quarantine(_) ->
    {ok, _} = fenrir_healer:start_link(#{nif => stuck_nif(), notify => self()}),
    Sig = seed(<<"sig4">>),
    quarantined = fenrir_healer:heal(Sig),
    %% Sig is not drifting in the detector (no records there) → reconcile clears.
    ok = fenrir_healer:reconcile_now(),
    healthy = fenrir_healer:status(Sig).

tick_heals_missed_drift(_) ->
    %% No notify target: push is off, only the tick can heal.
    {ok, _} = fenrir_healer:start_link(#{nif => improving_nif()}),
    Sig = seed(<<"sig5">>),
    ok = fenrir_drift_detector:record(Sig, 0.1),
    ok = fenrir_drift_detector:record(Sig, 0.1),
    true = fenrir_drift_detector:drifting(Sig),
    ok = fenrir_healer:reconcile_now(),
    healed = fenrir_healer:status(Sig).

push_path_heals_e2e(Config) ->
    %% Replace the per-testcase detector (no notify) with one wired to the healer.
    [_S, _C, D0] = ?config(pids, Config),
    gen_server:stop(D0),
    {ok, _} = fenrir_healer:start_link(#{nif => improving_nif()}),
    {ok, _} = fenrir_drift_detector:start_link(
                #{window_size => 2, threshold => 0.9, notify => fenrir_healer}),
    Sig = seed(<<"sig6">>),
    ok = fenrir_drift_detector:record(Sig, 0.1),
    ok = fenrir_drift_detector:record(Sig, 0.1),
    %% The {drift,Sig} cast is processed before this synchronous status call.
    healed = fenrir_healer:status(Sig).

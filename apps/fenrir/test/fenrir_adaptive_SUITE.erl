-module(fenrir_adaptive_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([healthy_batch_no_escalation/1, degraded_batch_escalates_and_improves/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [healthy_batch_no_escalation, degraded_batch_escalates_and_improves].

init_per_testcase(_, Config) ->
    {ok, S} = fenrir_recipe_store:start_link(#{disk => false}),
    {ok, C} = fenrir_confidence_monitor:start_link(#{min_conf => 0.95, min_samples => 2}),
    {ok, D} = fenrir_drift_detector:start_link(#{window_size => 3, threshold => 0.9}),
    [{pids, [S, C, D]} | Config].

end_per_testcase(_, Config) ->
    [gen_server:stop(P) || P <- ?config(pids, Config)],
    ok.

%% A clean batch: high confidence, no escalation.
healthy_batch_no_escalation(_) ->
    Sample = <<"name;age\nAlice;30\nBob;25\n">>,
    Lines  = [<<"Carol;40">>, <<"Dan;22">>],
    Report = fenrir:ingest_adaptive(Sample, Lines),
    false = maps:get(escalated, Report).

%% A degraded batch (the age column becomes non-numeric): confidence drops,
%% Fenrir escalates, re-learns (age → String), and the patch improves confidence.
degraded_batch_escalates_and_improves(_) ->
    Sample = <<"name;age\nAlice;30\nBob;25\n">>,
    Lines  = [<<"Carol;N/A">>, <<"Dan;unknown">>, <<"Eve;n/a">>],
    Report = fenrir:ingest_adaptive(Sample, Lines),
    true  = maps:get(escalated, Report),
    true  = maps:get(improved, Report),
    Old   = maps:get(old_conf, Report),
    New   = maps:get(new_conf, Report),
    true  = (New > Old),
    %% The patched recipe is persisted (a new reusable version).
    Sig = maps:get(signature, Report),
    {ok, _} = fenrir_recipe_store:get(Sig).

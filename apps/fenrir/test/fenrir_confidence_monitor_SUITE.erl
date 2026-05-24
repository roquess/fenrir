-module(fenrir_confidence_monitor_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([tracks_mean/1, routes_low_to_dead_letters/1, triggers_escalation/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [tracks_mean, routes_low_to_dead_letters, triggers_escalation].

init_per_testcase(_, Config) ->
    {ok, Pid} = fenrir_confidence_monitor:start_link(#{min_conf => 0.9, min_samples => 2}),
    [{mon, Pid} | Config].

end_per_testcase(_, Config) ->
    gen_server:stop(?config(mon, Config)).

tracks_mean(_) ->
    ok = fenrir_confidence_monitor:observe(<<"s">>, <<"l1">>, 1.0),
    ok = fenrir_confidence_monitor:observe(<<"s">>, <<"l2">>, 0.0),
    M = fenrir_confidence_monitor:mean(<<"s">>),
    true = (abs(M - 0.5) < 1.0e-9).

routes_low_to_dead_letters(_) ->
    ok = fenrir_confidence_monitor:observe(<<"s">>, <<"good">>, 1.0),
    ok = fenrir_confidence_monitor:observe(<<"s">>, <<"bad">>, 0.3),
    [<<"bad">>] = fenrir_confidence_monitor:dead_letters(<<"s">>).

triggers_escalation(_) ->
    %% Un seul échantillon : pas encore (min_samples = 2).
    ok = fenrir_confidence_monitor:observe(<<"s">>, <<"bad">>, 0.2),
    false = fenrir_confidence_monitor:needs_escalation(<<"s">>),
    %% Deux échantillons sous le seuil → escalade.
    ok = fenrir_confidence_monitor:observe(<<"s">>, <<"bad2">>, 0.2),
    true = fenrir_confidence_monitor:needs_escalation(<<"s">>).

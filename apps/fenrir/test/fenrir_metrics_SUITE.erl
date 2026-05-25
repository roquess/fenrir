-module(fenrir_metrics_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([incr_accumulates/1, incr_is_noop_when_absent/1,
         snapshot_has_uptime_and_totals/1, snapshot_aggregates_per_signature/1,
         ingest_adaptive_counts_heal/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [incr_accumulates, incr_is_noop_when_absent,
          snapshot_has_uptime_and_totals, snapshot_aggregates_per_signature,
          ingest_adaptive_counts_heal].

-define(SERVERS, [fenrir_healer, fenrir_drift_detector,
                  fenrir_confidence_monitor, fenrir_recipe_store, fenrir_metrics]).

stop_all() -> [catch gen_server:stop(M) || M <- ?SERVERS], ok.

init_per_testcase(incr_is_noop_when_absent, Config) ->
    stop_all(),
    Config;
init_per_testcase(_, Config) ->
    stop_all(),
    {ok, _} = fenrir_metrics:start_link(),
    Config.

end_per_testcase(_, _Config) ->
    stop_all(),
    ok.

incr_accumulates(_) ->
    [fenrir_metrics:incr(heals) || _ <- [1, 2, 3]],
    fenrir_metrics:incr(drifts),
    C = fenrir_metrics:counters(),
    3 = maps:get(heals, C),
    1 = maps:get(drifts, C),
    0 = maps:get(quarantines, C).

incr_is_noop_when_absent(_) ->
    %% fenrir_metrics not started: incr must not raise and returns ok.
    ok = fenrir_metrics:incr(heals).

snapshot_has_uptime_and_totals(_) ->
    fenrir_metrics:incr(runs),
    Snap = fenrir_metrics:snapshot(),
    true = is_integer(maps:get(uptime_ms, Snap)),
    true = maps:get(uptime_ms, Snap) >= 0,
    1 = maps:get(runs, maps:get(totals, Snap)),
    true = is_map(maps:get(signatures, Snap)).

snapshot_aggregates_per_signature(_) ->
    {ok, _} = fenrir_recipe_store:start_link(#{disk => false}),
    {ok, _} = fenrir_confidence_monitor:start_link(#{min_conf => 0.95, min_samples => 1}),
    {ok, _} = fenrir_healer:start_link(#{nif => #{}}),
    Sig = <<"csv:cols=2">>,
    ok = fenrir_recipe_store:put(Sig, #{<<"version">> => 1}),
    ok = fenrir_recipe_store:put(Sig, #{<<"version">> => 2}),
    ok = fenrir_confidence_monitor:observe(Sig, <<"bad">>, 0.2),
    Snap = fenrir_metrics:snapshot(),
    Info = maps:get(Sig, maps:get(signatures, Snap)),
    true = maps:get(dead, Info) >= 1,
    healthy = maps:get(status, Info),
    2 = maps:get(version, Info).

ingest_adaptive_counts_heal(_) ->
    {ok, _} = fenrir_recipe_store:start_link(#{disk => false}),
    {ok, _} = fenrir_confidence_monitor:start_link(#{min_conf => 0.95, min_samples => 1}),
    {ok, _} = fenrir_drift_detector:start_link(#{}),
    Nif = #{parse_line => fun(Json, _L) ->
                              case Json of <<"new">> -> {<<"{}">>, 1.0};
                                           _         -> {<<"{}">>, 0.5} end
                          end,
            relearn => fun(_P, _C) -> <<"new">> end},
    {ok, _} = fenrir_healer:start_link(#{nif => Nif}),
    Sig = <<"csv:cols=2">>,
    ok = fenrir_recipe_store:put(Sig, #{<<"signature">> => Sig, <<"json">> => <<"old">>,
                                        <<"sample">> => <<"name;age\nAlice;30\n">>}),
    ok = fenrir_confidence_monitor:observe(Sig, <<"Bob;x">>, 0.5),
    ok = fenrir_confidence_monitor:observe(Sig, <<"Eve;y">>, 0.5),
    healed = fenrir_healer:heal(Sig),
    1 = maps:get(heals, fenrir_metrics:counters()).

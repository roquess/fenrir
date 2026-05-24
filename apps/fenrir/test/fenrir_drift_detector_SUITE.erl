-module(fenrir_drift_detector_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([no_drift_when_healthy/1, drift_when_window_degrades/1, window_is_bounded/1,
         notifies_target_on_drift_edge/1, enumerates_drifting_signatures/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [no_drift_when_healthy, drift_when_window_degrades, window_is_bounded,
          notifies_target_on_drift_edge, enumerates_drifting_signatures].

init_per_testcase(notifies_target_on_drift_edge, Config) -> Config;
init_per_testcase(enumerates_drifting_signatures, Config) -> Config;
init_per_testcase(_, Config) ->
    {ok, Pid} = fenrir_drift_detector:start_link(#{window_size => 3, threshold => 0.9}),
    [{d, Pid} | Config].

end_per_testcase(_, _Config) ->
    catch gen_server:stop(fenrir_drift_detector),
    ok.

no_drift_when_healthy(_) ->
    [fenrir_drift_detector:record(<<"s">>, 1.0) || _ <- lists:seq(1, 3)],
    false = fenrir_drift_detector:drifting(<<"s">>).

drift_when_window_degrades(_) ->
    %% Window not full yet: no verdict.
    ok = fenrir_drift_detector:record(<<"s">>, 0.2),
    false = fenrir_drift_detector:drifting(<<"s">>),
    %% Window full and mean low → drift.
    ok = fenrir_drift_detector:record(<<"s">>, 0.3),
    ok = fenrir_drift_detector:record(<<"s">>, 0.1),
    true = fenrir_drift_detector:drifting(<<"s">>).

window_is_bounded(_) ->
    [fenrir_drift_detector:record(<<"s">>, 1.0) || _ <- lists:seq(1, 10)],
    3 = length(fenrir_drift_detector:window(<<"s">>)).

notifies_target_on_drift_edge(_) ->
    {ok, _} = fenrir_drift_detector:start_link(
                #{window_size => 2, threshold => 0.9, notify => self()}),
    %% Two low samples fill the window and cross into drift → one message.
    ok = fenrir_drift_detector:record(<<"s">>, 0.1),
    ok = fenrir_drift_detector:record(<<"s">>, 0.1),
    receive {drift, <<"s">>} -> ok after 1000 -> ct:fail(no_drift_msg) end,
    %% Still drifting on the next low sample → NO duplicate (edge-triggered).
    ok = fenrir_drift_detector:record(<<"s">>, 0.1),
    receive {drift, <<"s">>} -> ct:fail(duplicate_drift_msg) after 200 -> ok end.

enumerates_drifting_signatures(_) ->
    {ok, _} = fenrir_drift_detector:start_link(#{window_size => 2, threshold => 0.9}),
    ok = fenrir_drift_detector:record(<<"a">>, 0.1),
    ok = fenrir_drift_detector:record(<<"a">>, 0.1),
    ok = fenrir_drift_detector:record(<<"b">>, 1.0),
    ok = fenrir_drift_detector:record(<<"b">>, 1.0),
    [<<"a">>] = fenrir_drift_detector:drifting_signatures().

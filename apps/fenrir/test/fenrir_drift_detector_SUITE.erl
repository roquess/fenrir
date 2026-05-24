-module(fenrir_drift_detector_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([no_drift_when_healthy/1, drift_when_window_degrades/1, window_is_bounded/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [no_drift_when_healthy, drift_when_window_degrades, window_is_bounded].

init_per_testcase(_, Config) ->
    {ok, Pid} = fenrir_drift_detector:start_link(#{window_size => 3, threshold => 0.9}),
    [{d, Pid} | Config].

end_per_testcase(_, Config) ->
    gen_server:stop(?config(d, Config)).

no_drift_when_healthy(_) ->
    [fenrir_drift_detector:record(<<"s">>, 1.0) || _ <- lists:seq(1, 3)],
    false = fenrir_drift_detector:drifting(<<"s">>).

drift_when_window_degrades(_) ->
    %% Fenêtre pas encore pleine : pas de verdict.
    ok = fenrir_drift_detector:record(<<"s">>, 0.2),
    false = fenrir_drift_detector:drifting(<<"s">>),
    %% Fenêtre pleine et moyenne basse → dérive.
    ok = fenrir_drift_detector:record(<<"s">>, 0.3),
    ok = fenrir_drift_detector:record(<<"s">>, 0.1),
    true = fenrir_drift_detector:drifting(<<"s">>).

window_is_bounded(_) ->
    [fenrir_drift_detector:record(<<"s">>, 1.0) || _ <- lists:seq(1, 10)],
    3 = length(fenrir_drift_detector:window(<<"s">>)).

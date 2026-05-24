-module(fenrir_singleflight_SUITE).
-export([all/0]).
-export([leader_computes_and_shares/1, done_serves_immediately/1]).

all() -> [leader_computes_and_shares, done_serves_immediately].

leader_computes_and_shares(_) ->
    Coord = fenrir_singleflight:start(),
    Self = self(),
    Key = <<"k">>,
    Compute = fun() -> Self ! computed, value42 end,
    Pids = [spawn(fun() -> Self ! {r, fenrir_singleflight:acquire(Coord, Key, Compute)} end)
            || _ <- lists:seq(1, 5)],
    Results = [receive {r, V} -> V end || _ <- Pids],
    %% All 5 get the same value.
    [value42, value42, value42, value42, value42] = Results,
    %% A single execution of the computation.
    1 = count(computed, 0),
    fenrir_singleflight:stop(Coord).

done_serves_immediately(_) ->
    Coord = fenrir_singleflight:start(),
    Self = self(),
    Key = <<"k">>,
    %% Premier acquire calcule et termine.
    value = fenrir_singleflight:acquire(Coord, Key, fun() -> Self ! computed, value end),
    %% Second acquire (after done) is served without recomputation.
    value = fenrir_singleflight:acquire(Coord, Key, fun() -> Self ! computed, other end),
    1 = count(computed, 0),
    fenrir_singleflight:stop(Coord).

count(Msg, N) ->
    receive Msg -> count(Msg, N + 1) after 0 -> N end.

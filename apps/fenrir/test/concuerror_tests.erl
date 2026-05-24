-module(concuerror_tests).
-export([singleflight_computes_once/0, concurrent_drift_single_heal/0,
         stream_demand_exactly_once/0]).

%% Model checking (Concuerror) of single-flight.
%%
%% Two processes ask to learn THE SAME key in parallel. Property verified for
%% ALL possible scheduling interleavings:
%%   1. the expensive compute function runs only ONCE (zero redundant call),
%%      whatever the order,
%%   2. both processes receive the SAME result,
%%   3. no deadlock (both terminate).
singleflight_computes_once() ->
    Coord = fenrir_singleflight:start(),
    Self = self(),
    Key = <<"csv:cols=2">>,
    %% ComputeFun signals each real execution to the test process.
    Compute = fun() -> Self ! computed, learned_recipe end,
    spawn(fun() -> Self ! {r1, fenrir_singleflight:acquire(Coord, Key, Compute)} end),
    spawn(fun() -> Self ! {r2, fenrir_singleflight:acquire(Coord, Key, Compute)} end),
    R1 = receive {r1, X} -> X end,
    R2 = receive {r2, Y} -> Y end,
    %% Property 2: same shared result.
    learned_recipe = R1,
    learned_recipe = R2,
    %% Property 1: exactly one computation (a single 'computed' message).
    1 = count_computed(0),
    fenrir_singleflight:stop(Coord),
    ok.

%% Counts the remaining 'computed' messages without blocking.
count_computed(N) ->
    receive
        computed -> count_computed(N + 1)
    after 0 -> N
    end.

%% Model checking (Concuerror) of the healer's in-flight dedup.
%%
%% A guard process holds the per-signature status. The first trigger starts a
%% heal in a SEPARATE process (mirroring async work) that stays in flight until
%% released; while in flight the status is 'healing'. A second concurrent
%% trigger (a second drift push, or the reconciliation tick) arriving during
%% that window must be rejected by the real fenrir_healer:should_heal/1 guard.
%%
%% The compute is released only after BOTH triggers have been processed (the
%% guard acks each), so the in-flight window provably spans both triggers.
%% Property over ALL interleavings: exactly one compute runs; no deadlock.
concurrent_drift_single_heal() ->
    Self = self(),
    Guard = spawn(fun() -> heal_guard(healthy, Self) end),
    Guard ! {trigger, Self},            %% drift push #1
    Guard ! {trigger, Self},            %% drift push #2 / reconcile tick
    Compute = receive {started, C} -> C end,   %% the single in-flight heal
    receive acked -> ok end,            %% trigger #1 handled
    receive acked -> ok end,            %% trigger #2 handled (must be skipped)
    Compute ! release,
    receive {healed, _} -> ok end,
    0 = extra_healed(0),                %% no second compute ever
    Guard ! stop,
    ok.

heal_guard(Status, Reporter) ->
    receive
        {trigger, From} ->
            case fenrir_healer:should_heal(Status) of
                true ->
                    G = self(),
                    C = spawn(fun() ->
                                  receive release ->
                                      Reporter ! {healed, G},
                                      G ! done
                                  end
                              end),
                    Reporter ! {started, C},
                    From ! acked,
                    heal_guard(healing, Reporter);
                false ->
                    From ! acked,
                    heal_guard(Status, Reporter)
            end;
        done ->
            heal_guard(healed, Reporter);
        stop ->
            ok
    end.

extra_healed(N) ->
    receive {healed, _} -> extra_healed(N + 1)
    after 0 -> N
    end.

%% Model checking (Concuerror) of the demand protocol.
%%
%% A coordinator hands one record per demand to whichever worker asks; when the
%% records run out it replies 'done' to each worker. Two workers contend. The
%% real coordinator serializes demands in its mailbox exactly like this model.
%% Property over ALL interleavings: each record is delivered to exactly one
%% worker (no loss, no duplication) and every process terminates (no deadlock).
stream_demand_exactly_once() ->
    Self = self(),
    Coord = spawn(fun() -> sd_coord([a, b, c], 2) end),
    spawn(fun() -> sd_worker(Coord, Self) end),
    spawn(fun() -> sd_worker(Coord, Self) end),
    Got = sd_collect(3, []),
    [a, b, c] = lists:sort(Got),
    ok.

sd_coord([R | T], Wc) ->
    receive {demand, W} -> W ! {batch, R}, sd_coord(T, Wc) end;
sd_coord([], Wc) when Wc > 0 ->
    receive {demand, W} -> W ! done, sd_coord([], Wc - 1) end;
sd_coord([], 0) ->
    ok.

sd_worker(Coord, Rep) ->
    Coord ! {demand, self()},
    receive
        {batch, R} -> Rep ! {got, R}, sd_worker(Coord, Rep);
        done -> ok
    end.

sd_collect(0, Acc) -> Acc;
sd_collect(N, Acc) -> receive {got, R} -> sd_collect(N - 1, [R | Acc]) end.

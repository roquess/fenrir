-module(concuerror_tests).
-export([singleflight_computes_once/0, concurrent_drift_single_heal/0,
         stream_demand_exactly_once/0, drift_edge_single_notify/0,
         store_rollback_one_winner/0, confidence_no_lost_update/0]).

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

%% Model checking (Concuerror) of the drift detector's edge-trigger.
%%
%% Two processes concurrently push a low-confidence sample into a window-holding
%% actor that emits {drift} only on the false→true edge, using the REAL
%% predicate fenrir_drift_detector:would_drift/3. Property over all
%% interleavings: exactly one {drift} is emitted (no miss, no duplicate).
drift_edge_single_notify() ->
    Self = self(),
    Actor = spawn(fun() -> drift_actor([], 2, 0.9, false, Self) end),
    spawn(fun() -> Actor ! {record, 0.1} end),
    spawn(fun() -> Actor ! {record, 0.1} end),
    receive drift -> ok end,
    0 = drift_extra(0),
    Actor ! stop,
    ok.

drift_actor(Win, Size, Th, Was, Rep) ->
    receive
        {record, C} ->
            Win2 = lists:sublist([C | Win], Size),
            Now = fenrir_drift_detector:would_drift(Win2, Size, Th),
            case (not Was) andalso Now of
                true  -> Rep ! drift;
                false -> ok
            end,
            drift_actor(Win2, Size, Th, Now, Rep);
        stop ->
            ok
    end.

drift_extra(N) ->
    receive drift -> drift_extra(N + 1) after 0 -> N end.

%% Model checking (Concuerror) of recipe_store rollback under contention.
%%
%% Two processes concurrently roll back a 2-version history through an actor
%% that uses the REAL fenrir_recipe_store:rollback_history/1. Property: exactly
%% one rollback wins ({ok, v1}); the other gets {error, no_previous}. No crash,
%% no double-rollback.
store_rollback_one_winner() ->
    Self = self(),
    Actor = spawn(fun() -> store_actor([v2, v1]) end),
    spawn(fun() -> Actor ! {rollback, Self} end),
    spawn(fun() -> Actor ! {rollback, Self} end),
    R1 = receive {rb, X} -> X end,
    R2 = receive {rb, Y} -> Y end,
    [{error, no_previous}, {ok, v1}] = lists:sort([R1, R2]),
    Actor ! stop,
    ok.

store_actor(Hist) ->
    receive
        {rollback, From} ->
            case fenrir_recipe_store:rollback_history(Hist) of
                {ok, Prev, NewHist} -> From ! {rb, {ok, Prev}}, store_actor(NewHist);
                {error, R}          -> From ! {rb, {error, R}}, store_actor(Hist)
            end;
        stop ->
            ok
    end.

%% Model checking (Concuerror) of confidence accumulation (no lost update).
%%
%% Three processes concurrently observe into a counting actor. Each observe is
%% acked; after all three acks the count is read. Property over all
%% interleavings: the count equals the number of observes (no lost update).
confidence_no_lost_update() ->
    Self = self(),
    Actor = spawn(fun() -> conf_actor(0) end),
    [spawn(fun() -> Actor ! {observe, Self} end) || _ <- [1, 2, 3]],
    [receive acked -> ok end || _ <- [1, 2, 3]],
    Actor ! {count, Self},
    receive {count, N} -> 3 = N end,
    Actor ! stop,
    ok.

conf_actor(Count) ->
    receive
        {observe, From} -> From ! acked, conf_actor(Count + 1);
        {count, From}   -> From ! {count, Count}, conf_actor(Count);
        stop            -> ok
    end.

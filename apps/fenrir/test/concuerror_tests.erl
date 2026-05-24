-module(concuerror_tests).
-export([singleflight_computes_once/0]).

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

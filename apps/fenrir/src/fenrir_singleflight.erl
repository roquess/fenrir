-module(fenrir_singleflight).

%% Single-flight: when several concurrent requests ask to learn THE SAME
%% source (key) before a recipe exists, only one runs the expensive
%% computation (sniff/AI) — it is the "leader"; the others wait and share its
%% result. Guarantees zero redundant calls during a concurrent cold start.
%%
%% Implemented with pure message passing (spawn/send/receive): verifiable by
%% model checking (Concuerror) over all interleavings.

-export([start/0, stop/1, acquire/3, loop/1]).

%% Starts the coordinator. Returns its pid.
start() ->
    spawn(?MODULE, loop, [#{}]).

stop(Coord) ->
    Coord ! stop,
    ok.

%% acquire(Coord, Key, ComputeFun) -> Result
%% ComputeFun :: fun(() -> Result), run by the single leader only.
acquire(Coord, Key, ComputeFun) ->
    Coord ! {acquire, Key, self()},
    receive
        {lead, Key} ->
            Result = ComputeFun(),
            Coord ! {provide, Key, Result},
            Result;
        {result, Key, Result} ->
            Result
    end.

%% Coordinator loop.
%% State :: #{Key => {computing, [pid()]} | {done, term()}}
loop(State) ->
    receive
        stop ->
            ok;
        {acquire, Key, From} ->
            case maps:get(Key, State, undefined) of
                undefined ->
                    From ! {lead, Key},
                    loop(State#{Key => {computing, []}});
                {computing, Waiters} ->
                    loop(State#{Key => {computing, [From | Waiters]}});
                {done, Result} ->
                    From ! {result, Key, Result},
                    loop(State)
            end;
        {provide, Key, Result} ->
            case maps:get(Key, State, undefined) of
                {computing, Waiters} ->
                    [W ! {result, Key, Result} || W <- Waiters],
                    loop(State#{Key => {done, Result}});
                _ ->
                    loop(State)
            end
    end.

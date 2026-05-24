-module(fenrir_singleflight).

%% Single-flight : quand plusieurs requêtes concurrentes demandent à apprendre
%% LA MÊME source (clé) avant qu'une recette n'existe, une seule exécute le
%% calcul coûteux (sniff/IA) — c'est le « leader » ; les autres attendent et
%% partagent son résultat. Garantit zéro appel redondant lors d'un démarrage
%% à froid concurrent.
%%
%% Implémenté en pur passage de messages (spawn/send/receive) : vérifiable par
%% model checking (Concuerror) sur tous les entrelacements.

-export([start/0, stop/1, acquire/3, loop/1]).

%% Démarre le coordinateur. Retourne son pid.
start() ->
    spawn(?MODULE, loop, [#{}]).

stop(Coord) ->
    Coord ! stop,
    ok.

%% acquire(Coord, Key, ComputeFun) -> Result
%% ComputeFun :: fun(() -> Result), exécutée par le seul leader.
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

%% Boucle du coordinateur.
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

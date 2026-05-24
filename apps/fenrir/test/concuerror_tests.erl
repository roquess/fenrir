-module(concuerror_tests).
-export([singleflight_computes_once/0]).

%% Model checking (Concuerror) du single-flight.
%%
%% Deux process demandent à apprendre LA MÊME clé en parallèle. Propriété
%% vérifiée pour TOUS les entrelacements d'ordonnancement possibles :
%%   1. la fonction de calcul coûteuse ne s'exécute qu'UNE seule fois
%%      (zéro appel redondant), quel que soit l'ordre,
%%   2. les deux process reçoivent le MÊME résultat,
%%   3. aucun deadlock (les deux terminent).
singleflight_computes_once() ->
    Coord = fenrir_singleflight:start(),
    Self = self(),
    Key = <<"csv:cols=2">>,
    %% ComputeFun signale chaque exécution réelle au process de test.
    Compute = fun() -> Self ! computed, learned_recipe end,
    spawn(fun() -> Self ! {r1, fenrir_singleflight:acquire(Coord, Key, Compute)} end),
    spawn(fun() -> Self ! {r2, fenrir_singleflight:acquire(Coord, Key, Compute)} end),
    R1 = receive {r1, X} -> X end,
    R2 = receive {r2, Y} -> Y end,
    %% Propriété 2 : même résultat partagé.
    learned_recipe = R1,
    learned_recipe = R2,
    %% Propriété 1 : exactement un calcul (un seul message 'computed').
    1 = count_computed(0),
    fenrir_singleflight:stop(Coord),
    ok.

%% Compte les messages 'computed' restants sans bloquer.
count_computed(N) ->
    receive
        computed -> count_computed(N + 1)
    after 0 -> N
    end.

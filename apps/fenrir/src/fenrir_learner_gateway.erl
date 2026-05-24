-module(fenrir_learner_gateway).

%% Passerelle d'apprentissage : produit une recette patchée à partir de la
%% recette courante et des enregistrements qui ont échoué. La fonction
%% d'apprentissage est injectable (LearnFun) — par défaut le ré-apprentissage
%% heuristique déterministe du cœur Rust ; en Phase 2+ on peut y brancher un
%% service IA sans toucher au reste.

-export([patch/3, default_learn_fun/0]).

%% patch(Recipe, FailingLines, LearnFun) -> NewRecipe
%% Recipe :: #{<<"signature">> := binary(), <<"json">> := binary(),
%%             <<"sample">> := binary()}
%% LearnFun :: fun((PrevJson :: binary(), Corpus :: binary()) -> NewJson :: binary())
patch(Recipe, FailingLines, LearnFun) ->
    Sample   = maps:get(<<"sample">>, Recipe, <<>>),
    PrevJson = maps:get(<<"json">>, Recipe),
    %% Corpus enrichi : échantillon connu + lignes en échec.
    Corpus = iolist_to_binary([Sample | [[<<"\n">>, L] || L <- FailingLines]]),
    NewJson = LearnFun(PrevJson, Corpus),
    Recipe#{<<"json">> => NewJson, <<"sample">> => Corpus}.

default_learn_fun() ->
    fun fenrir_core_nif:relearn/2.

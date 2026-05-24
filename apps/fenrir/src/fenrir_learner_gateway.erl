-module(fenrir_learner_gateway).

%% Learning gateway: produces a patched recipe from the current recipe and the
%% records that failed. The learning function is injectable (LearnFun) — by
%% default the deterministic heuristic re-learning from the Rust core; an AI
%% service can be plugged in here without touching the rest.

-export([patch/3, default_learn_fun/0]).

%% patch(Recipe, FailingLines, LearnFun) -> NewRecipe
%% Recipe :: #{<<"signature">> := binary(), <<"json">> := binary(),
%%             <<"sample">> := binary()}
%% LearnFun :: fun((PrevJson :: binary(), Corpus :: binary()) -> NewJson :: binary())
patch(Recipe, FailingLines, LearnFun) ->
    Sample   = maps:get(<<"sample">>, Recipe, <<>>),
    PrevJson = maps:get(<<"json">>, Recipe),
    %% Enriched corpus: known sample + failing lines.
    Corpus = iolist_to_binary([Sample | [[<<"\n">>, L] || L <- FailingLines]]),
    NewJson = LearnFun(PrevJson, Corpus),
    Recipe#{<<"json">> => NewJson, <<"sample">> => Corpus}.

default_learn_fun() ->
    fun fenrir_core_nif:relearn/2.

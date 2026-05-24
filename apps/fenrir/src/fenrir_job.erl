-module(fenrir_job).
-export([learn/2, run/3, signature_of/1]).

%% Nif :: #{sniff => fun((binary()) -> binary()),
%%          parse_line => fun((binary(), binary()) -> {binary(), float()})}

%% PERCEIVE + SUGGEST + REMEMBER : apprend (ou réutilise) une recette.
learn(Sample, Nif) ->
    Sig = signature_of(Sample),
    case fenrir_recipe_store:get(Sig) of
        {ok, R} ->
            {ok, R};                          %% réutilise : zéro IA
        not_found ->
            SniffFun = maps:get(sniff, Nif),
            Json = SniffFun(Sample),
            Recipe = #{<<"signature">> => Sig, <<"json">> => Json},
            ok = fenrir_recipe_store:put(Sig, Recipe),
            {ok, Recipe}
    end.

%% ACT : applique la recette à des enregistrements.
run(Recipe, Lines, Nif) ->
    ParseFun = maps:get(parse_line, Nif),
    Json = maps:get(<<"json">>, Recipe, <<"{}">>),
    [ ParseFun(Json, L) || L <- Lines ].

%% Signature de structure de la source (perceive).
signature_of(Sample) ->
    First = case binary:split(Sample, <<"\n">>) of [H | _] -> H; _ -> Sample end,
    Cols = length(binary:split(First, [<<";">>, <<",">>, <<"\t">>, <<"|">>], [global])),
    iolist_to_binary(io_lib:format("csv:cols=~p", [Cols])).

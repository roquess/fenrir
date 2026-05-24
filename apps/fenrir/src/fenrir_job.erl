-module(fenrir_job).
-export([learn/2, learn/3, run/3, escalate/3, signature_of/1]).

%% Nif :: #{sniff      => fun((binary()) -> binary()),
%%          parse_line => fun((binary(), binary()) -> {binary(), float()}),
%%          relearn    => fun((binary(), binary()) -> binary())}

%% PERCEIVE + SUGGEST + REMEMBER: learn (or reuse) a recipe.
learn(Sample, Nif) -> learn(Sample, Nif, undefined).

%% Variant with a single-flight coordinator: during a concurrent cold start
%% (several requests before a recipe exists), only one leader runs the
%% expensive sniff; the others share its result.
learn(Sample, Nif, Coord) ->
    Sig = signature_of(Sample),
    case fenrir_recipe_store:get(Sig) of
        {ok, R} ->
            {ok, R};                          %% reuse: zero learning
        not_found when Coord =:= undefined ->
            do_learn(Sample, Sig, Nif);
        not_found ->
            R = fenrir_singleflight:acquire(
                  Coord, Sig,
                  fun() -> {ok, Rec} = do_learn(Sample, Sig, Nif), Rec end),
            {ok, R}
    end.

do_learn(Sample, Sig, Nif) ->
    SniffFun = maps:get(sniff, Nif),
    Json = SniffFun(Sample),
    Recipe = #{<<"signature">> => Sig, <<"json">> => Json, <<"sample">> => Sample},
    ok = fenrir_recipe_store:put(Sig, Recipe),
    {ok, Recipe}.

%% ACT: apply the recipe to records.
run(Recipe, Lines, Nif) ->
    ParseFun = maps:get(parse_line, Nif),
    Json = maps:get(<<"json">>, Recipe, <<"{}">>),
    [ ParseFun(Json, L) || L <- Lines ].

%% ESCALATION: from the failing records, re-learn a patched recipe and keep
%% it ONLY if it actually improves confidence on those same records.
%% Otherwise reject it (anti-regression / rollback).
escalate(_Recipe, [], _Nif) ->
    no_change;
escalate(Recipe, DeadLetters, Nif) ->
    ParseFun = maps:get(parse_line, Nif),
    LearnFun = maps:get(relearn, Nif, fenrir_learner_gateway:default_learn_fun()),
    Patched  = fenrir_learner_gateway:patch(Recipe, DeadLetters, LearnFun),
    OldMean  = mean_conf(maps:get(<<"json">>, Recipe), DeadLetters, ParseFun),
    NewMean  = mean_conf(maps:get(<<"json">>, Patched), DeadLetters, ParseFun),
    case NewMean > OldMean of
        true  -> {improved, Patched, OldMean, NewMean};
        false -> {rejected, Recipe, OldMean, NewMean}
    end.

mean_conf(_Json, [], _ParseFun) -> 1.0;
mean_conf(Json, Lines, ParseFun) ->
    Confs = [ element(2, ParseFun(Json, L)) || L <- Lines ],
    lists:sum(Confs) / length(Confs).

%% Structural signature of the source (perceive).
signature_of(Sample) ->
    First = case binary:split(Sample, <<"\n">>) of [H | _] -> H; _ -> Sample end,
    Cols = length(binary:split(First, [<<";">>, <<",">>, <<"\t">>, <<"|">>], [global])),
    iolist_to_binary(io_lib:format("csv:cols=~p", [Cols])).

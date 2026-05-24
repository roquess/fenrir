-module(fenrir_escalation_SUITE).
-export([all/0]).
-export([improves_keeps_patch/1, regression_is_rejected/1, no_dead_letters_no_change/1]).

all() -> [improves_keeps_patch, regression_is_rejected, no_dead_letters_no_change].

%% parse_line mocké : la recette "new" parse parfaitement (1.0), les autres
%% mal (0.5). Permet de simuler une amélioration ou une régression.
nif(RelearnResult) ->
    #{
        parse_line => fun(Json, _Line) ->
            case Json of
                <<"new">> -> {<<"{}">>, 1.0};
                _         -> {<<"{}">>, 0.5}
            end
        end,
        relearn => fun(_PrevJson, _Corpus) -> RelearnResult end
    }.

recipe() ->
    #{<<"signature">> => <<"sig">>,
      <<"json">> => <<"old">>,
      <<"sample">> => <<"name;age\nAlice;30\n">>}.

improves_keeps_patch(_) ->
    Nif = nif(<<"new">>),
    DL = [<<"Bob;notanumber">>, <<"Eve;n/a">>],
    {improved, Patched, Old, New} = fenrir_job:escalate(recipe(), DL, Nif),
    <<"new">> = maps:get(<<"json">>, Patched),
    true = (New > Old).

regression_is_rejected(_) ->
    %% Le ré-apprentissage ne change rien d'utile (toujours "old") → pas mieux.
    Nif = nif(<<"old">>),
    DL = [<<"Bob;notanumber">>, <<"Eve;n/a">>],
    {rejected, Recipe, Old, New} = fenrir_job:escalate(recipe(), DL, Nif),
    <<"old">> = maps:get(<<"json">>, Recipe),
    true = (New =< Old).

no_dead_letters_no_change(_) ->
    no_change = fenrir_job:escalate(recipe(), [], nif(<<"new">>)).

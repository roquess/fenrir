-module(fenrir).
-export([ingest/3, ingest_adaptive/2, nif/0]).

%% Façade NIF par défaut (mode exécution + ré-apprentissage heuristique).
nif() ->
    #{sniff      => fun fenrir_core_nif:sniff/1,
      parse_line => fun fenrir_core_nif:parse_line/2,
      relearn    => fun fenrir_core_nif:relearn/2}.

%% Pipeline simple : sample d'apprentissage, lignes à parser, format de sortie.
ingest(Sample, Lines, Format) ->
    Nif = nif(),
    {ok, Recipe} = fenrir_job:learn(Sample, Nif),
    Records = fenrir_job:run(Recipe, Lines, Nif),
    Values  = [V || {V, _Conf} <- Records],
    Joined  = <<"[", (iolist_to_binary(lists:join(<<",">>, Values)))/binary, "]">>,
    fenrir_core_nif:load(Joined, Format).

%% Pipeline auto-réparateur complet (la boucle hybride de bout en bout) :
%%   1. apprend (ou réutilise) une recette,
%%   2. parse les lignes en observant la confiance (monitor + drift),
%%   3. si la confiance globale chute → escalade : ré-apprend sur les
%%      enregistrements en échec et ne retient le patch que s'il améliore,
%%   4. renvoie un rapport décrivant ce qui s'est passé.
%% Nécessite fenrir_recipe_store / _confidence_monitor / _drift_detector
%% démarrés (cf. arbre de supervision).
ingest_adaptive(Sample, Lines) ->
    Nif = nif(),
    {ok, Recipe} = fenrir_job:learn(Sample, Nif),
    Sig = maps:get(<<"signature">>, Recipe),
    fenrir_confidence_monitor:reset(Sig),

    Records = fenrir_job:run(Recipe, Lines, Nif),
    lists:foreach(
      fun({{_V, Conf}, Line}) ->
          fenrir_confidence_monitor:observe(Sig, Line, Conf),
          fenrir_drift_detector:record(Sig, Conf)
      end,
      lists:zip(Records, Lines)),

    Base = #{signature => Sig,
             records => Records,
             drifting => fenrir_drift_detector:drifting(Sig)},

    case fenrir_confidence_monitor:needs_escalation(Sig) of
        false ->
            Base#{escalated => false, recipe => Recipe};
        true ->
            DeadLetters = fenrir_confidence_monitor:dead_letters(Sig),
            case fenrir_job:escalate(Recipe, DeadLetters, Nif) of
                {improved, Patched, Old, New} ->
                    ok = fenrir_recipe_store:put(Sig, Patched),
                    fenrir_confidence_monitor:reset(Sig),
                    Base#{escalated => true, improved => true,
                          old_conf => Old, new_conf => New, recipe => Patched};
                {rejected, _Recipe, Old, New} ->
                    Base#{escalated => true, improved => false,
                          old_conf => Old, new_conf => New, recipe => Recipe};
                no_change ->
                    Base#{escalated => false, recipe => Recipe}
            end
    end.

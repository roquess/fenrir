-module(fenrir).
-export([ingest/3]).

%% Pipeline complet : sample d'apprentissage, lignes à parser, format de sortie.
%% Utilise le vrai NIF.
ingest(Sample, Lines, Format) ->
    Nif = #{sniff      => fun fenrir_core_nif:sniff/1,
            parse_line => fun fenrir_core_nif:parse_line/2},
    {ok, Recipe} = fenrir_job:learn(Sample, Nif),
    Records = fenrir_job:run(Recipe, Lines, Nif),
    Values  = [V || {V, _Conf} <- Records],
    Joined  = <<"[", (iolist_to_binary(lists:join(<<",">>, Values)))/binary, "]">>,
    fenrir_core_nif:load(Joined, Format).

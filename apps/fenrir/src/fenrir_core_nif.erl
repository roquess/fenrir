-module(fenrir_core_nif).
-export([sniff/1, parse_line/2, relearn/2, load/2,
         detect_format/1, sniff_xml/1, parse_xml/2,
         sniff_text/1, parse_text/2]).
-on_load(init/0).

-define(APPNAME, fenrir).
-define(LIBNAME, fenrir_core_nif).

init() ->
    SoName = filename:join([code:priv_dir(?APPNAME), ?LIBNAME]),
    erlang:load_nif(SoName, 0).

%% Remplacés par le NIF au chargement ; ces clauses ne s'exécutent
%% que si le NIF n'a pas pu être chargé.
sniff(_Sample) -> erlang:nif_error(nif_not_loaded).
parse_line(_RecipeJson, _Line) -> erlang:nif_error(nif_not_loaded).
relearn(_PrevRecipeJson, _Corpus) -> erlang:nif_error(nif_not_loaded).
load(_ValueJson, _Format) -> erlang:nif_error(nif_not_loaded).
detect_format(_Sample) -> erlang:nif_error(nif_not_loaded).
sniff_xml(_Sample) -> erlang:nif_error(nif_not_loaded).
parse_xml(_RecipeJson, _Fragment) -> erlang:nif_error(nif_not_loaded).
sniff_text(_Pattern) -> erlang:nif_error(nif_not_loaded).
parse_text(_RecipeJson, _Line) -> erlang:nif_error(nif_not_loaded).

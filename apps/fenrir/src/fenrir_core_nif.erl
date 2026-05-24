-module(fenrir_core_nif).
-export([sniff/1, parse_line/2, load/2]).
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
load(_ValueJson, _Format) -> erlang:nif_error(nif_not_loaded).

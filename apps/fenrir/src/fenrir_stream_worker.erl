-module(fenrir_stream_worker).
-export([run/3]).

%% Pure parse worker. Pulls a batch from the coordinator on demand, parses each
%% record via the NIF, and returns the results; it performs NO side effects
%% (confidence/drift/sink run on the coordinator). This lets a worker run on any
%% node that has the NIF loaded.
run(Coord, RecipeJson, Nif) ->
    Coord ! {demand, self(), []},
    loop(Coord, RecipeJson, Nif).

loop(Coord, RecipeJson, Nif) ->
    receive
        {batch, Records} ->
            ParseFun = maps:get(parse_line, Nif),
            Results = [ParseFun(RecipeJson, R) || R <- Records],
            Coord ! {demand, self(), Results},
            loop(Coord, RecipeJson, Nif);
        done ->
            ok
    end.

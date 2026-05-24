-module(fenrir_stream_worker).
-export([run/5]).

%% Worker loop. Pulls work from the coordinator on demand, parses each record,
%% observes its confidence (feeding the monitor + drift detector), and calls the
%% sink. A sink that raises a normal exception is caught (the record is counted
%% as handled but logged); a hard crash (e.g. kill) terminates the worker, whose
%% outstanding batch the coordinator re-queues (at-least-once).
run(Coord, RecipeJson, Sig, Nif, Sink) ->
    Coord ! {demand, self(), 0},
    loop(Coord, RecipeJson, Sig, Nif, Sink).

loop(Coord, RecipeJson, Sig, Nif, Sink) ->
    receive
        {batch, Records} ->
            K = handle_batch(Records, RecipeJson, Sig, Nif, Sink),
            Coord ! {demand, self(), K},
            loop(Coord, RecipeJson, Sig, Nif, Sink);
        done ->
            ok
    end.

handle_batch(Records, RecipeJson, Sig, Nif, Sink) ->
    ParseFun = maps:get(parse_line, Nif),
    lists:foldl(
      fun(R, Acc) ->
          {Value, Conf} = ParseFun(RecipeJson, R),
          fenrir_confidence_monitor:observe(Sig, R, Conf),
          fenrir_drift_detector:record(Sig, Conf),
          try Sink(Value, Conf)
          catch C:Reason ->
              logger:warning("fenrir_stream: sink failed ~p:~p", [C, Reason])
          end,
          Acc + 1
      end, 0, Records).

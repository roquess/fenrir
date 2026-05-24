-module(fenrir_writer_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([close_returns_count/1, serializes_concurrent_writes/1, writer_sink_end_to_end/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [close_returns_count, serializes_concurrent_writes, writer_sink_end_to_end].

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, _Config) -> ok.

close_returns_count(Config) ->
    Path = filename:join(?config(priv_dir, Config), "w1.jsonl"),
    {ok, W} = fenrir_writer:start(Path, "json"),
    Sink = fenrir_writer:sink(W),
    Sink(<<"{\"a\":1}">>, 1.0),
    Sink(<<"{\"a\":2}">>, 1.0),
    2 = fenrir_writer:close(W),
    {ok, Bin} = file:read_file(Path),
    2 = length(nonempty_lines(Bin)).

serializes_concurrent_writes(Config) ->
    Path = filename:join(?config(priv_dir, Config), "w2.jsonl"),
    {ok, W} = fenrir_writer:start(Path, "json"),
    Sink = fenrir_writer:sink(W),
    Parent = self(),
    Pids = [spawn(fun() ->
                      [Sink(list_to_binary("{\"k\":" ++ integer_to_list(I * 100 + J) ++ "}"), 1.0)
                       || J <- lists:seq(1, 25)],
                      Parent ! {done, self()}
                  end) || I <- lists:seq(1, 4)],
    [receive {done, _P} -> ok end || _ <- Pids],
    100 = fenrir_writer:close(W),
    {ok, Bin} = file:read_file(Path),
    Lines = nonempty_lines(Bin),
    100 = length(Lines),
    true = lists:all(fun(L) -> jsonish(L) =:= ok end, Lines).

writer_sink_end_to_end(Config) ->
    catch gen_server:stop(fenrir_confidence_monitor),
    catch gen_server:stop(fenrir_drift_detector),
    {ok, _} = fenrir_confidence_monitor:start_link(#{}),
    {ok, _} = fenrir_drift_detector:start_link(#{}),
    Path = filename:join(?config(priv_dir, Config), "sink.jsonl"),
    Recipe = #{<<"signature">> => <<"sig-ws">>, <<"json">> => <<"recipe">>,
               <<"sample">> => <<>>},
    Nif = #{parse_line => fun(_RJ, R) -> {<<"{\"v\":\"", R/binary, "\"}">>, 1.0} end},
    Records = [list_to_binary("r" ++ integer_to_list(I)) || I <- lists:seq(1, 30)],
    Src = fenrir_stream:list_source(Records),
    {Sink, Await} = fenrir_stream:writer_sink(Path, "json"),
    Report = fenrir_stream:run(Src, Recipe, Sink, #{pool_size => 4, batch_size => 5, nif => Nif}),
    30 = maps:get(processed, Report),
    30 = Await(),
    {ok, Bin} = file:read_file(Path),
    30 = length(nonempty_lines(Bin)),
    catch gen_server:stop(fenrir_confidence_monitor),
    catch gen_server:stop(fenrir_drift_detector).

nonempty_lines(Bin) ->
    [L || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>].

jsonish(L) ->
    case re:run(L, "^\\{\"[a-z]+\":[0-9]+\\}$") of
        {match, _} -> ok;
        nomatch -> not_json
    end.

-module(fenrir_stream_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([list_source_yields_then_eof/1, file_source_reads_lines/1,
         worker_parses_and_returns_results/1, processes_all_records_unordered/1,
         source_is_pulled_lazily/1, worker_crash_record_still_processed/1,
         confidence_is_observed/1, report_has_throughput/1,
         file_source_strips_bom/1]).
-include_lib("common_test/include/ct.hrl").

all() ->
    [list_source_yields_then_eof, file_source_reads_lines,
     worker_parses_and_returns_results, processes_all_records_unordered,
     source_is_pulled_lazily, worker_crash_record_still_processed,
     confidence_is_observed, report_has_throughput, file_source_strips_bom].

init_per_testcase(_, Config) ->
    catch gen_server:stop(fenrir_confidence_monitor),
    catch gen_server:stop(fenrir_drift_detector),
    {ok, _} = fenrir_confidence_monitor:start_link(#{}),
    {ok, _} = fenrir_drift_detector:start_link(#{}),
    Config.

end_per_testcase(_, _Config) ->
    catch gen_server:stop(fenrir_confidence_monitor),
    catch gen_server:stop(fenrir_drift_detector),
    ok.

%% ---- helpers ----

mock_nif() ->
    #{parse_line => fun(_RecipeJson, R) ->
                        {<<"{\"v\":\"", R/binary, "\"}">>, 1.0}
                    end}.

low_conf_nif() ->
    #{parse_line => fun(_RecipeJson, _R) -> {<<"{}">>, 0.2} end}.

recipe() ->
    #{<<"signature">> => <<"sig-stream">>, <<"json">> => <<"recipe">>,
      <<"sample">> => <<>>}.

drain_recs(Acc) ->
    receive {rec, V} -> drain_recs([V | Acc])
    after 200 -> Acc
    end.

%% ---- tests ----

list_source_yields_then_eof(_) ->
    Src = fenrir_stream:list_source([<<"a">>, <<"b">>]),
    {ok, <<"a">>} = Src(),
    {ok, <<"b">>} = Src(),
    eof = Src(),
    eof = Src().

file_source_reads_lines(Config) ->
    Path = filename:join(?config(priv_dir, Config), "src.txt"),
    ok = file:write_file(Path, <<"l1\nl2\n">>),
    Src = fenrir_stream:file_source(Path),
    {ok, <<"l1">>} = Src(),
    {ok, <<"l2">>} = Src(),
    eof = Src().

worker_parses_and_returns_results(_) ->
    Coord = self(),
    W = spawn(fun() ->
                  fenrir_stream_worker:run(Coord, <<"recipe">>, mock_nif())
              end),
    receive {demand, W, []} -> ok after 1000 -> ct:fail(no_first_demand) end,
    W ! {batch, [<<"x">>, <<"y">>]},
    receive
        {demand, W, Results} ->
            [{<<"{\"v\":\"x\"}">>, 1.0}, {<<"{\"v\":\"y\"}">>, 1.0}] = Results
    after 1000 -> ct:fail(no_results)
    end,
    W ! done.

processes_all_records_unordered(_) ->
    Self = self(),
    Sink = fun(V, _C) -> Self ! {rec, V}, ok end,
    Records = [list_to_binary("r" ++ integer_to_list(I)) || I <- lists:seq(1, 50)],
    Src = fenrir_stream:list_source(Records),
    Report = fenrir_stream:run(Src, recipe(), Sink,
                               #{pool_size => 4, batch_size => 7, nif => mock_nif()}),
    50 = maps:get(processed, Report),
    Got = drain_recs([]),
    50 = length(Got),
    Expected = lists:sort([<<"{\"v\":\"", R/binary, "\"}">> || R <- Records]),
    Expected = lists:sort(Got).

source_is_pulled_lazily(_) ->
    Self = self(),
    Ctr = counters:new(1, []),
    Sink = fun(_V, _C) -> Self ! sunk, ok end,
    Src = counting_source(1000, Ctr),
    Report = fenrir_stream:run(Src, recipe(), Sink,
                               #{pool_size => 2, batch_size => 10, nif => mock_nif()}),
    1000 = maps:get(processed, Report),
    Calls = counters:get(Ctr, 1),
    true = (Calls >= 1000),
    true = (Calls =< 1000 + 50),
    drain_sunk().

counting_source(N, CounterRef) ->
    Ix = atomics:new(1, [{signed, false}]),
    fun() ->
        counters:add(CounterRef, 1, 1),
        I = atomics:add_get(Ix, 1, 1),
        case I =< N of
            true  -> {ok, integer_to_binary(I)};
            false -> eof
        end
    end.

drain_sunk() ->
    receive sunk -> drain_sunk() after 100 -> ok end.

worker_crash_record_still_processed(_) ->
    Self = self(),
    Flag = atomics:new(1, [{signed, false}]),
    %% The crash now happens inside the worker (parse_line), since the sink runs
    %% on the coordinator. First parse hard-kills the worker; later parses succeed.
    Nif = #{parse_line => fun(_RJ, R) ->
                              case atomics:add_get(Flag, 1, 1) of
                                  1 -> exit(self(), kill);
                                  _ -> {<<"{\"v\":\"", R/binary, "\"}">>, 1.0}
                              end
                          end},
    Sink = fun(V, _C) -> Self ! {rec, V}, ok end,
    Records = [<<"a">>, <<"b">>, <<"c">>],
    Src = fenrir_stream:list_source(Records),
    Report = fenrir_stream:run(Src, recipe(), Sink,
                               #{pool_size => 1, batch_size => 3, nif => Nif}),
    Got = drain_recs([]),
    true = lists:member(<<"{\"v\":\"a\"}">>, Got),
    true = lists:member(<<"{\"v\":\"b\"}">>, Got),
    true = lists:member(<<"{\"v\":\"c\"}">>, Got),
    true = maps:get(processed, Report) >= 2.

file_source_strips_bom(Config) ->
    Path = filename:join(?config(priv_dir, Config), "bom.csv"),
    ok = file:write_file(Path, <<239, 187, 191, "name;age\n", "Alice;30\n">>),
    Src = fenrir_stream:file_source(Path),
    {ok, First} = Src(),
    <<"name;age">> = First.

report_has_throughput(_) ->
    Nif = #{parse_line => fun(_RJ, R) -> {<<"{\"v\":\"", R/binary, "\"}">>, 1.0} end},
    Src = fenrir_stream:list_source([<<"a">>, <<"b">>, <<"c">>]),
    Report = fenrir_stream:run(Src, recipe(), fun(_V, _C) -> ok end,
                               #{pool_size => 2, batch_size => 2, nif => Nif}),
    3 = maps:get(processed, Report),
    true = is_integer(maps:get(elapsed_ms, Report)),
    true = maps:get(elapsed_ms, Report) >= 0,
    true = is_integer(maps:get(throughput_per_s, Report)).

confidence_is_observed(_) ->
    Sink = fun(_V, _C) -> ok end,
    Src = fenrir_stream:list_source([<<"r1">>, <<"r2">>, <<"r3">>]),
    _ = fenrir_stream:run(Src, recipe(), Sink,
                          #{pool_size => 1, batch_size => 3, nif => low_conf_nif()}),
    Mean = fenrir_confidence_monitor:mean(<<"sig-stream">>),
    true = (Mean < 0.95),
    3 = length(fenrir_confidence_monitor:dead_letters(<<"sig-stream">>)).

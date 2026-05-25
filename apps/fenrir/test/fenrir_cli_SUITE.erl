-module(fenrir_cli_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([ingest_csv_to_jsonl/1, missing_input_errors/1, unknown_format_errors/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [ingest_csv_to_jsonl, missing_input_errors, unknown_format_errors].

init_per_testcase(_, Config) ->
    catch application:stop(fenrir),
    Config.

end_per_testcase(_, _Config) ->
    catch application:stop(fenrir),
    ok.

ingest_csv_to_jsonl(Config) ->
    Dir = ?config(priv_dir, Config),
    In = filename:join(Dir, "people.csv"),
    Out = filename:join(Dir, "people.out.jsonl"),
    ok = file:write_file(In, <<"name;age\nAlice;30\nBob;25\nCarol;40\n">>),
    {ok, Report} = fenrir_cli:run(["ingest", In, "--to", "json", "-o", Out, "--sample", "10"]),
    3 = maps:get(processed, Report),
    3 = maps:get(written, Report),
    true = is_float(maps:get(mean_confidence, Report)),
    true = is_boolean(maps:get(drifting, Report)),
    true = is_map(maps:get(totals, Report)),
    {ok, Bin} = file:read_file(Out),
    Lines = [L || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>],
    3 = length(Lines),
    true = lists:all(fun(L) -> binary:match(L, <<"name">>) =/= nomatch end, Lines).

missing_input_errors(_) ->
    {error, {input_not_found, _}} =
        fenrir_cli:run(["ingest", "/no/such/file.csv", "--to", "json"]).

unknown_format_errors(Config) ->
    Dir = ?config(priv_dir, Config),
    In = filename:join(Dir, "x.csv"),
    ok = file:write_file(In, <<"a;b\n1;2\n">>),
    {error, {unknown_format, "nope"}} =
        fenrir_cli:run(["ingest", In, "--to", "nope"]).

-module(fenrir_job_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([learns_then_reuses/1, parses_records/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [learns_then_reuses, parses_records].

init_per_testcase(_, Config) ->
    {ok, Store} = fenrir_recipe_store:start_link(#{dir => ?config(priv_dir, Config)}),
    %% NIF mock: sniff returns a fixed recipe, parse_line returns {json, 1.0}
    Nif = #{
        sniff => fun(_S) -> <<"{\"signature\":\"sig-x\",\"version\":1}">> end,
        parse_line => fun(_R, L) -> {<<"{\"line\":\"", L/binary, "\"}">>, 1.0} end
    },
    [{store, Store}, {nif, Nif} | Config].

end_per_testcase(_, Config) ->
    gen_server:stop(?config(store, Config)).

learns_then_reuses(Config) ->
    Nif = ?config(nif, Config),
    Counter = counters:new(1, []),
    SniffFun = fun(S) -> counters:add(Counter, 1, 1), (maps:get(sniff, Nif))(S) end,
    Nif2 = Nif#{sniff => SniffFun},
    Sample = <<"a;b\n1;2\n">>,
    {ok, _R1} = fenrir_job:learn(Sample, Nif2),
    {ok, _R2} = fenrir_job:learn(Sample, Nif2),
    1 = counters:get(Counter, 1).   %% 2nd call served from the store: zero AI sniff

parses_records(Config) ->
    Nif = ?config(nif, Config),
    Sample = <<"a;b\n1;2\n">>,
    {ok, Recipe} = fenrir_job:learn(Sample, Nif),
    Records = fenrir_job:run(Recipe, [<<"x;y">>, <<"z;w">>], Nif),
    2 = length(Records),
    [{_, 1.0} | _] = Records.

-module(fenrir_e2e_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([full_pipeline/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [full_pipeline].

init_per_testcase(_, Config) ->
    {ok, Store} = fenrir_recipe_store:start_link(#{dir => ?config(priv_dir, Config)}),
    [{store, Store} | Config].

end_per_testcase(_, Config) ->
    gen_server:stop(?config(store, Config)).

full_pipeline(_) ->
    Sample = <<"name;age\nAlice;30\nBob;25\n">>,
    Lines  = [<<"Carol;40">>, <<"Dan;22">>],
    {ok, Out} = fenrir:ingest(Sample, Lines, <<"json">>),
    true = is_binary(Out) orelse is_list(Out),
    {match, _} = re:run(Out, "Carol").

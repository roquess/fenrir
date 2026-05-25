-module(fenrir_robustness_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([parse_line_corrupted_recipe/1, load_invalid_json/1, sniff_empty/1,
         run_with_corrupted_recipe_degrades/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [parse_line_corrupted_recipe, load_invalid_json, sniff_empty,
          run_with_corrupted_recipe_degrades].

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, _Config) -> ok.

%% A corrupted recipe JSON must not crash the NIF: it returns an error-JSON
%% binary plus zero confidence.
parse_line_corrupted_recipe(_) ->
    {Json, Conf} = fenrir_core_nif:parse_line(<<"not json at all">>, <<"a;b">>),
    true = is_binary(Json),
    true = (Conf == 0.0),
    {match, _} = re:run(Json, "error").

%% Invalid JSON to load → {error, _}, never a crash.
load_invalid_json(_) ->
    {error, _} = fenrir_core_nif:load(<<"not json">>, <<"json">>).

%% Empty input still yields a (non-empty) recipe JSON.
sniff_empty(_) ->
    R = fenrir_core_nif:sniff(<<>>),
    true = is_binary(R),
    true = (byte_size(R) > 0).

%% A run with a corrupted recipe degrades (each record an error result) but
%% does not crash.
run_with_corrupted_recipe_degrades(_) ->
    Nif = #{parse_line => fun fenrir_core_nif:parse_line/2},
    Recipe = #{<<"signature">> => <<"s">>, <<"json">> => <<"garbage">>,
               <<"sample">> => <<>>},
    Records = fenrir_job:run(Recipe, [<<"x;y">>, <<"z;w">>], Nif),
    2 = length(Records),
    lists:foreach(fun({J, C}) -> true = is_binary(J), true = (C == 0.0) end, Records).

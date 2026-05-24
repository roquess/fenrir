-module(fenrir_recipe_store_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([put_then_get/1, miss_returns_not_found/1, versioning_keeps_history/1,
         rollback_reverts_to_previous/1, rollback_without_history_errors/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [put_then_get, miss_returns_not_found, versioning_keeps_history,
          rollback_reverts_to_previous, rollback_without_history_errors].

init_per_testcase(_, Config) ->
    {ok, Pid} = fenrir_recipe_store:start_link(#{dir => ?config(priv_dir, Config)}),
    [{store, Pid} | Config].

end_per_testcase(_, Config) ->
    gen_server:stop(?config(store, Config)).

put_then_get(_) ->
    ok = fenrir_recipe_store:put(<<"sig1">>, #{<<"version">> => 1, <<"json">> => <<"{}">>}),
    {ok, R} = fenrir_recipe_store:get(<<"sig1">>),
    1 = maps:get(<<"version">>, R).

miss_returns_not_found(_) ->
    not_found = fenrir_recipe_store:get(<<"nope">>).

versioning_keeps_history(_) ->
    ok = fenrir_recipe_store:put(<<"s">>, #{<<"version">> => 1}),
    ok = fenrir_recipe_store:put(<<"s">>, #{<<"version">> => 2}),
    {ok, R} = fenrir_recipe_store:get(<<"s">>),
    2 = maps:get(<<"version">>, R),
    [1, 2] = lists:sort([maps:get(<<"version">>, V) || V <- fenrir_recipe_store:history(<<"s">>)]).

rollback_reverts_to_previous(_) ->
    ok = fenrir_recipe_store:put(<<"r">>, #{<<"version">> => 1}),
    ok = fenrir_recipe_store:put(<<"r">>, #{<<"version">> => 2}),
    {ok, #{<<"version">> := 2}} = fenrir_recipe_store:get(<<"r">>),
    {ok, #{<<"version">> := 1}} = fenrir_recipe_store:rollback(<<"r">>),
    {ok, #{<<"version">> := 1}} = fenrir_recipe_store:get(<<"r">>).

rollback_without_history_errors(_) ->
    ok = fenrir_recipe_store:put(<<"single">>, #{<<"version">> => 1}),
    {error, no_previous} = fenrir_recipe_store:rollback(<<"single">>).

-module(fenrir_distributed_SUITE).
-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([distributes_across_nodes/1, peer_down_requeues/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [distributes_across_nodes, peer_down_requeues].

init_per_suite(Config) ->
    %% net_kernel:start brings up distribution (and epmd, where the runtime can).
    %% If it can't (no epmd / no distribution in this environment), skip cleanly
    %% rather than fail — the suite then runs only where distribution is available.
    %% NB: never shell out to `epmd -daemon` here — on Windows os:cmd blocks
    %% forever on the detached daemon and trips the timetrap.
    try net_kernel:start([fenrir_ct, shortnames]) of
        {ok, _} -> Config;
        {error, {already_started, _}} -> Config;
        {error, Reason} -> {skip, {no_distribution, Reason}}
    catch
        _:Err -> {skip, {no_distribution, Err}}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_, Config) ->
    catch gen_server:stop(fenrir_confidence_monitor),
    catch gen_server:stop(fenrir_drift_detector),
    {ok, _} = fenrir_confidence_monitor:start_link(#{}),
    {ok, _} = fenrir_drift_detector:start_link(#{}),
    %% Start a peer node sharing this node's code paths (NIF + worker load there).
    CodePaths = code:get_path(),
    {ok, Peer, Node} = peer:start(#{name => peer:random_name(),
                                    args => ["-pa" | CodePaths]}),
    [{peer, Peer}, {node, Node} | Config].

end_per_testcase(_, Config) ->
    catch peer:stop(?config(peer, Config)),
    catch gen_server:stop(fenrir_confidence_monitor),
    catch gen_server:stop(fenrir_drift_detector),
    ok.

recipe() ->
    #{<<"signature">> => <<"sig-dist">>, <<"json">> => <<"recipe">>,
      <<"sample">> => <<>>}.

nif() ->
    #{parse_line => fun(_RJ, R) -> {<<"{\"v\":\"", R/binary, "\"}">>, 1.0} end}.

drain(N) ->
    receive {rec, _} -> drain(N + 1) after 500 -> N end.

distributes_across_nodes(Config) ->
    Peer = ?config(node, Config),
    Self = self(),
    Sink = fun(V, _C) -> Self ! {rec, V}, ok end,
    Records = [list_to_binary("r" ++ integer_to_list(I)) || I <- lists:seq(1, 40)],
    Src = fenrir_stream:list_source(Records),
    Report = fenrir_stream:run(Src, recipe(), Sink,
                               #{nodes => [node(), Peer], pool_size => 4,
                                 batch_size => 3, nif => nif()}),
    40 = maps:get(processed, Report),
    40 = drain(0),
    NodesUsed = maps:get(nodes_used, Report),
    true = lists:member(Peer, NodesUsed).

peer_down_requeues(Config) ->
    Peer = ?config(node, Config),
    PeerCtrl = ?config(peer, Config),
    Self = self(),
    Sink = fun(V, _C) -> Self ! {rec, V}, ok end,
    Records = [list_to_binary("r" ++ integer_to_list(I)) || I <- lists:seq(1, 30)],
    Src = fenrir_stream:list_source(Records),
    %% Kill the peer shortly after the run starts; the run must still complete.
    spawn(fun() -> timer:sleep(20), catch peer:stop(PeerCtrl) end),
    Report = fenrir_stream:run(Src, recipe(), Sink,
                               #{nodes => [node(), Peer], pool_size => 4,
                                 batch_size => 2, nif => nif()}),
    true = maps:get(processed, Report) >= 30,
    _ = drain(0),
    ok.

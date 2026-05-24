-module(fenrir_stream).
-behaviour(gen_server).

-export([run/4, list_source/1, file_source/1, writer_sink/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% ---- Sources : fun(() -> {ok, binary()} | eof) ----

%% In-memory source (tests). Stateful via an atomics index; the coordinator
%% calls it serially, so a lock-free counter is sufficient.
list_source(Records) when is_list(Records) ->
    Vec = list_to_tuple(Records),
    N = tuple_size(Vec),
    Ix = atomics:new(1, [{signed, false}]),
    fun() ->
        I = atomics:add_get(Ix, 1, 1),
        case I =< N of
            true  -> {ok, element(I, Vec)};
            false -> eof
        end
    end.

%% File source: one line per call, trailing newline stripped, bounded memory.
file_source(Path) ->
    {ok, Dev} = file:open(Path, [read, binary, {read_ahead, 65536}]),
    fun() ->
        case file:read_line(Dev) of
            {ok, Line} -> {ok, strip_nl(Line)};
            eof -> file:close(Dev), eof
        end
    end.

strip_nl(Bin) ->
    S1 = case binary:last(Bin) of
             $\n -> binary:part(Bin, 0, byte_size(Bin) - 1);
             _   -> Bin
         end,
    case S1 of
        <<>> -> <<>>;
        _ -> case binary:last(S1) of
                 $\r -> binary:part(S1, 0, byte_size(S1) - 1);
                 _   -> S1
             end
    end.

%% Packages a fenrir_writer as a stream sink. Returns {Sink, AwaitFun}: pass
%% Sink to run/4, then call AwaitFun() AFTER run/4 returns (all workers finished,
%% so no more sink calls) to close the file and get the written count.
writer_sink(Path, Format) ->
    {ok, W} = fenrir_writer:start(Path, Format),
    {fenrir_writer:sink(W), fun() -> fenrir_writer:close(W) end}.

%% ---- coordinator ----

-record(st, {source, recipe_json, sig, nif, sink, batch_size, pool_size,
             pending = [], outstanding = #{}, refs = #{}, idle = [],
             source_done = false, processed = 0, caller}).

%% Synchronous run: blocks until the source is exhausted and all in-flight
%% batches are done, then returns #{processed => N}.
run(Source, Recipe, Sink, Opts) ->
    Args = #{source => Source, recipe => Recipe, sink => Sink,
             opts => Opts, caller => self()},
    {ok, Pid} = gen_server:start(?MODULE, Args, []),
    MRef = erlang:monitor(process, Pid),
    receive
        {fenrir_stream_done, Report} ->
            erlang:demonitor(MRef, [flush]),
            Report;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.

init(#{source := Source, recipe := Recipe, sink := Sink, opts := Opts, caller := Caller}) ->
    PoolSize = maps:get(pool_size, Opts, erlang:system_info(schedulers)),
    BatchSize = maps:get(batch_size, Opts, 100),
    Nif = maps:get(nif, Opts, fenrir:nif()),
    St0 = #st{source = Source,
              recipe_json = maps:get(<<"json">>, Recipe),
              sig = maps:get(<<"signature">>, Recipe),
              nif = Nif, sink = Sink,
              batch_size = BatchSize, pool_size = PoolSize,
              caller = Caller},
    %% Drop leading records (e.g. a CSV header) before workers start.
    StSkipped = drain(St0, maps:get(skip, Opts, 0)),
    St = lists:foldl(fun(_, Acc) -> spawn_worker(Acc) end, StSkipped,
                     lists:seq(1, PoolSize)),
    {ok, St}.

drain(St, 0) ->
    St;
drain(#st{source = Src} = St, N) when N > 0 ->
    case Src() of
        {ok, _} -> drain(St, N - 1);
        eof     -> St#st{source_done = true}
    end.

handle_call(_, _, S) -> {reply, ok, S}.
handle_cast(_, S) -> {noreply, S}.

handle_info({demand, W, AckK}, St0) ->
    St1 = St0#st{processed = St0#st.processed + AckK,
                 outstanding = maps:remove(W, St0#st.outstanding),
                 idle = [W | lists:delete(W, St0#st.idle)]},
    serve(St1);
handle_info({'DOWN', _Ref, process, W, _Reason}, St0) ->
    Requeued = maps:get(W, St0#st.outstanding, []),
    St1 = St0#st{pending = Requeued ++ St0#st.pending,
                 outstanding = maps:remove(W, St0#st.outstanding),
                 refs = maps:remove(W, St0#st.refs),
                 idle = lists:delete(W, St0#st.idle)},
    St2 = maybe_respawn(St1),
    serve(St2);
handle_info(_, S) -> {noreply, S}.

terminate(_, _) -> ok.

%% Hand work to idle workers until none are idle or no work is available;
%% finalize when the run is complete.
serve(#st{idle = []} = St) ->
    {noreply, St};
serve(#st{idle = [W | Rest]} = St) ->
    {Batch, St1} = pull_batch(St),
    case Batch of
        [] ->
            case complete(St1) of
                true  -> finalize(St1);
                false -> {noreply, St1}
            end;
        _ ->
            W ! {batch, Batch},
            St2 = St1#st{idle = Rest,
                         outstanding = maps:put(W, Batch, St1#st.outstanding)},
            serve(St2)
    end.

complete(St) ->
    St#st.source_done andalso St#st.pending =:= []
        andalso map_size(St#st.outstanding) =:= 0.

finalize(St) ->
    [W ! done || W <- St#st.idle],
    St#st.caller ! {fenrir_stream_done, #{processed => St#st.processed}},
    {stop, normal, St}.

%% Pull up to batch_size records: pending first, then the source.
pull_batch(St) -> pull_batch(St, St#st.batch_size, []).

pull_batch(St, 0, Acc) ->
    {lists:reverse(Acc), St};
pull_batch(#st{pending = [R | T]} = St, N, Acc) ->
    pull_batch(St#st{pending = T}, N - 1, [R | Acc]);
pull_batch(#st{pending = [], source_done = true} = St, _N, Acc) ->
    {lists:reverse(Acc), St};
pull_batch(#st{pending = [], source_done = false, source = Src} = St, N, Acc) ->
    case Src() of
        {ok, R} -> pull_batch(St, N - 1, [R | Acc]);
        eof     -> pull_batch(St#st{source_done = true}, N, Acc)
    end.

maybe_respawn(St) ->
    case complete(St) of
        true  -> St;
        false -> spawn_worker(St)
    end.

spawn_worker(St) ->
    {Pid, Ref} = spawn_monitor(
                   fenrir_stream_worker, run,
                   [self(), St#st.recipe_json, St#st.sig, St#st.nif, St#st.sink]),
    St#st{refs = maps:put(Pid, Ref, St#st.refs)}.

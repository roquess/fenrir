-module(fenrir_writer).
-export([start/2, sink/1, close/1, loop/3]).

%% A single process that serializes writes from concurrent stream workers to one
%% output file as JSONL: one loki_weave-formatted object per line. Because all
%% writes funnel through this process, lines never interleave.

start(Path, Format) ->
    {ok, Dev} = file:open(Path, [write, binary]),
    %% The NIF decodes the format as a binary, so normalize a "json" string too.
    Pid = spawn(?MODULE, loop, [Dev, iolist_to_binary(Format), 0]),
    {ok, Pid}.

%% Sink for fenrir_stream:run/4. Confidence is ignored by this sink.
sink(Writer) ->
    fun(Value, _Conf) -> Writer ! {write, Value}, ok end.

%% Flush, close, and return the number of records written.
close(Writer) ->
    Writer ! {close, self()},
    receive {closed, N} -> N end.

loop(Dev, Format, N) ->
    receive
        {write, Value} ->
            case fenrir_core_nif:load(Value, Format) of
                {ok, Line} ->
                    ok = file:write(Dev, [Line, $\n]),
                    loop(Dev, Format, N + 1);
                {error, Reason} ->
                    logger:warning("fenrir_writer: load failed ~p", [Reason]),
                    loop(Dev, Format, N)
            end;
        {close, From} ->
            ok = file:close(Dev),
            From ! {closed, N},
            ok
    end.

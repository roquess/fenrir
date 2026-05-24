-module(fenrir_cli).
-export([main/1, run/1]).

%% Entry for the bin/fenrir wrapper (halts the VM with an exit code).
main(Args) ->
    case run(Args) of
        {ok, Report} ->
            io:format("~p~n", [Report]),
            halt(0);
        {error, Reason} ->
            io:format(standard_error, "fenrir: ~p~n", [Reason]),
            halt(1)
    end.

%% Testable entry: same logic, returns a result instead of halting.
run(["ingest", Input | Rest]) ->
    Defaults = #{to => "json", out => Input ++ ".out.jsonl", sample => 50},
    case parse_opts(Rest, Defaults) of
        {error, R} -> {error, R};
        Opts -> do_ingest(Input, Opts)
    end;
run(_) ->
    {error, usage}.

parse_opts([], Acc) -> Acc;
parse_opts(["--to", F | T], Acc) -> parse_opts(T, Acc#{to => F});
parse_opts(["-o", O | T], Acc) -> parse_opts(T, Acc#{out => O});
parse_opts(["--sample", N | T], Acc) ->
    case catch list_to_integer(N) of
        I when is_integer(I), I > 0 -> parse_opts(T, Acc#{sample => I});
        _ -> {error, {bad_sample, N}}
    end;
parse_opts([Unknown | _], _) -> {error, {unknown_arg, Unknown}}.

do_ingest(Input, #{to := Format, out := Out, sample := SampleN}) ->
    case filelib:is_regular(Input) of
        false ->
            {error, {input_not_found, Input}};
        true ->
            {ok, _} = application:ensure_all_started(fenrir),
            case valid_format(Format) of
                false ->
                    {error, {unknown_format, Format}};
                true ->
                    Sample = read_sample(Input, SampleN),
                    {ok, Recipe} = fenrir_job:learn(Sample, fenrir:nif()),
                    {Sink, Await} = fenrir_stream:writer_sink(Out, Format),
                    Src = fenrir_stream:file_source(Input),
                    %% Skip the header line if the learned recipe has one.
                    Skip = case binary:match(maps:get(<<"json">>, Recipe), <<"\"header\":true">>) of
                               nomatch -> 0;
                               _       -> 1
                           end,
                    Report = fenrir_stream:run(Src, Recipe, Sink, #{skip => Skip}),
                    Written = Await(),
                    {ok, Report#{written => Written, output => list_to_binary(Out)}}
            end
    end.

%% Probe loki_weave with a trivial value to validate the target format.
valid_format(Format) ->
    case fenrir_core_nif:load(<<"{}">>, iolist_to_binary(Format)) of
        {ok, _}    -> true;
        {error, _} -> false
    end.

read_sample(Path, N) ->
    {ok, Dev} = file:open(Path, [read, binary]),
    Lines = read_lines(Dev, N, []),
    file:close(Dev),
    iolist_to_binary(Lines).

read_lines(_Dev, 0, Acc) -> lists:reverse(Acc);
read_lines(Dev, N, Acc) ->
    case file:read_line(Dev) of
        {ok, L} -> read_lines(Dev, N - 1, [L | Acc]);
        eof     -> lists:reverse(Acc)
    end.

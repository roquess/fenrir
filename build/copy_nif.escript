#!/usr/bin/env escript
%%! -noshell
%% Copies the freshly-built Rust NIF into apps/fenrir/priv/ under the name
%% erlang:load_nif/2 expects. Cross-platform (no arch-regex hooks, no PowerShell):
%%   - Windows : fenrir_nif.dll      -> fenrir_core_nif.dll
%%   - macOS   : libfenrir_nif.dylib -> fenrir_core_nif.so   (Erlang NIFs are .so)
%%   - Linux   : libfenrir_nif.so    -> fenrir_core_nif.so
%% Fails loudly if the built artifact is missing.
main(_) ->
    Rel = "native/fenrir_nif/target/release/",
    {Src, DestExt} =
        case os:type() of
            {win32, _}     -> {Rel ++ "fenrir_nif.dll", "dll"};
            {unix, darwin} -> {Rel ++ "libfenrir_nif.dylib", "so"};
            {unix, _}      -> {Rel ++ "libfenrir_nif.so", "so"}
        end,
    Dest = "apps/fenrir/priv/fenrir_core_nif." ++ DestExt,
    ok = filelib:ensure_dir("apps/fenrir/priv/keep"),
    case file:copy(Src, Dest) of
        {ok, _} ->
            io:format("copy_nif: ~s -> ~s~n", [Src, Dest]);
        {error, Reason} ->
            io:format(standard_error, "copy_nif: FAILED ~s -> ~s : ~p~n",
                      [Src, Dest, Reason]),
            halt(1)
    end.

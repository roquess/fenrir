-module(fenrir_recipe_store).
-behaviour(gen_server).

-export([start_link/0, start_link/1, put/2, get/1, history/1, rollback/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

%% tab  : Sig -> current Recipe
%% hist : Sig -> [Recipe]  (most recent first)
-record(state, {tab, hist, dir, disk = true}).

start_link() -> start_link(#{dir => "priv/recipes"}).
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

put(Sig, Recipe) -> gen_server:call(?MODULE, {put, Sig, Recipe}).
get(Sig)         -> gen_server:call(?MODULE, {get, Sig}).
history(Sig)     -> gen_server:call(?MODULE, {history, Sig}).
rollback(Sig)    -> gen_server:call(?MODULE, {rollback, Sig}).

init(Opts) ->
    Dir  = maps:get(dir, Opts, "priv/recipes"),
    Disk = maps:get(disk, Opts, true),
    Disk andalso filelib:ensure_dir(filename:join(Dir, "x")),
    Tab  = ets:new(fenrir_recipes, [set, private]),
    Hist = ets:new(fenrir_recipes_hist, [set, private]),
    {ok, #state{tab = Tab, hist = Hist, dir = Dir, disk = Disk}}.

handle_call({put, Sig, Recipe}, _From, S) ->
    ets:insert(S#state.tab, {Sig, Recipe}),
    Prev = case ets:lookup(S#state.hist, Sig) of
               [{Sig, L}] -> L;
               []         -> []
           end,
    ets:insert(S#state.hist, {Sig, [Recipe | Prev]}),
    maybe_persist(S, Sig, Recipe),
    {reply, ok, S};

handle_call({get, Sig}, _From, S) ->
    case ets:lookup(S#state.tab, Sig) of
        [{Sig, R}] -> {reply, {ok, R}, S};
        []         -> {reply, not_found, S}
    end;

handle_call({history, Sig}, _From, S) ->
    case ets:lookup(S#state.hist, Sig) of
        [{Sig, L}] -> {reply, L, S};
        []         -> {reply, [], S}
    end;

handle_call({rollback, Sig}, _From, S) ->
    case ets:lookup(S#state.hist, Sig) of
        [{Sig, [_Latest, Prev | Rest]}] ->
            ets:insert(S#state.tab, {Sig, Prev}),
            ets:insert(S#state.hist, {Sig, [Prev | Rest]}),
            maybe_persist(S, Sig, Prev),
            {reply, {ok, Prev}, S};
        _ ->
            {reply, {error, no_previous}, S}
    end.

handle_cast(_, S) -> {noreply, S}.
terminate(_, _) -> ok.

maybe_persist(#state{disk = false}, _Sig, _Recipe) -> ok;
maybe_persist(#state{dir = Dir}, Sig, Recipe) ->
    file:write_file(disk_path(Dir, Sig), term_to_binary(Recipe)).

disk_path(Dir, Sig) ->
    Safe = binary:replace(Sig, [<<"/">>, <<":">>], <<"_">>, [global]),
    filename:join(Dir, <<Safe/binary, ".recipe">>).

-module(fenrir_recipe_store).
-behaviour(gen_server).

-export([start_link/0, start_link/1, put/2, get/1, history/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-record(state, {tab, hist, dir, disk = true}).

start_link() -> start_link(#{dir => "priv/recipes"}).
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

put(Sig, Recipe) -> gen_server:call(?MODULE, {put, Sig, Recipe}).
get(Sig)         -> gen_server:call(?MODULE, {get, Sig}).
history(Sig)     -> gen_server:call(?MODULE, {history, Sig}).

init(Opts) ->
    Dir  = maps:get(dir, Opts, "priv/recipes"),
    Disk = maps:get(disk, Opts, true),
    Disk andalso filelib:ensure_dir(filename:join(Dir, "x")),
    Tab  = ets:new(fenrir_recipes, [set, private]),
    Hist = ets:new(fenrir_recipes_hist, [bag, private]),
    {ok, #state{tab = Tab, hist = Hist, dir = Dir, disk = Disk}}.

handle_call({put, Sig, Recipe}, _From, S) ->
    ets:insert(S#state.tab, {Sig, Recipe}),
    ets:insert(S#state.hist, {Sig, Recipe}),
    S#state.disk andalso
        file:write_file(disk_path(S#state.dir, Sig), term_to_binary(Recipe)),
    {reply, ok, S};

handle_call({get, Sig}, _From, S) ->
    case ets:lookup(S#state.tab, Sig) of
        [{Sig, R}] -> {reply, {ok, R}, S};
        []         -> {reply, not_found, S}
    end;

handle_call({history, Sig}, _From, S) ->
    Rs = [R || {_, R} <- ets:lookup(S#state.hist, Sig)],
    {reply, Rs, S}.

handle_cast(_, S) -> {noreply, S}.
terminate(_, _) -> ok.

disk_path(Dir, Sig) ->
    Safe = binary:replace(Sig, [<<"/">>, <<":">>], <<"_">>, [global]),
    filename:join(Dir, <<Safe/binary, ".recipe">>).

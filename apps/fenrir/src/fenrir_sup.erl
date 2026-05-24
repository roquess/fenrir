-module(fenrir_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    Store = #{id => fenrir_recipe_store,
              start => {fenrir_recipe_store, start_link, []},
              restart => permanent, type => worker},
    JobSup = #{id => fenrir_job_sup,
               start => {fenrir_job_sup, start_link, []},
               restart => permanent, type => supervisor},
    {ok, {SupFlags, [Store, JobSup]}}.

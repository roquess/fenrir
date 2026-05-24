-module(fenrir_job_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {#{strategy => simple_one_for_one},
          [#{id => fenrir_job, start => {fenrir_job, learn, []},
             restart => temporary, type => worker}]}}.

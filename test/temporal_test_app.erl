%% The application callback of temporal_test_ffi:start_application/2.
-module(temporal_test_app).
-behaviour(application).
-export([start/2, stop/1]).

start(_Type, StartSupervisor) ->
    {ok, StartSupervisor()}.

stop(_State) ->
    ok.

%% Blocking wrappers around temporal_nif, plus the few process primitives the workflow
%% runtime needs that Gleam cannot express.
-module(temporal_ffi).

-export([connect/4, worker_new/3, replay_worker_new/2, poll_workflow/1, complete_workflow/2,
         poll_activity/1, complete_activity/3, heartbeat/2, worker_shutdown/1,
         start_workflow/5, signal_workflow/5, query_workflow/5, cancel_workflow/3,
         terminate_workflow/4, workflow_result/3, fetch_history/3,
         send_activation/2, receive_activation/0, get_state/0, put_state/1, rescue/1,
         exit_normal/0, kill/1]).

%% Start an asynchronous NIF call and wait for its reply. A NIF that rejects its arguments
%% returns the error directly and never replies.
call(Start) ->
    Ref = make_ref(),
    case Start(self(), Ref) of
        ok -> receive {temporal_nif, Ref, Result} -> Result end;
        Error -> Error
    end.

connect(Target, Namespace, Identity, ApiKey) ->
    call(fun(P, R) -> temporal_nif:connect(P, R, Target, Namespace, Identity, ApiKey) end).

worker_new(Conn, TaskQueue, MaxCached) ->
    call(fun(P, R) -> temporal_nif:worker_new(P, R, Conn, TaskQueue, MaxCached) end).

replay_worker_new(TaskQueue, Histories) ->
    temporal_nif:replay_worker_new(TaskQueue, Histories).

poll_error({error, {error, Message}}) -> {error, {poll_failed, Message}};
poll_error(Other) -> Other.

poll_workflow(Worker) ->
    poll_error(call(fun(P, R) -> temporal_nif:worker_poll_workflow(P, R, Worker) end)).

complete_workflow(Worker, Completion) ->
    call(fun(P, R) -> temporal_nif:worker_complete_workflow(P, R, Worker, Completion) end).

poll_activity(Worker) ->
    poll_error(call(fun(P, R) -> temporal_nif:worker_poll_activity(P, R, Worker) end)).

complete_activity(Worker, Token, Result) ->
    call(fun(P, R) -> temporal_nif:worker_complete_activity(P, R, Worker, Token, Result) end).

heartbeat(Worker, Token) ->
    temporal_nif:worker_heartbeat(Worker, Token, []).

worker_shutdown(Worker) ->
    call(fun(P, R) -> temporal_nif:worker_shutdown(P, R, Worker) end).

start_workflow(Conn, Id, Type, TaskQueue, Input) ->
    call(fun(P, R) -> temporal_nif:start_workflow(P, R, Conn, Id, Type, TaskQueue, Input) end).

signal_workflow(Conn, Id, RunId, Name, Input) ->
    call(fun(P, R) -> temporal_nif:signal_workflow(P, R, Conn, Id, RunId, Name, Input) end).

query_workflow(Conn, Id, RunId, Type, Args) ->
    call(fun(P, R) -> temporal_nif:query_workflow(P, R, Conn, Id, RunId, Type, Args) end).

cancel_workflow(Conn, Id, RunId) ->
    call(fun(P, R) -> temporal_nif:cancel_workflow(P, R, Conn, Id, RunId) end).

terminate_workflow(Conn, Id, RunId, Reason) ->
    call(fun(P, R) -> temporal_nif:terminate_workflow(P, R, Conn, Id, RunId, Reason) end).

workflow_result(Conn, Id, RunId) ->
    call(fun(P, R) -> temporal_nif:workflow_result(P, R, Conn, Id, RunId) end).

fetch_history(Conn, Id, RunId) ->
    call(fun(P, R) -> temporal_nif:fetch_history(P, R, Conn, Id, RunId) end).

send_activation(Pid, Activation) ->
    Pid ! {temporal_activation, Activation},
    nil.

receive_activation() ->
    receive {temporal_activation, Activation} -> Activation end.

get_state() ->
    erlang:get(temporal_workflow_state).

put_state(State) ->
    erlang:put(temporal_workflow_state, State),
    nil.

%% Run user code, turning a crash into an error. Exits pass through: a workflow process
%% that is evicted exits from inside user code and must not be mistaken for a crash.
rescue(Fun) ->
    try {ok, Fun()}
    catch
        error:Reason:Stack ->
            {error, unicode:characters_to_binary(io_lib:format("~0tp~n~0tp", [Reason, Stack]))};
        throw:Reason ->
            {error, unicode:characters_to_binary(io_lib:format("throw: ~0tp", [Reason]))}
    end.

exit_normal() ->
    exit(normal).

kill(Pid) ->
    exit(Pid, kill),
    nil.

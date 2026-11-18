%% Blocking wrappers around temporal_nif, plus the few process primitives the workflow
%% runtime needs that Gleam cannot express.
-module(temporal_ffi).

-export([connect/5, schedule_put/5, schedule_describe/2, schedule_delete/2,
         schedule_patch/3, schedule_list/1, system_time_ms/0, server_start/2, server_stop/1, time_control/2, worker_new/3, replay_worker_new/2, poll_workflow/1, complete_workflow/2,
         poll_activity/1, complete_activity/3, heartbeat/3, send_activity_cancel/2, activity_cancel_reason/0, rescue_all/1, log/3, worker_shutdown/1,
         start_workflow/3, describe_workflow/3, list_workflows/3, count_workflows/2, signal_workflow/5, query_workflow/5, cancel_workflow/3,
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

connect(Target, Namespace, Identity, ApiKey, Tls) ->
    call(fun(P, R) -> temporal_nif:connect(P, R, Target, Namespace, Identity, ApiKey, Tls) end).

system_time_ms() ->
    erlang:system_time(millisecond).

server_start(Kind, Exe) ->
    call(fun(P, R) -> temporal_nif:server_start(P, R, Kind, Exe) end).

server_stop(Server) ->
    call(fun(P, R) -> temporal_nif:server_stop(P, R, Server) end).

time_control(Conn, Op) ->
    call(fun(P, R) -> temporal_nif:time_control(P, R, Conn, Op) end).

schedule_put(Conn, Id, Schedule, Token, Trigger) ->
    call(fun(P, R) -> temporal_nif:schedule_put(P, R, Conn, Id, Schedule, Token, Trigger) end).

schedule_describe(Conn, Id) ->
    call(fun(P, R) -> temporal_nif:schedule_describe(P, R, Conn, Id) end).

schedule_delete(Conn, Id) ->
    call(fun(P, R) -> temporal_nif:schedule_delete(P, R, Conn, Id) end).

schedule_patch(Conn, Id, Patch) ->
    call(fun(P, R) -> temporal_nif:schedule_patch(P, R, Conn, Id, Patch) end).

schedule_list(Conn) ->
    call(fun(P, R) -> temporal_nif:schedule_list(P, R, Conn) end).

worker_new(Conn, TaskQueue, Options) ->
    call(fun(P, R) -> temporal_nif:worker_new(P, R, Conn, TaskQueue, Options) end).

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

heartbeat(Worker, Token, Details) ->
    temporal_nif:worker_heartbeat(Worker, Token, Details).

send_activity_cancel(Pid, Reason) ->
    Pid ! {temporal_activity_cancel, Reason},
    nil.

%% The cancellation delivered to this activity process, remembered once seen.
activity_cancel_reason() ->
    case erlang:get(temporal_activity_cancel) of
        undefined ->
            receive
                {temporal_activity_cancel, Reason} ->
                    erlang:put(temporal_activity_cancel, Reason),
                    {some, Reason}
            after 0 -> none
            end;
        Reason -> {some, Reason}
    end.

%% Metadata keys are a fixed set chosen by this library, so making atoms of them is safe.
log(Level, Message, Metadata) ->
    Meta = maps:fold(fun(K, V, Acc) -> Acc#{binary_to_atom(K) => V} end,
                     #{domain => [temporal]}, Metadata),
    logger:log(Level, "~ts", [Message], Meta),
    nil.

rescue_all(Fun) ->
    try {ok, Fun()}
    catch
        Class:Reason:Stack ->
            {error, unicode:characters_to_binary(
                      io_lib:format("~p: ~0tp~n~0tp", [Class, Reason, Stack]))}
    end.

worker_shutdown(Worker) ->
    call(fun(P, R) -> temporal_nif:worker_shutdown(P, R, Worker) end).

start_workflow(Conn, Request, Signal) ->
    call(fun(P, R) -> temporal_nif:start_workflow(P, R, Conn, Request, Signal) end).

describe_workflow(Conn, Id, RunId) ->
    call(fun(P, R) -> temporal_nif:describe_workflow(P, R, Conn, Id, RunId) end).

list_workflows(Conn, Query, Limit) ->
    call(fun(P, R) -> temporal_nif:list_workflows(P, R, Conn, Query, Limit) end).

count_workflows(Conn, Query) ->
    call(fun(P, R) -> temporal_nif:count_workflows(P, R, Conn, Query) end).

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

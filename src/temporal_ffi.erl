%% Blocking wrappers around temporal_nif, plus the few process primitives the workflow
%% runtime needs that Gleam cannot express.
-module(temporal_ffi).

-export([connect/5, update_with_start/4, complete_activity_async/3, defer_activity/0,
         activity_deferred/0, follow/1, new_cell/0, read_cell/1, write_cell/2, update_workflow/4, poll_update/4, poll_nexus/1, complete_nexus/3, aes_gcm_encrypt/2, aes_gcm_decrypt/2, schedule_put/5, schedule_describe/2, schedule_delete/2,
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

update_workflow(Conn, Target, Args, Stage) ->
    call(fun(P, R) -> temporal_nif:update_workflow(P, R, Conn, Target, Args, Stage) end).

poll_update(Conn, Id, RunId, UpdateId) ->
    call(fun(P, R) -> temporal_nif:poll_update(P, R, Conn, Id, RunId, UpdateId) end).

poll_nexus(Worker) ->
    poll_error(call(fun(P, R) -> temporal_nif:worker_poll_nexus(P, R, Worker) end)).

complete_nexus(Worker, Token, Result) ->
    call(fun(P, R) -> temporal_nif:worker_complete_nexus(P, R, Worker, Token, Result) end).

update_with_start(Conn, Request, Update, Args) ->
    call(fun(P, R) -> temporal_nif:update_with_start(P, R, Conn, Request, Update, Args) end).

complete_activity_async(Conn, Token, Result) ->
    call(fun(P, R) -> temporal_nif:complete_activity_async(P, R, Conn, Token, Result) end).

%% Whether the activity running in this process said it would be completed later.
defer_activity() ->
    erlang:put(temporal_activity_deferred, true),
    nil.

activity_deferred() ->
    erlang:get(temporal_activity_deferred) =:= true.

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

%% Cells live in the process dictionary of the workflow run that made them.
new_cell() ->
    {temporal_cell, make_ref()}.

read_cell(Cell) ->
    case erlang:get(Cell) of
        undefined -> none;
        {value, Value} -> {some, Value}
    end.

write_cell(Cell, Value) ->
    erlang:put(Cell, {value, Value}),
    nil.

follow(Owner) ->
    erlang:monitor(process, Owner),
    nil.

%% The only process a run monitors is the loop that feeds it; without it the run would
%% wait for ever.
receive_activation() ->
    receive
        {temporal_activation, Activation} -> Activation;
        {'DOWN', _, process, _, _} -> exit(normal)
    end.

get_state() ->
    case erlang:get(temporal_workflow_state) of
        undefined ->
            erlang:error({temporal_not_in_workflow,
                          <<"This function works only in workflow code, in the process the "
                            "worker runs the workflow in. Activities, tests and processes a "
                            "workflow spawns cannot call it.">>});
        State -> State
    end.

put_state(State) ->
    erlang:put(temporal_workflow_state, State),
    nil.

%% Run user code, turning a crash into an error. Exits pass through: a workflow process
%% that is evicted exits from inside user code and must not be mistaken for a crash.
rescue(Fun) ->
    try {ok, Fun()}
    catch
        error:Reason:Stack ->
            {error, unicode:characters_to_binary([describe(Reason), io_lib:format("~n~0tp", [Stack])])};
        throw:Reason ->
            {error, unicode:characters_to_binary(io_lib:format("throw: ~0tp", [Reason]))}
    end.

%% A Gleam panic or failed assertion as a sentence; anything else as the Erlang term.
describe(#{gleam_error := Kind, message := Message, module := Module, function := Function,
           line := Line}) ->
    io_lib:format("~ts in ~ts.~ts, line ~b: ~ts", [Kind, Module, Function, Line, Message]);
describe(Reason) ->
    io_lib:format("~0tp", [Reason]).

exit_normal() ->
    exit(normal).

kill(Pid) ->
    exit(Pid, kill),
    nil.

%% AES-256-GCM with a random 12-byte nonce: Nonce ++ Ciphertext ++ Tag.
aes_gcm_encrypt(Key, Plain) ->
    Nonce = crypto:strong_rand_bytes(12),
    {Cipher, Tag} = crypto:crypto_one_time_aead(aes_256_gcm, Key, Nonce, Plain, <<>>, 16, true),
    <<Nonce/binary, Cipher/binary, Tag/binary>>.

aes_gcm_decrypt(Key, <<Nonce:12/binary, Rest/binary>>) when byte_size(Rest) >= 16 ->
    Size = byte_size(Rest) - 16,
    <<Cipher:Size/binary, Tag:16/binary>> = Rest,
    try crypto:crypto_one_time_aead(aes_256_gcm, Key, Nonce, Cipher, <<>>, Tag, false) of
        error -> {error, nil};
        Plain -> {ok, Plain}
    catch
        _:_ -> {error, nil}
    end;
aes_gcm_decrypt(_, _) ->
    {error, nil}.

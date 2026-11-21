%% Blocking wrappers around temporal_nif, plus the few process primitives the workflow
%% runtime needs that Gleam cannot express.
-module(temporal_ffi).

-export([connect/1, schedule_create/4, schedule_update/5, update_with_start/4, complete_activity_async/3, defer_activity/0,
         activity_deferred/0, follow/1, new_cell/0, read_cell/1, write_cell/2, update_workflow/4, poll_update/4, poll_nexus/1, complete_nexus/3, aes_gcm_encrypt/2, aes_gcm_decrypt/2, schedule_describe/2, schedule_delete/2,
         schedule_patch/3, schedule_list/1, system_time_ms/0, server_start/2, server_stop/1, time_control/2, worker_new/3, replay_worker_new/2, poll_workflow/1, complete_workflow/2,
         poll_activity/1, complete_activity/3, heartbeat/3, send_activity_cancel/2, activity_cancel_reason/0, rescue_all/1, log/4, log_notice/3, flush_logs/0, worker_shutdown/2, new_flag/0, raise_flag/1, flag_raised/1, new_tally/0, tally/2, tallied/1,
         start_workflow/3, describe_workflow/3, list_workflows/3, count_workflows/2, signal_workflow/5, query_workflow/5, cancel_workflow/3,
         terminate_workflow/4, workflow_result/3, fetch_history/3,
         start_activity/2, activity_result/3, describe_activity/3, cancel_activity/4,
         terminate_activity/4, list_activities/3, count_activities/2,
         send_activation/2, receive_activation/0, get_state/0, put_state/1, rescue/1, describe_exit/1, is_musl/0,
         exit_normal/0, kill/1, replace_lone_surrogates/1]).

%% Start an asynchronous NIF call and wait for its reply. A NIF that rejects its arguments
%% returns the error directly and never replies.
call(Start) ->
    Ref = make_ref(),
    case Start(self(), Ref) of
        ok -> receive {temporal_nif, Ref, Result} -> Result end;
        Error -> Error
    end.

connect(Options) ->
    call(fun(P, R) -> temporal_nif:connect(P, R, Options) end).

system_time_ms() ->
    erlang:system_time(millisecond).

server_start(Kind, Exe) ->
    call(fun(P, R) -> temporal_nif:server_start(P, R, Kind, Exe) end).

server_stop(Server) ->
    call(fun(P, R) -> temporal_nif:server_stop(P, R, Server) end).

time_control(Conn, Op) ->
    call(fun(P, R) -> temporal_nif:time_control(P, R, Conn, Op) end).

schedule_create(Conn, Id, Schedule, Trigger) ->
    call(fun(P, R) -> temporal_nif:schedule_create(P, R, Conn, Id, Schedule, Trigger) end).

schedule_update(Conn, Id, Stored, Described, Changed) ->
    call(fun(P, R) -> temporal_nif:schedule_update(P, R, Conn, Id, Stored, Described, Changed) end).

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
    call(fun(P, R) -> temporal_nif:worker_poll_nexus(P, R, Worker) end).

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

poll_workflow(Worker) ->
    call(fun(P, R) -> temporal_nif:worker_poll_workflow(P, R, Worker) end).

complete_workflow(Worker, Completion) ->
    call(fun(P, R) -> temporal_nif:worker_complete_workflow(P, R, Worker, Completion) end).

poll_activity(Worker) ->
    call(fun(P, R) -> temporal_nif:worker_poll_activity(P, R, Worker) end).

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

%% `Identity` and `Fields` are lists of {Key, Value} with binary keys; a later entry
%% replaces an earlier one with the same key.
%%
%% Identity keys are the SDK's own and become top-level metadata atoms through
%% identity_key/1, a fixed table. Field keys come from the caller's code and may be built
%% from data, so no atom is ever made of one: the fields go, keyed by binary, in one map
%% under `fields`. The atom table is never collected, and a VM that fills it stops.
%%
%% The event carries no `domain`: OTP's default handler drops every event whose domain is
%% not `[otp]` or `[otp, sasl]`, so a domain of our own would hide the SDK's lines from
%% anyone who has not reconfigured logger. `temporal => true` marks them instead.
log(Level, Message, Identity, Fields) ->
    Meta = maps:from_list([{identity_key(K), V} || {K, V} <- Identity]),
    WithFields = case Fields of
                     [] -> Meta;
                     _ -> Meta#{fields => maps:from_list(Fields)}
                 end,
    logger:log(Level, "~ts", [Message], WithFields#{temporal => true}),
    nil.

identity_key(<<"workflow_id">>) -> workflow_id;
identity_key(<<"run_id">>) -> run_id;
identity_key(<<"workflow_type">>) -> workflow_type;
identity_key(<<"task_queue">>) -> task_queue;
identity_key(<<"activity_id">>) -> activity_id;
identity_key(<<"activity_type">>) -> activity_type;
identity_key(<<"activity_run_id">>) -> activity_run_id;
identity_key(<<"attempt">>) -> attempt.

log_notice(Message, Identity, Fields) ->
    log(notice, Message, Identity, Fields).

%% Wait until the lines logged so far have been written by logger's standard handlers,
%% for a program that is about to halt: they write in processes of their own.
flush_logs() ->
    [catch logger_std_h:filesync(Id)
     || #{id := Id, module := logger_std_h} <- logger:get_handler_config()],
    nil.

%% Like rescue/1, and catches exits too.
rescue_all(Fun) ->
    try {ok, Fun()}
    catch
        Class:Reason:Stack -> {error, crash(Class, Reason, Stack)}
    end.

worker_shutdown(Worker, LimitMs) ->
    call(fun(P, R) -> temporal_nif:worker_shutdown(P, R, Worker, LimitMs) end).

%% A flag that processes share without messages: an activity reads its worker's.
new_flag() ->
    atomics:new(1, []).

raise_flag(Flag) ->
    atomics:put(Flag, 1, 1),
    nil.

flag_raised(Flag) ->
    atomics:get(Flag, 1) =:= 1.

%% Three counts that processes add to without messages: how the activities of a worker
%% that is stopping ended.
new_tally() ->
    counters:new(3, []).

tally(Tally, Index) ->
    counters:add(Tally, Index, 1),
    nil.

tallied(Tally) ->
    {counters:get(Tally, 1), counters:get(Tally, 2), counters:get(Tally, 3)}.

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

start_activity(Conn, Request) ->
    call(fun(P, R) -> temporal_nif:start_activity(P, R, Conn, Request) end).

activity_result(Conn, Id, RunId) ->
    call(fun(P, R) -> temporal_nif:activity_result(P, R, Conn, Id, RunId) end).

describe_activity(Conn, Id, RunId) ->
    call(fun(P, R) -> temporal_nif:describe_activity(P, R, Conn, Id, RunId) end).

cancel_activity(Conn, Id, RunId, Reason) ->
    call(fun(P, R) -> temporal_nif:cancel_activity(P, R, Conn, Id, RunId, Reason) end).

terminate_activity(Conn, Id, RunId, Reason) ->
    call(fun(P, R) -> temporal_nif:terminate_activity(P, R, Conn, Id, RunId, Reason) end).

list_activities(Conn, Query, Limit) ->
    call(fun(P, R) -> temporal_nif:list_activities(P, R, Conn, Query, Limit) end).

count_activities(Conn, Query) ->
    call(fun(P, R) -> temporal_nif:count_activities(P, R, Conn, Query) end).

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
        error:Reason:Stack -> {error, crash(error, Reason, Stack)};
        throw:Reason:Stack -> {error, crash(throw, Reason, Stack)}
    end.

%% A crash three ways: {Summary, StackTrace, Detail}.
%%
%% Summary and StackTrace go into the failure Temporal records, which leaves the machine
%% and is not covered by a payload codec, so they must hold no values: an Erlang error
%% term such as `{badmatch, V}` or a stack frame's argument list carries the data the code
%% was working on. Summary names the class and kind of error, and StackTrace the frames
%% as module, function, arity, file and line.
%%
%% Detail is the whole term, for the local log only.
crash(Class, Reason, Stack) ->
    {unicode:characters_to_binary(describe(Class, Reason)),
     unicode:characters_to_binary(stack_trace(Stack)),
     unicode:characters_to_binary(io_lib:format("~p: ~0tp~n~0tp", [Class, Reason, Stack]))}.

%% What a monitored process exited with, as {Summary, Detail}: see crash/3. A process
%% that crashed exits with `{Reason, Stack}`.
describe_exit({Reason, [Frame | _] = Stack}) when is_tuple(Frame), tuple_size(Frame) >= 3,
                                                 is_atom(element(1, Frame)) ->
    {unicode:characters_to_binary([describe(error, Reason), $\n, stack_trace(Stack)]),
     unicode:characters_to_binary(io_lib:format("~0tp", [{Reason, Stack}]))};
describe_exit(Reason) ->
    {unicode:characters_to_binary(describe(exit, Reason)),
     unicode:characters_to_binary(io_lib:format("~0tp", [Reason]))}.

%% A Gleam panic or failed assertion as a sentence: the text is written in the code, and
%% the values an assertion compared are left out. Anything else as its class and kind.
describe(_, #{gleam_error := Kind, message := Message, module := Module, function := Function,
              line := Line}) ->
    io_lib:format("~ts in ~ts.~ts, line ~b: ~ts", [Kind, Module, Function, Line, Message]);
%% The SDK's own errors, which explain themselves.
describe(Class, {temporal_not_in_workflow = Kind, Explanation}) ->
    io_lib:format("~p: ~p: ~ts", [Class, Kind, Explanation]);
describe(Class, {temporal_library_not_loaded = Kind, Explanation, Why}) ->
    io_lib:format("~p: ~p: ~ts (~ts)", [Class, Kind, Explanation, Why]);
describe(Class, Kind) when is_atom(Kind) ->
    io_lib:format("~p: ~p", [Class, Kind]);
describe(Class, {Kind, Value}) when Kind =:= badmatch; Kind =:= case_clause;
                                    Kind =:= try_clause; Kind =:= badrecord ->
    io_lib:format("~p: ~p: a value did not match (~ts)", [Class, Kind, type_of(Value)]);
describe(Class, Reason) when is_tuple(Reason), tuple_size(Reason) >= 1,
                             is_atom(element(1, Reason)) ->
    [_ | Rest] = tuple_to_list(Reason),
    io_lib:format("~p: ~p (with ~ts)",
                  [Class, element(1, Reason), lists:join(", ", [type_of(V) || V <- Rest])]);
describe(Class, Reason) ->
    io_lib:format("~p: ~ts", [Class, type_of(Reason)]).

%% The type of a value and its size, never the value.
type_of(V) when is_integer(V) -> "an integer";
type_of(V) when is_float(V) -> "a float";
type_of(V) when is_atom(V) -> "an atom";
type_of(V) when is_binary(V) -> io_lib:format("a binary of ~b bytes", [byte_size(V)]);
type_of(V) when is_bitstring(V) -> "a bit string";
type_of(V) when is_list(V) -> "a list";
type_of(V) when is_tuple(V) -> io_lib:format("a tuple of ~b elements", [tuple_size(V)]);
type_of(V) when is_map(V) -> io_lib:format("a map of ~b entries", [map_size(V)]);
type_of(V) when is_function(V) -> "a function";
type_of(V) when is_pid(V) -> "a pid";
type_of(V) when is_reference(V) -> "a reference";
type_of(V) when is_port(V) -> "a port";
type_of(_) -> "a term".

%% One line per frame: `module:function/arity (file:line)`. A frame of a function that
%% matched no clause holds the arguments it was called with; only their number is kept.
stack_trace(Stack) when is_list(Stack) ->
    lists:join($\n, [frame(Frame) || Frame <- Stack]);
stack_trace(_) ->
    [].

frame({Module, Function, Args, Location}) when is_list(Args) ->
    frame({Module, Function, length(Args), Location});
frame({Module, Function, Arity, Location}) when is_integer(Arity), is_list(Location) ->
    Where = case {lists:keyfind(file, 1, Location), lists:keyfind(line, 1, Location)} of
                {{file, File}, {line, Line}} when is_integer(Line) ->
                    io_lib:format(" (~ts:~b)", [File, Line]);
                {{file, File}, _} -> io_lib:format(" (~ts)", [File]);
                _ -> ""
            end,
    io_lib:format("~ts:~ts/~b~ts", [atom_to_binary(Module), name(Function), Arity, Where]);
frame(_) ->
    "?".

name(Function) when is_atom(Function) -> atom_to_binary(Function);
name(_) -> <<"?">>.

%% Whether this VM is linked against musl, as on Alpine Linux.
is_musl() ->
    string:find(erlang:system_info(system_architecture), "musl") =/= nomatch.

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

%% JSON text with every escaped UTF-16 surrogate that has no partner replaced by the
%% escape of U+FFFD. A backslash appears in JSON only inside a string and always starts an
%% escape, so the escapes can be walked without parsing anything else.
replace_lone_surrogates(Json) ->
    replace_lone_surrogates(Json, <<>>).

replace_lone_surrogates(<<"\\u", High:4/binary, "\\u", Low:4/binary, Rest/binary>>, Acc) ->
    case {surrogate(High), surrogate(Low)} of
        {high, low} ->
            replace_lone_surrogates(Rest, <<Acc/binary, "\\u", High/binary, "\\u", Low/binary>>);
        {none, _} ->
            replace_lone_surrogates(<<"\\u", Low/binary, Rest/binary>>,
                                    <<Acc/binary, "\\u", High/binary>>);
        {_, _} ->
            replace_lone_surrogates(<<"\\u", Low/binary, Rest/binary>>,
                                    <<Acc/binary, "\\ufffd">>)
    end;
replace_lone_surrogates(<<"\\u", Code:4/binary, Rest/binary>>, Acc) ->
    case surrogate(Code) of
        none -> replace_lone_surrogates(Rest, <<Acc/binary, "\\u", Code/binary>>);
        _ -> replace_lone_surrogates(Rest, <<Acc/binary, "\\ufffd">>)
    end;
replace_lone_surrogates(<<$\\, Escaped, Rest/binary>>, Acc) ->
    replace_lone_surrogates(Rest, <<Acc/binary, $\\, Escaped>>);
replace_lone_surrogates(<<Byte, Rest/binary>>, Acc) ->
    replace_lone_surrogates(Rest, <<Acc/binary, Byte>>);
replace_lone_surrogates(<<>>, Acc) ->
    Acc.

surrogate(<<D, X, _, _>>) when (D =:= $d orelse D =:= $D) ->
    if
        X =:= $8; X =:= $9; X =:= $a; X =:= $b; X =:= $A; X =:= $B -> high;
        X =:= $c; X =:= $d; X =:= $e; X =:= $f; X =:= $C; X =:= $D; X =:= $E; X =:= $F -> low;
        true -> none
    end;
surrogate(_) ->
    none.

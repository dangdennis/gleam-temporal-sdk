-module(temporal_test_ffi).
-export([unique/0, cmd/1, read_file/1, record/2, recorded/1, test_server_path/0, tls_proxy/2, capture_logs/1, capture_log_metadata/1, log/2, write_file/2, linked/1, monitored_by/1, quietly/1, shut_down/1, memory_mb/0, env_int/2]).

unique() ->
    integer_to_binary(erlang:system_time(microsecond)).

cmd(Command) ->
    unicode:characters_to_binary(os:cmd(binary_to_list(Command))).

read_file(Path) ->
    case file:read_file(Path) of
        {ok, Bytes} -> {ok, Bytes};
        {error, Reason} -> {error, atom_to_binary(Reason)}
    end.

%% A scratch store shared by activities and the test that started them.
record(Key, Value) ->
    persistent_term:put({temporal_test, Key}, Value),
    nil.

recorded(Key) ->
    persistent_term:get({temporal_test, Key}, <<>>).

%% A test server already on disk, named by TEMPORAL_TEST_SERVER or found on the PATH, so
%% that the suite does not download one.
test_server_path() ->
    case os:getenv("TEMPORAL_TEST_SERVER") of
        false ->
            case os:find_executable("temporal-test-server") of
                false -> {error, nil};
                Path -> {ok, unicode:characters_to_binary(Path)}
            end;
        Path -> {ok, unicode:characters_to_binary(Path)}
    end.

%% A TLS-terminating proxy in front of the plaintext development server, so that TLS
%% connections can be tested locally. Listens on a free port, requires a client certificate
%% signed by the CA in Dir, and relays to 127.0.0.1:UpstreamPort. Returns the port.
tls_proxy(Dir, UpstreamPort) ->
    {ok, _} = application:ensure_all_started(ssl),
    File = fun(Name) -> filename:join(binary_to_list(Dir), Name) end,
    Parent = self(),
    spawn(fun() ->
        {ok, Listen} = ssl:listen(0, [
            binary, {active, false}, {reuseaddr, true},
            {certfile, File("server.pem")}, {keyfile, File("server.key")},
            {cacertfile, File("ca.pem")},
            {verify, verify_peer}, {fail_if_no_peer_cert, true},
            {alpn_preferred_protocols, [<<"h2">>]}, {log_level, none}
        ]),
        {ok, {_, Port}} = ssl:sockname(Listen),
        Parent ! {tls_proxy, Port},
        accept_loop(Listen, UpstreamPort)
    end),
    receive {tls_proxy, Port} -> Port end.

accept_loop(Listen, UpstreamPort) ->
    {ok, Transport} = ssl:transport_accept(Listen),
    spawn(fun() ->
        case ssl:handshake(Transport, 5000) of
            {ok, Tls} ->
                {ok, Tcp} = gen_tcp:connect("127.0.0.1", UpstreamPort,
                                            [binary, {active, false}]),
                Self = self(),
                spawn_link(fun() -> relay(fun() -> ssl:recv(Tls, 0) end,
                                          fun(D) -> gen_tcp:send(Tcp, D) end, Self) end),
                spawn_link(fun() -> relay(fun() -> gen_tcp:recv(Tcp, 0) end,
                                          fun(D) -> ssl:send(Tls, D) end, Self) end),
                receive closed -> ok end,
                ssl:close(Tls),
                gen_tcp:close(Tcp);
            {error, _} -> ok
        end
    end),
    accept_loop(Listen, UpstreamPort).

relay(Recv, Send, Owner) ->
    case Recv() of
        {ok, Data} ->
            case Send(Data) of
                ok -> relay(Recv, Send, Owner);
                {error, _} -> Owner ! closed
            end;
        {error, _} -> Owner ! closed
    end.

%% Run Fun and return the messages logged under the temporal domain meanwhile, with the
%% workflow id each carried. The default handler is silenced so the lines do not also reach
%% the test output.
capture_logs(Fun) ->
    capture(Fun, workflow_id).

%% Like capture_logs/1, but each line comes with all the metadata its caller gave it:
%% {Text, ["key=value", ...]}, values printed as Erlang terms so that their types show,
%% sorted by key. What logger adds by itself is left out.
capture_log_metadata(Fun) ->
    [{Text, Meta} || {_, Text, Meta} <- capture(Fun, everything)].

capture(Fun, Detail) ->
    #{level := Level} = logger:get_primary_config(),
    {ok, #{level := DefaultLevel}} = logger:get_handler_config(default),
    ok = logger:set_primary_config(level, debug),
    ok = logger:set_handler_config(default, level, none),
    ok = logger:add_handler(temporal_test, ?MODULE, #{config => {self(), Detail}}),
    Fun(),
    ok = logger:remove_handler(temporal_test),
    ok = logger:set_handler_config(default, level, DefaultLevel),
    ok = logger:set_primary_config(level, Level),
    drain_logs([]).

drain_logs(Seen) ->
    receive {temporal_log, Line} -> drain_logs([Line | Seen])
    after 0 -> lists:reverse(Seen)
    end.

%% logger handler callback.
log(#{level := Level, msg := {Format, Args}, meta := #{domain := [temporal]} = Meta},
    #{config := {Pid, Detail}}) ->
    Text = unicode:characters_to_binary(io_lib:format(Format, Args)),
    Pid ! {temporal_log, {Level, Text, detail(Detail, Meta)}},
    ok;
log(_, _) ->
    ok.

detail(workflow_id, Meta) ->
    maps:get(workflow_id, Meta, <<>>);
detail(everything, Meta) ->
    Given = maps:without([domain, pid, gl, time, mfa, file, line], Meta),
    [unicode:characters_to_binary(io_lib:format("~ts=~0tp", [K, V]))
     || {K, V} <- lists:sort(maps:to_list(Given))].

write_file(Path, Bytes) ->
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, Bytes),
    nil.

%% The processes linked to Pid, and the processes monitoring it.
linked(Pid) ->
    case erlang:process_info(Pid, links) of
        {links, Links} -> [P || P <- Links, is_pid(P)];
        undefined -> []
    end.

monitored_by(Pid) ->
    case erlang:process_info(Pid, monitored_by) of
        {monitored_by, Pids} -> [P || P <- Pids, is_pid(P)];
        undefined -> []
    end.

%% Run Fun with the default log handler silenced, for tests that kill processes on purpose.
quietly(Fun) ->
    {ok, #{level := Level}} = logger:get_handler_config(default),
    ok = logger:set_handler_config(default, level, none),
    try Fun() after logger:set_handler_config(default, level, Level) end.

%% Stop a supervisor the way its own supervisor would.
shut_down(Pid) ->
    exit(Pid, shutdown),
    nil.

memory_mb() ->
    erlang:memory(total) div (1024 * 1024).

env_int(Name, Default) ->
    case os:getenv(binary_to_list(Name)) of
        false -> Default;
        Value -> list_to_integer(Value)
    end.

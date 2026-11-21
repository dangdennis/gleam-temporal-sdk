-module(temporal_test_ffi).
-export([unique/0, cmd/1, read_file/1, record/2, recorded/1, test_server_path/0, tls_proxy/2, capture_logs/1, capture_log_metadata/1, log/2, write_file/2, linked/1, monitored_by/1, quietly/1, shut_down/1, memory_mb/0, env_int/2, tcp_proxy/1, tcp_proxy_mode/2, silent_listener/0, run_only/1, await_log/2, record_self/1, recorded_pid/1, os_pid/0,
         start_application/2, workers_drained_on_sigterm/0, signal_handlers/0, quiet_notices/0, atom_count/0, crash/2]).

%% Crash the way Erlang code does, with Value inside the error term or, for a function
%% that matches no clause, in the stack trace's argument list.
crash(<<"function_clause">>, Value) -> only_nil(Value, 1);
crash(<<"badarg">>, Value) -> _ = binary_to_existing_atom(Value), nil;
crash(<<"case_clause">>, Value) -> case Value of nil -> nil end;
crash(<<"badmatch">>, Value) -> nil = Value;
crash(<<"error">>, Value) -> erlang:error({order_rejected, Value, 7});
crash(<<"throw">>, Value) -> throw({rejected, Value});
crash(<<"exit">>, Value) -> exit({shutdown, Value}).

only_nil(nil, _) -> nil.

atom_count() ->
    erlang:system_info(atom_count).

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

%% Run Fun and return the messages the SDK logged meanwhile, with the
%% workflow id each carried. The default handler is silenced so the lines do not also reach
%% the test output.
capture_logs(Fun) ->
    capture(Fun, workflow_id).

%% Like capture_logs/1, but each line comes with all the metadata its caller gave it:
%% {Text, ["key=value", ...]}, values printed as Erlang terms so that their types show,
%% sorted. An entry of the `fields` map is "fields.key=value". What logger adds by itself
%% is left out.
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

%% Inside capture_logs/1: wait until a line that contains Text has been logged. The lines
%% read while waiting are put back for capture_logs/1 to return.
await_log(Text, Timeout) ->
    await_log(Text, erlang:monotonic_time(millisecond) + Timeout, []).

await_log(Text, Deadline, Seen) ->
    PutBack = fun() -> [self() ! {temporal_log, Line} || Line <- lists:reverse(Seen)] end,
    receive
        {temporal_log, {_, Logged, _} = Line} ->
            case binary:match(Logged, Text) of
                nomatch -> await_log(Text, Deadline, [Line | Seen]);
                _ -> PutBack(), self() ! {temporal_log, Line}, true
            end
    after max(0, Deadline - erlang:monotonic_time(millisecond)) ->
        PutBack(), false
    end.

drain_logs(Seen) ->
    receive {temporal_log, Line} -> drain_logs([Line | Seen])
    after 0 -> lists:reverse(Seen)
    end.

%% logger handler callback.
log(#{level := Level, msg := {Format, Args}, meta := #{temporal := true} = Meta},
    #{config := {Pid, Detail}}) ->
    Text = unicode:characters_to_binary(io_lib:format(Format, Args)),
    Pid ! {temporal_log, {Level, Text, detail(Detail, Meta)}},
    ok;
log(_, _) ->
    ok.

detail(workflow_id, Meta) ->
    maps:get(workflow_id, Meta, <<>>);
detail(everything, Meta) ->
    Given = maps:without([temporal, pid, gl, time, mfa, file, line, fields], Meta),
    Fields = [{[<<"fields.">>, K], V} || {K, V} <- maps:to_list(maps:get(fields, Meta, #{}))],
    lists:sort([unicode:characters_to_binary(io_lib:format("~ts=~0tp", [K, V]))
                || {K, V} <- maps:to_list(Given) ++ Fields]).

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

%% A TCP proxy in front of 127.0.0.1:UpstreamPort, whose link can be broken in the ways a
%% network breaks. Listens on a free port and returns it. See tcp_proxy_mode/2.
tcp_proxy(UpstreamPort) ->
    Parent = self(),
    spawn(fun() ->
        {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true},
                                          {ip, {127, 0, 0, 1}}]),
        {ok, Port} = inet:port(Listen),
        persistent_term:put({tcp_proxy, Port}, forward),
        Parent ! {tcp_proxy, Port},
        tcp_accept_loop(Listen, Port, UpstreamPort)
    end),
    receive {tcp_proxy, Port} -> Port end.

%% `forward` relays.
%%
%% `blackhole` swallows every byte in both directions and closes nothing, as a dropped route
%% does. A connection that has lost bytes stays dead when the proxy forwards again, as one
%% whose route came back without its state would: only new connections work.
%%
%% `not_found` closes the connections there are and answers every request on a new one
%% with HTTP 404, as a load balancer that has lost its backend does. gRPC reads that as
%% UNIMPLEMENTED, which is not an error worth retrying.
tcp_proxy_mode(Port, Mode) ->
    persistent_term:put({tcp_proxy, Port}, Mode),
    nil.

tcp_proxy_mode(Port) ->
    persistent_term:get({tcp_proxy, Port}).

tcp_accept_loop(Listen, Port, UpstreamPort) ->
    {ok, Client} = gen_tcp:accept(Listen),
    spawn(fun() ->
        case tcp_proxy_mode(Port) of
            not_found -> http2_not_found(Client, Port);
            _ -> tcp_relay(Client, Port, UpstreamPort)
        end
    end),
    tcp_accept_loop(Listen, Port, UpstreamPort).

tcp_relay(Client, Port, UpstreamPort) ->
    {ok, Upstream} = gen_tcp:connect("127.0.0.1", UpstreamPort, [binary, {active, false}]),
    Dead = atomics:new(1, []),
    Self = self(),
    Relay = fun(From, To) ->
        spawn_link(fun() -> tcp_relay(From, To, Port, Dead, Self) end)
    end,
    Relay(Client, Upstream),
    Relay(Upstream, Client),
    receive closed -> ok end,
    gen_tcp:close(Client),
    gen_tcp:close(Upstream).

tcp_relay(From, To, Port, Dead, Owner) ->
    %% Woken often, so that a change of mode is seen on a quiet connection too.
    Received = gen_tcp:recv(From, 0, 100),
    case tcp_proxy_mode(Port) of
        blackhole -> atomics:put(Dead, 1, 1);
        _ -> ok
    end,
    case {Received, tcp_proxy_mode(Port), atomics:get(Dead, 1)} of
        {_, not_found, _} -> Owner ! closed;
        {{error, timeout}, _, _} -> tcp_relay(From, To, Port, Dead, Owner);
        {{error, _}, _, _} -> Owner ! closed;
        {{ok, _}, _, 1} -> tcp_relay(From, To, Port, Dead, Owner);
        {{ok, Data}, _, 0} ->
            case gen_tcp:send(To, Data) of
                ok -> tcp_relay(From, To, Port, Dead, Owner);
                {error, _} -> Owner ! closed
            end
    end.

%% Just enough HTTP/2 to refuse every request: read the client's preface, send empty
%% settings, then answer frame by frame.
http2_not_found(Socket, Port) ->
    case gen_tcp:recv(Socket, 24) of
        {ok, <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>} ->
            ok = gen_tcp:send(Socket, <<0:24, 4, 0, 0:32>>),
            http2_frames(Socket, Port);
        _ -> gen_tcp:close(Socket)
    end.

http2_frames(Socket, Port) ->
    case {http2_frame(Socket), tcp_proxy_mode(Port)} of
        {{ok, Type, Flags, Stream, Payload}, not_found} ->
            Reply = case {Type, Flags band 1, Flags band 4} of
                %% Data: give the connection's window back.
                {0, _, _} when byte_size(Payload) > 0 ->
                    <<4:24, 8, 0, 0:32, 0:1, (byte_size(Payload)):31>>;
                %% Headers, complete: `:status 404`, and the stream ends.
                {1, _, 4} -> <<1:24, 1, 5, 0:1, Stream:31, 16#8d>>;
                %% Settings and pings are acknowledged.
                {4, 0, _} -> <<0:24, 4, 1, 0:32>>;
                {6, 0, _} -> <<8:24, 6, 1, 0:32, Payload/binary>>;
                _ -> <<>>
            end,
            case gen_tcp:send(Socket, Reply) of
                ok -> http2_frames(Socket, Port);
                {error, _} -> gen_tcp:close(Socket)
            end;
        %% The backend is back: the client reconnects and is relayed to it.
        _ -> gen_tcp:close(Socket)
    end.

http2_frame(Socket) ->
    case gen_tcp:recv(Socket, 9) of
        {ok, <<0:24, Type, Flags, _:1, Stream:31>>} -> {ok, Type, Flags, Stream, <<>>};
        {ok, <<Length:24, Type, Flags, _:1, Stream:31>>} ->
            case gen_tcp:recv(Socket, Length) of
                {ok, Payload} -> {ok, Type, Flags, Stream, Payload};
                Error -> Error
            end;
        Error -> Error
    end.

%% A port that accepts connections and never says anything on them.
silent_listener() ->
    Parent = self(),
    spawn(fun() ->
        {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
        {ok, Port} = inet:port(Listen),
        Parent ! {silent_listener, Port},
        silent_accept_loop(Listen, [])
    end),
    receive {silent_listener, Port} -> Port end.

silent_accept_loop(Listen, Held) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    silent_accept_loop(Listen, [Socket | Held]).

%% Run the tests of the named modules only: `gleam test` runs all of them.
run_only(Modules) ->
    case eunit:test([binary_to_atom(M) || M <- Modules], [verbose, {scale_timeouts, 10}]) of
        ok -> halt(0);
        _ -> halt(1)
    end.

%% The calling process, kept where the test that started it can find it.
record_self(Key) ->
    persistent_term:put({temporal_test_pid, Key}, self()),
    nil.

recorded_pid(Key) ->
    case persistent_term:get({temporal_test_pid, Key}, none) of
        none -> {error, nil};
        Pid -> {ok, Pid}
    end.

os_pid() ->
    list_to_binary(os:getpid()).

%% Start an OTP application whose top supervisor is the one StartSupervisor starts, so
%% that `init:stop()` shuts it down the way it shuts down any application.
start_application(Name, StartSupervisor) ->
    App = binary_to_atom(Name),
    ok = application:load({application, App,
                           [{description, "temporal_gleam test application"},
                            {vsn, "1"},
                            {applications, [kernel, stdlib]},
                            {mod, {temporal_test_app, StartSupervisor}}]}),
    ok = application:start(App),
    nil.

%% How many workers the SDK's SIGTERM handler holds, zero if it is not installed.
workers_drained_on_sigterm() ->
    case lists:keyfind(temporal_signal, 1, sys:get_state(erl_signal_server)) of
        {_, _, #{workers := Workers}} -> map_size(Workers);
        false -> 0
    end.

%% The handlers the VM delivers signals to.
signal_handlers() ->
    unicode:characters_to_binary(
      io_lib:format("~0p", [gen_event:which_handlers(erl_signal_server)])).

%% Print warnings and errors only: every worker logs a notice when it starts and when it
%% stops, and the suite starts hundreds.
quiet_notices() ->
    ok = logger:set_handler_config(default, level, warning),
    nil.

%% Lets workers finish what they are doing when the operating system sends SIGTERM,
%% instead of the VM stopping underneath them.
%%
%% The VM delivers signals to the handlers of the `erl_signal_server` event manager, and
%% its own handler, `erl_signal_handler`, answers SIGTERM with `init:stop()`. This module
%% takes that handler's place. On SIGTERM it wakes the processes waiting in `await/0` and
%% drains every worker registered with `register_worker/2`. Once they have drained it
%% hands the signal to `erl_signal_handler`, unless some process asked for the signal
%% with `install/0` or `await/0`: then exiting is left to the program. Every other signal
%% goes straight to `erl_signal_handler`, so that it does what it always did. SIGINT is
%% not among the signals the VM hands out.
-module(temporal_signal).
-behaviour(gen_event).

-export([install/0, await/0, register_worker/2, forget_worker/1]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

%% Take SIGTERM over for a program that exits by itself once the signal has come.
install() ->
    add_handler(),
    gen_event:call(erl_signal_server, ?MODULE, asked, infinity),
    nil.

%% Have `Drain` called on SIGTERM for as long as `Pid` lives. It must return once the
%% worker has stopped working, and leave `Pid` alive.
register_worker(Pid, Drain) ->
    add_handler(),
    gen_event:call(erl_signal_server, ?MODULE, {register, Pid, Drain}, infinity),
    nil.

%% `Pid` has stopped and needs no draining. In a VM that is stopping the event manager may
%% be gone already.
forget_worker(Pid) ->
    try installed() andalso
            gen_event:call(erl_signal_server, ?MODULE, {forget, Pid}, infinity)
    catch _:_ -> ok
    end,
    nil.

await() ->
    add_handler(),
    Ref = make_ref(),
    case gen_event:call(erl_signal_server, ?MODULE, {await, self(), Ref}, infinity) of
        signalled -> nil;
        waiting -> receive {temporal_shutdown_signal, Ref} -> nil end
    end.

add_handler() ->
    case installed() of
        true -> nil;
        false -> add_handler_once()
    end.

installed() ->
    lists:member(?MODULE, gen_event:which_handlers(erl_signal_server)).

%% A registered name is the lock that keeps two processes from each adding the handler.
add_handler_once() ->
    try register(temporal_signal_install, self()) of
        true ->
            case installed() of
                true -> ok;
                false ->
                    ok = os:set_signal(sigterm, handle),
                    ok = gen_event:swap_handler(erl_signal_server,
                                                {erl_signal_handler, []}, {?MODULE, []})
            end,
            unregister(temporal_signal_install),
            nil
    catch
        error:badarg ->
            timer:sleep(1),
            add_handler()
    end.

init(_) ->
    {ok, Default} = erl_signal_handler:init([]),
    {ok, #{signalled => false,
           %% Whether a process asked for the signal, and so exits the program itself.
           asked => false,
           waiters => [],
           %% Pid => Drain of every registered worker.
           workers => #{},
           default => Default}}.

%% A second SIGTERM changes nothing: the first is still being answered.
handle_event(sigterm, #{signalled := true} = State) ->
    {ok, State};
handle_event(sigterm, #{waiters := Waiters, workers := Workers} = State) ->
    Drains = maps:values(alive(Workers)),
    temporal_ffi:log(notice, <<"temporal: SIGTERM received, stopping gracefully">>,
                     [{<<"workers">>, length(Drains)}]),
    [Pid ! {temporal_shutdown_signal, Ref} || {Pid, Ref} <- Waiters],
    spawn(fun() ->
                  drain(Drains),
                  gen_event:call(erl_signal_server, ?MODULE, drained, infinity)
          end),
    {ok, State#{signalled := true, waiters := []}};
handle_event(Signal, #{default := Default} = State) ->
    {ok, Default1} = erl_signal_handler:handle_event(Signal, Default),
    {ok, State#{default := Default1}}.

%% Drain the workers at the same time and wait for each to be done, however it ends: a
%% worker that is stopped meanwhile takes its manager, and so the call, with it.
drain(Drains) ->
    Monitors = [element(2, spawn_monitor(fun() -> catch Drain() end)) || Drain <- Drains],
    [receive {'DOWN', Monitor, process, _, _} -> ok end || Monitor <- Monitors],
    ok.

handle_call(asked, State) ->
    {ok, ok, State#{asked := true}};
%% Workers that died without saying so are dropped here, so that they do not pile up. The
%% event manager cannot monitor them: every handler would be sent the message.
handle_call({register, Pid, Drain}, #{workers := Workers} = State) ->
    {ok, ok, State#{workers := (alive(Workers))#{Pid => Drain}}};
handle_call({forget, Pid}, #{workers := Workers} = State) ->
    {ok, ok, State#{workers := maps:remove(Pid, Workers)}};
%% The workers have drained. A program that asked for the signal exits by itself; any
%% other is stopped as the VM would have stopped it.
handle_call(drained, #{asked := false, default := Default} = State) ->
    {ok, Default1} = erl_signal_handler:handle_event(sigterm, Default),
    {ok, ok, State#{default := Default1}};
handle_call({await, _, _}, #{signalled := true} = State) ->
    {ok, signalled, State#{asked := true}};
handle_call({await, Pid, Ref}, #{waiters := Waiters} = State) ->
    {ok, waiting, State#{asked := true, waiters := [{Pid, Ref} | Waiters]}};
handle_call(_, State) ->
    {ok, ok, State}.

handle_info(_, State) ->
    {ok, State}.

alive(Workers) ->
    maps:filter(fun(Pid, _) -> is_process_alive(Pid) end, Workers).

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

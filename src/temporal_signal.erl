%% Lets a program stop its workers when the operating system sends SIGTERM, instead of the
%% VM stopping underneath them.
%%
%% The VM delivers signals to the handlers of the `erl_signal_server` event manager, and
%% its own handler, `erl_signal_handler`, answers SIGTERM with `init:stop()`. This module
%% takes that handler's place: SIGTERM wakes the processes waiting in `await/0`, and every
%% other signal is passed to `erl_signal_handler` so that it does what it always did.
%% SIGINT is not among the signals the VM hands out.
-module(temporal_signal).
-behaviour(gen_event).

-export([install/0, await/0]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

install() ->
    case installed() of
        true -> nil;
        false -> install_once()
    end.

installed() ->
    lists:member(?MODULE, gen_event:which_handlers(erl_signal_server)).

%% A registered name is the lock that keeps two processes from each adding the handler.
install_once() ->
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
            install()
    end.

await() ->
    install(),
    Ref = make_ref(),
    case gen_event:call(erl_signal_server, ?MODULE, {await, self(), Ref}, infinity) of
        signalled -> nil;
        waiting -> receive {temporal_shutdown_signal, Ref} -> nil end
    end.

init(_) ->
    {ok, Default} = erl_signal_handler:init([]),
    {ok, #{signalled => false, waiters => [], default => Default}}.

handle_event(sigterm, #{waiters := Waiters} = State) ->
    temporal_ffi:log(notice, <<"temporal: SIGTERM received, stopping gracefully">>, []),
    [Pid ! {temporal_shutdown_signal, Ref} || {Pid, Ref} <- Waiters],
    {ok, State#{signalled := true, waiters := []}};
handle_event(Signal, #{default := Default} = State) ->
    {ok, Default1} = erl_signal_handler:handle_event(Signal, Default),
    {ok, State#{default := Default1}}.

handle_call({await, _, _}, #{signalled := true} = State) ->
    {ok, signalled, State};
handle_call({await, Pid, Ref}, #{waiters := Waiters} = State) ->
    {ok, waiting, State#{waiters := [{Pid, Ref} | Waiters]}};
handle_call(_, State) ->
    {ok, ok, State}.

handle_info(_, State) ->
    {ok, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

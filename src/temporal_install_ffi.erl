%% Downloads the prebuilt NIF for temporal/install. Kept apart from temporal_ffi so that it
%% runs when the NIF is missing.
-module(temporal_install_ffi).

-export([install/1, machine/0]).

-define(REPOSITORY, "dangdennis/gleam-temporal-sdk").

%% What temporal/install chooses the library by: the VM's architecture, the operating
%% system's name, and whether the system is Alpine Linux.
machine() ->
    {_, Os} = os:type(),
    {unicode:characters_to_binary(erlang:system_info(system_architecture)),
     atom_to_binary(Os),
     filelib:is_regular("/etc/alpine-release")}.

%% Fetch the library built for `Target`, a Rust target name, and this package version into
%% the package's priv directory. A tag given as the first command-line argument overrides
%% the version.
install(TargetBinary) ->
    Tag = tag(),
    Target = binary_to_list(TargetBinary),
    case os:find_executable("curl") of
        false ->
            {error, <<"curl is needed to download the library and was not found">>};
        Curl ->
            Priv = code:priv_dir(temporal_gleam),
            File = filename:join(Priv, "libtemporal_nif.so"),
            Partial = File ++ ".part",
            Url = "https://github.com/" ?REPOSITORY "/releases/download/" ++ Tag
                ++ "/libtemporal_nif-" ++ Target ++ ".so",
            ok = filelib:ensure_dir(File),
            Args = ["--fail", "--location", "--silent", "--show-error", "--output", Partial, Url],
            case run(Curl, Args) of
                {0, _} ->
                    ok = file:rename(Partial, File),
                    {ok, unicode:characters_to_binary(
                        ["installed the Temporal library ", Tag, " for ", Target])};
                {_, Output} ->
                    _ = file:delete(Partial),
                    {error, unicode:characters_to_binary(
                        ["could not download ", Url, "\n", Output,
                         "If release ", Tag, " has no library for ", Target,
                         ", compile it with bin/build-nif in the package directory"])}
            end
    end.

tag() ->
    case init:get_plain_arguments() of
        [Given | _] -> Given;
        [] ->
            _ = application:load(temporal_gleam),
            {ok, Version} = application:get_key(temporal_gleam, vsn),
            "v" ++ Version
    end.

run(Program, Args) ->
    Port = open_port({spawn_executable, Program},
                     [{args, Args}, exit_status, stderr_to_stdout, binary]),
    collect(Port, []).

collect(Port, Output) ->
    receive
        {Port, {data, Data}} -> collect(Port, [Data | Output]);
        {Port, {exit_status, Status}} -> {Status, lists:reverse(Output)}
    end.

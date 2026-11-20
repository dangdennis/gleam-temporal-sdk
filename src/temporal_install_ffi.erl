%% Downloads the prebuilt NIF for temporal/install. Kept apart from temporal_ffi so that it
%% runs when the NIF is missing.
-module(temporal_install_ffi).

-export([install/0]).

-define(REPOSITORY, "dangdennis/gleam-temporal-sdk").

%% Fetch the library matching this machine and this package version into the package's priv
%% directory. A tag given as the first command-line argument overrides the version.
install() ->
    Tag = tag(),
    case {target(), os:find_executable("curl")} of
        {{error, Machine}, _} ->
            {error, unicode:characters_to_binary(
                ["there is no prebuilt library for ", Machine,
                 "; compile it with bin/build-nif in the package directory"])};
        {_, false} ->
            {error, <<"curl is needed to download the library and was not found">>};
        {{ok, Target}, Curl} ->
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
                        ["could not download ", Url, "\n", Output])}
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

%% The Rust target name the release files are published under.
target() ->
    Architecture = erlang:system_info(system_architecture),
    Cpu = case Architecture of
              "x86_64" ++ _ -> "x86_64";
              "amd64" ++ _ -> "x86_64";
              "aarch64" ++ _ -> "aarch64";
              "arm64" ++ _ -> "aarch64";
              _ -> unknown
          end,
    case {Cpu, os:type()} of
        {unknown, _} -> {error, Architecture};
        {_, {unix, linux}} -> {ok, Cpu ++ "-unknown-linux-gnu"};
        {_, {unix, darwin}} -> {ok, Cpu ++ "-apple-darwin"};
        _ -> {error, Architecture}
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

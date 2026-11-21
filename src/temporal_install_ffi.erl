%% File and network primitives for temporal/install, which decides what to download and
%% whether to keep it. Kept apart from temporal_ffi so that it runs when the NIF is
%% missing.
-module(temporal_install_ffi).

-export([machine/0, version/0, library_path/0, read_checksums/0, download/2, sha256/1,
         move/2, delete/1]).

%% What temporal/install chooses the library by: the VM's architecture, the operating
%% system's name, and whether the system is Alpine Linux.
machine() ->
    {_, Os} = os:type(),
    {unicode:characters_to_binary(erlang:system_info(system_architecture)),
     atom_to_binary(Os),
     filelib:is_regular("/etc/alpine-release")}.

%% The package's own version. The library installed is always this version's.
version() ->
    _ = application:load(temporal_gleam),
    {ok, Version} = application:get_key(temporal_gleam, vsn),
    unicode:characters_to_binary(Version).

library_path() ->
    unicode:characters_to_binary(
      filename:join(code:priv_dir(temporal_gleam), "libtemporal_nif.so")).

read_checksums() ->
    File = filename:join(code:priv_dir(temporal_gleam), "checksums.txt"),
    case file:read_file(File) of
        {ok, Text} -> {ok, Text};
        {error, Reason} ->
            {error, unicode:characters_to_binary(
                      ["could not read ", File, ": ", file:format_error(Reason),
                       ". It lists the checksums a download is verified against, so "
                       "nothing can be installed without it. Compile the library with "
                       "bin/build-nif in the package directory"])}
    end.

%% Fetch Url into the file Path with curl. The error is curl's own output.
download(Url, Path) ->
    case os:find_executable("curl") of
        false ->
            {error, <<"curl is needed to download the library and was not found">>};
        Curl ->
            ok = filelib:ensure_dir(Path),
            Args = ["--fail", "--location", "--silent", "--show-error",
                    "--output", binary_to_list(Path), binary_to_list(Url)],
            case run(Curl, Args) of
                {0, _} -> {ok, nil};
                {_, Output} -> {error, unicode:characters_to_binary(Output)}
            end
    end.

%% The SHA-256 of the file at Path, as 64 lowercase hexadecimal digits.
sha256(Path) ->
    case file:read_file(Path) of
        {ok, Bytes} ->
            {ok, _} = application:ensure_all_started(crypto),
            {ok, binary:encode_hex(crypto:hash(sha256, Bytes), lowercase)};
        {error, Reason} -> {error, file_error("read", Path, Reason)}
    end.

move(From, To) ->
    case file:rename(From, To) of
        ok -> {ok, nil};
        {error, Reason} -> {error, file_error("put in place", To, Reason)}
    end.

delete(Path) ->
    _ = file:delete(Path),
    nil.

file_error(Verb, Path, Reason) ->
    unicode:characters_to_binary(["could not ", Verb, " ", Path, ": ",
                                  file:format_error(Reason)]).

run(Program, Args) ->
    Port = open_port({spawn_executable, Program},
                     [{args, Args}, exit_status, stderr_to_stdout, binary]),
    collect(Port, []).

collect(Port, Output) ->
    receive
        {Port, {data, Data}} -> collect(Port, [Data | Output]);
        {Port, {exit_status, Status}} -> {Status, lists:reverse(Output)}
    end.

-module(temporal_examples_test_ffi).
-export([cmd/1, test_server_path/0]).

%% Run a shell command and return its output.
cmd(Command) ->
    unicode:characters_to_binary(os:cmd(binary_to_list(Command))).

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

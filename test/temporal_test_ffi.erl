-module(temporal_test_ffi).
-export([unique/0, cmd/1, read_file/1, record/2, recorded/1, test_server_path/0]).

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

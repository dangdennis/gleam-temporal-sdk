-module(temporal_test_ffi).
-export([unique/0, cmd/1, read_file/1]).

unique() ->
    integer_to_binary(erlang:system_time(microsecond)).

cmd(Command) ->
    unicode:characters_to_binary(os:cmd(binary_to_list(Command))).

read_file(Path) ->
    case file:read_file(Path) of
        {ok, Bytes} -> {ok, Bytes};
        {error, Reason} -> {error, atom_to_binary(Reason)}
    end.

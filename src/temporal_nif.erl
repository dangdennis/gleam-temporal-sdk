%% NIF stubs for native/temporal_nif. Asynchronous functions take the caller's pid and a
%% reference and answer with `{temporal_nif, Ref, Result}`; use temporal_ffi rather than
%% calling these directly.
-module(temporal_nif).

-export([connect/7, describe_workflow/5, list_workflows/5, count_workflows/4, worker_new/5, replay_worker_new/2, worker_poll_workflow/3,
         worker_complete_workflow/4, worker_poll_activity/3, worker_complete_activity/5,
         worker_heartbeat/3, worker_shutdown/3, start_workflow/5, signal_workflow/7,
         query_workflow/7, cancel_workflow/5, terminate_workflow/6, workflow_result/5,
         fetch_history/5]).

-on_load(init/0).

init() ->
    Priv = case code:priv_dir(temporal_gleam) of
               {error, _} -> "priv";
               Dir -> Dir
           end,
    erlang:load_nif(filename:join(Priv, "libtemporal_nif"), 0).

connect(_, _, _, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
worker_new(_, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
replay_worker_new(_, _) -> erlang:nif_error(nif_not_loaded).
worker_poll_workflow(_, _, _) -> erlang:nif_error(nif_not_loaded).
worker_complete_workflow(_, _, _, _) -> erlang:nif_error(nif_not_loaded).
worker_poll_activity(_, _, _) -> erlang:nif_error(nif_not_loaded).
worker_complete_activity(_, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
worker_heartbeat(_, _, _) -> erlang:nif_error(nif_not_loaded).
worker_shutdown(_, _, _) -> erlang:nif_error(nif_not_loaded).
start_workflow(_, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
signal_workflow(_, _, _, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
query_workflow(_, _, _, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
cancel_workflow(_, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
terminate_workflow(_, _, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
workflow_result(_, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
fetch_history(_, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
describe_workflow(_, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
list_workflows(_, _, _, _, _) -> erlang:nif_error(nif_not_loaded).
count_workflows(_, _, _, _) -> erlang:nif_error(nif_not_loaded).

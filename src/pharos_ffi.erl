-module(pharos_ffi).

-export([
    %% Telemetry poller wrapper
    start_poller/1,
    list_measurements/1,
    %% Telemetry attach/detach
    attach_many/4,
    detach/1,
    %% Memory unit conversion
    convert_memory/2,
    %% Alert diagnostic
    collect_diagnostic/0,
    %% Custom telemetry measurement emitters
    emit_cluster_nodes/0,
    emit_host_memory/0,
    emit_host_disk/0,
    %% Cross-typed-name plumbing for the event bus
    manager_from_name/1,
    %% Supervisor shutdown
    shutdown_supervisor/1
]).

%% Politely stop a supervisor (and therefore its tree). Idempotent: a
%% second call against a dead pid is a no-op so `pharos:stop/1` is safe to
%% call multiple times.
shutdown_supervisor(Pid) when is_pid(Pid) ->
    case erlang:is_process_alive(Pid) of
        false ->
            nil;
        true ->
            try
                _ = supervisor:stop(Pid),
                nil
            catch
                _:_ -> nil
            end
    end;
shutdown_supervisor(_Other) ->
    nil.

%% ===========================================================================
%% Alert diagnostic
%% ===========================================================================

%% Capture a brief VM snapshot for the alert diagnostic field.
%% Returns a binary (Gleam String) describing memory usage and process count.
collect_diagnostic() ->
    Memory = erlang:memory(),
    TotalBytes = proplists:get_value(total, Memory, 0),
    TotalMb = TotalBytes / 1_048_576,
    Procs = erlang:system_info(process_count),
    iolist_to_binary(
        io_lib:format("memory=~.2fMb processes=~w", [TotalMb, Procs])
    ).

%% ===========================================================================
%% Telemetry poller
%% ===========================================================================

%% Translate Gleam-encoded poller options into the keyword list shape
%% telemetry_poller expects, then start the poller.
%%
%% Gleam's variant encoding (e.g. `{period, Ms}`, `{builtin, memory}`) does
%% not match telemetry_poller's expected term shapes (e.g. bare atom
%% `memory`, `{process_info, [{name, _}, ...]}`), so we translate at the
%% FFI boundary.
start_poller(GleamOptions) ->
    ErlangOptions = lists:map(fun translate_option/1, GleamOptions),
    case telemetry_poller:start_link(ErlangOptions) of
        {ok, Pid} -> {ok, Pid};
        ignore -> {error, poller_ignored};
        {error, Reason} -> {error, {poller_start_failed, format_reason(Reason)}}
    end.

translate_option({period, Ms}) ->
    {period, Ms};
translate_option({measurements, Specs}) ->
    {measurements, lists:map(fun translate_measurement/1, Specs)}.

translate_measurement({builtin, Name}) ->
    %% Built-in measurements (`memory`, `total_run_queue_lengths`,
    %% `system_counts`, `persistent_term`) are bare atoms in
    %% telemetry_poller's API.
    Name;
translate_measurement({process_info_spec, Name, Event, Keys}) ->
    {process_info, [{name, Name}, {event, Event}, {keys, Keys}]};
translate_measurement({custom, Module, Function, Args}) ->
    {Module, Function, Args}.

%% telemetry_poller normalises measurements into internal
%% {telemetry_poller_builtin, Name, []} tuples when storing them.
%% This shim converts those back to the bare atoms the Gleam side may want.
list_measurements(Poller) ->
    lists:map(fun restore_measurement/1, telemetry_poller:list_measurements(Poller)).

restore_measurement({telemetry_poller_builtin, memory, _}) ->
    memory;
restore_measurement({telemetry_poller_builtin, total_run_queue_lengths, _}) ->
    total_run_queue_lengths;
restore_measurement({telemetry_poller_builtin, system_counts, _}) ->
    system_counts;
restore_measurement({telemetry_poller_builtin, persistent_term, _}) ->
    persistent_term;
restore_measurement(Other) ->
    Other.

%% ===========================================================================
%% Telemetry attach / detach
%% ===========================================================================

%% Wraps a 2-arity Gleam handler in the 4-arity Erlang fun that
%% telemetry:attach_many/4 expects, packing the four arguments into a
%% TelemetryEvent record that Gleam can pattern-match on.
attach_many(HandlerId, Events, GleamHandler, Config) ->
    Handler = fun(EventName, Measurements, Metadata, Cfg) ->
        Event = {telemetry_event, EventName, Measurements, Metadata},
        GleamHandler(Event, Cfg)
    end,
    case telemetry:attach_many(HandlerId, Events, Handler, Config) of
        ok -> {ok, nil};
        {error, already_exists} -> {error, handler_already_attached};
        {error, Reason} -> {error, {attach_failed, format_reason(Reason)}}
    end.

detach(HandlerId) ->
    case telemetry:detach(HandlerId) of
        ok -> {ok, nil};
        {error, not_found} -> {error, handler_not_found};
        {error, Reason} -> {error, {detach_failed, format_reason(Reason)}}
    end.

%% ===========================================================================
%% Memory unit conversion
%% ===========================================================================

convert_memory(Measurements, Unit) ->
    Factor =
        case Unit of
            kb -> 1024;
            mb -> 1_048_576;
            gb -> 1_073_741_824
        end,
    maps:map(fun(_Key, Bytes) -> Bytes / Factor end, Measurements).

%% ===========================================================================
%% Custom telemetry measurement emitters.
%%
%% Each function is invoked by telemetry_poller every polling cycle. They
%% emit telemetry events whose decoded shape lives in pharos/measurement.gleam.
%% ===========================================================================

%% Emit `[pharos, cluster, nodes]`: this node's name plus connected nodes.
emit_cluster_nodes() ->
    Self = erlang:atom_to_binary(erlang:node(), utf8),
    Nodes = [erlang:atom_to_binary(N, utf8) || N <- erlang:nodes()],
    telemetry:execute(
        [pharos, cluster, nodes],
        #{count => length(Nodes)},
        #{self => Self, nodes => Nodes}
    ).

%% Scaffolded host-memory probe. Emits a placeholder event with
%% `status: unimplemented` so subscribers can verify wiring before the real
%% probe lands.
emit_host_memory() ->
    telemetry:execute(
        [pharos, host, memory],
        #{total_bytes => 0, used_bytes => 0, available_bytes => 0},
        #{status => unimplemented}
    ).

%% Scaffolded host-disk probe. Same shape as `emit_host_memory/0`.
emit_host_disk() ->
    telemetry:execute(
        [pharos, host, disk],
        #{total_bytes => 0, used_bytes => 0, available_bytes => 0},
        #{status => unimplemented}
    ).

%% ===========================================================================
%% Name-to-Manager cast.
%%
%% At the Erlang level a `Name(AlertEvent)` is just a registered atom and an
%% eparch `Manager(AlertEvent)` is just a pid-or-name reference. gen_event
%% accepts atoms wherever it accepts pids, so this is a zero-cost identity
%% function that crosses the Gleam type boundary.
%% ===========================================================================

manager_from_name(Name) -> Name.

%% ===========================================================================
%% Internal
%% ===========================================================================

format_reason(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).

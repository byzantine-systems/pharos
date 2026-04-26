%% https://www.erlang.org/docs/29/system/secure_coding.html#code-style
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
    emit_host_disk/1,
    emit_host_cpu/0,
    emit_host_network/0,
    emit_beam_scheduler/0,
    emit_beam_reductions/0,
    %% Cross-typed-name plumbing for the event bus
    manager_from_name/1,
    %% Supervisor shutdown
    shutdown_supervisor/1,
    %% Metric capture
    now_ms/0,
    %% Hot buffer (ETS circular buffer)
    name_to_atom/1,
    hot_buffer_init/2,
    hot_buffer_push/2,
    hot_buffer_drain/1,
    hot_buffer_peek/1,
    hot_buffer_size/1,
    %% Spillover buffer (Dets cold tier)
    spillover_open/2,
    spillover_push/2,
    spillover_push_all/2,
    spillover_drain/1,
    spillover_size/1,
    %% Connection manager transport
    brain_target/2,
    brain_connect/1,
    brain_deliver/2,
    %% Sink delivery
    encode_etf/1,
    webhook_post/2,
    json_encode/1
]).

%% Politely stop a supervisor (and therefore its tree). Idempotent: a
%% second call against a dead pid is a no-op so `pharos:stop/1` is safe to
%% call multiple times.
shutdown_supervisor(Pid) when is_pid(Pid) ->
    case erlang:is_process_alive(Pid) of
        false ->
            nil;
        true ->
            %% Drop the start_link link first so the supervisor's `shutdown`
            %% exit can't propagate back and take this caller down with it.
            erlang:unlink(Pid),
            Ref = erlang:monitor(process, Pid),
            %% Graceful teardown: a `shutdown` exit signal makes the supervisor
            %% terminate its children (reverse start order, honouring each
            %% child's shutdown timeout) and then exit. Monitor and wait so the
            %% whole tree has finished cleaning up before returning.
            exit(Pid, shutdown),
            receive
                {'DOWN', Ref, process, Pid, _Reason} -> nil
            after 30_000 ->
                %% Safety valve: never let stop/1 hang indefinitely.
                erlang:demonitor(Ref, [flush]),
                nil
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
translate_option({init_delay, Ms}) ->
    {init_delay, Ms};
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

%% Host-memory probe. Reads OS memory totals from `/proc/meminfo` (Linux) and
%% emits `[pharos, host, memory]` with real byte counts and no `status` key
%% (an absent status decodes as `Implemented` on the Gleam side). On platforms
%% without a readable `/proc/meminfo`, degrades to the placeholder zeros +
%% `status: unimplemented` so the event contract holds everywhere.
emit_host_memory() ->
    case read_meminfo() of
        {ok, Total, Available} ->
            Used = max(Total - Available, 0),
            telemetry:execute(
                [pharos, host, memory],
                #{
                    total_bytes => Total,
                    used_bytes => Used,
                    available_bytes => Available
                },
                #{}
            );
        error ->
            telemetry:execute(
                [pharos, host, memory],
                #{total_bytes => 0, used_bytes => 0, available_bytes => 0},
                #{status => unimplemented}
            )
    end.

%% Host-disk probe for the filesystem containing `Path`. Uses os_mon's
%% `disksup:get_disk_info/1` (synchronous, per-path; OTP 26+), which reports
%% total and available space in KiB plus a used-percentage. Emits bytes
%% (KiB * 1024) with no `status` key (an absent status decodes as
%% `Implemented`). Degrades to the placeholder zeros + `status: unimplemented`
%% when os_mon cannot determine the path's usage (it returns `0`s then).
emit_host_disk(Path) when is_binary(Path) ->
    _ = application:ensure_all_started(os_mon),
    case disksup:get_disk_info(erlang:binary_to_list(Path)) of
        [{_Id, TotalKib, AvailKib, _Capacity} | _] when TotalKib > 0 ->
            Total = TotalKib * 1024,
            Available = AvailKib * 1024,
            Used = max(Total - Available, 0),
            telemetry:execute(
                [pharos, host, disk],
                #{
                    total_bytes => Total,
                    used_bytes => Used,
                    available_bytes => Available
                },
                #{}
            );
        _Other ->
            telemetry:execute(
                [pharos, host, disk],
                #{total_bytes => 0, used_bytes => 0, available_bytes => 0},
                #{status => unimplemented}
            )
    end.

%% Host-CPU probe via os_mon's `cpu_sup`. `util/0` returns the busy percentage
%% since the calling process's previous `util/0` call, so cpu_sup tracks the
%% delta itself; the first sample after startup is unreliable (utilisation
%% since boot) and should be treated as garbage. Load averages come back scaled
%% by 256 (256 = load 1.00), so are divided to a plain multiplier.
emit_host_cpu() ->
    _ = application:ensure_all_started(os_mon),
    telemetry:execute(
        [pharos, host, cpu],
        #{
            util => cpu_number(cpu_sup:util()),
            load1 => cpu_number(cpu_sup:avg1()) / 256.0,
            load5 => cpu_number(cpu_sup:avg5()) / 256.0,
            load15 => cpu_number(cpu_sup:avg15()) / 256.0
        },
        #{}
    ).

%% cpu_sup functions return `{error, _}` when the OS port is unavailable; map
%% any non-number to 0 so a probe failure surfaces as a benign zero sample
%% rather than crashing the poller.
cpu_number(Value) when is_number(Value) -> Value;
cpu_number(_Other) -> 0.0.

%% Host-network probe: per-second throughput derived from the cumulative
%% counters in `/proc/net/dev`. The previous snapshot and its capture time live
%% in the calling poller process's dictionary, so deltas are computed without
%% any shared table; a poller restart simply resets the baseline (first sample
%% after that emits zero rates).
emit_host_network() ->
    Now = erlang:monotonic_time(millisecond),
    Totals = proc_net_dev_totals(),
    {RxBps, TxBps, RxPps, TxPps} =
        case erlang:get(pharos_net_prev) of
            {PrevTs, PrevTotals} when Now > PrevTs ->
                net_rates(Totals, PrevTotals, Now - PrevTs);
            _NoPrevOrSameTick ->
                {0.0, 0.0, 0.0, 0.0}
        end,
    erlang:put(pharos_net_prev, {Now, Totals}),
    telemetry:execute(
        [pharos, host, network],
        #{
            rx_bytes_per_sec => RxBps,
            tx_bytes_per_sec => TxBps,
            rx_packets_per_sec => RxPps,
            tx_packets_per_sec => TxPps
        },
        #{}
    ).

%% BEAM scheduler-utilisation probe. `scheduler_wall_time` must be enabled once
%% (idempotent), after which `statistics/1` returns cumulative {Active, Total}
%% busy/total time per scheduler. Utilisation is the ratio of the *deltas*
%% against the previous snapshot (kept in the process dictionary), expressed as
%% a percentage; the first sample after enabling emits zero.
emit_beam_scheduler() ->
    _ = erlang:system_flag(scheduler_wall_time, true),
    {Active, Total} = scheduler_wall_time_totals(),
    Util =
        case erlang:get(pharos_sched_prev) of
            {PrevActive, PrevTotal} when Total > PrevTotal ->
                100.0 * (Active - PrevActive) / (Total - PrevTotal);
            _NoPrevOrSameTick ->
                0.0
        end,
    erlang:put(pharos_sched_prev, {Active, Total}),
    telemetry:execute([pharos, vm, scheduler], #{utilization => Util}, #{}).

%% BEAM reduction-count probe. `erlang:statistics(reductions)` returns
%% `{Total, SinceLastCall}`; the second element is the reductions executed since
%% this BIF was last called, i.e. the work done in the last polling interval.
emit_beam_reductions() ->
    {_Total, SinceLastCall} = erlang:statistics(reductions),
    telemetry:execute([pharos, vm, reductions], #{count => SinceLastCall}, #{}).

%% Sum the receive/transmit byte and packet counters across every non-loopback
%% interface in `/proc/net/dev`. Each data line is
%% `iface: rxbytes rxpackets ... (8 rx cols) txbytes txpackets ... (8 tx cols)`.
proc_net_dev_totals() ->
    case file:read_file("/proc/net/dev") of
        {ok, Bin} ->
            Lines = binary:split(Bin, <<"\n">>, [global]),
            lists:foldl(fun proc_net_dev_line/2, {0, 0, 0, 0}, Lines);
        {error, _Reason} ->
            {0, 0, 0, 0}
    end.

proc_net_dev_line(Line, {Rxb, Txb, Rxp, Txp} = Acc) ->
    case binary:split(Line, <<":">>) of
        [IfaceRaw, Rest] ->
            case string:trim(IfaceRaw) of
                <<"lo">> ->
                    Acc;
                <<>> ->
                    Acc;
                _Iface ->
                    case [string:to_integer(F) || F <- net_fields(Rest)] of
                        [
                            {RxB, _},
                            {RxP, _},
                            _,
                            _,
                            _,
                            _,
                            _,
                            _,
                            {TxB, _},
                            {TxP, _}
                            | _
                        ] when
                            is_integer(RxB),
                            is_integer(RxP),
                            is_integer(TxB),
                            is_integer(TxP)
                        ->
                            {Rxb + RxB, Txb + TxB, Rxp + RxP, Txp + TxP};
                        _Unparsable ->
                            Acc
                    end
            end;
        _NoColon ->
            Acc
    end.

%% Split a `/proc/net/dev` value section into its whitespace-separated fields.
net_fields(Rest) ->
    [F || F <- binary:split(string:trim(Rest), <<" ">>, [global]), F =/= <<>>].

net_rates({Rxb, Txb, Rxp, Txp}, {PRxb, PTxb, PRxp, PTxp}, DtMs) ->
    Dt = DtMs / 1000.0,
    {
        per_sec(Rxb - PRxb, Dt),
        per_sec(Txb - PTxb, Dt),
        per_sec(Rxp - PRxp, Dt),
        per_sec(Txp - PTxp, Dt)
    }.

%% A negative delta means counters reset (interface reset / 32-bit wrap); clamp
%% to zero rather than emit a spurious negative rate.
per_sec(Delta, Dt) when Delta >= 0, Dt > 0 -> Delta / Dt;
per_sec(_Delta, _Dt) -> 0.0.

scheduler_wall_time_totals() ->
    case erlang:statistics(scheduler_wall_time) of
        undefined ->
            {0, 0};
        Schedulers ->
            lists:foldl(
                fun({_Id, Active, Total}, {AccA, AccT}) ->
                    {AccA + Active, AccT + Total}
                end,
                {0, 0},
                Schedulers
            )
    end.

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
%% Metric capture
%% ===========================================================================

%% Wall-clock capture timestamp for a Metric, in milliseconds.
now_ms() ->
    erlang:system_time(millisecond).

%% ===========================================================================
%% Hot buffer: a bounded circular buffer of metrics in ETS.
%%
%% The table is an `ordered_set` keyed by a monotonically increasing integer
%% sequence number, so iteration order is insertion order and the oldest data
%% row is always `ets:first/1`. Two bookkeeping rows live in the same table
%% under the fixed atom keys `'$seq'` (next sequence) and `'$capacity'`; since
%% in Erlang term order integers sort before atoms, those rows always sort
%% after every data row and are filtered out of reads with an `is_integer`
%% guard. The table is public + named (the name is the owner process's
%% `new_name` atom, so no extra atoms are minted) and dies with its owner.
%%
%% `public` is required because two distinct processes touch it lock-free: the
%% telemetry handler writes, the connection manager reads/drains. The contents
%% are non-sensitive host/VM metrics (no secrets), so the broad access mode
%% does not expose sensitive data (rule MSC-004).
%% ===========================================================================

%% A `process:name()` is already an atom at runtime (minted once by
%% `gleam_erlang_ffi:new_name/1`). Reusing it as the ETS table name avoids
%% creating any additional atoms: no `list_to_atom/1` on unbounded input.
name_to_atom(Name) -> Name.

hot_buffer_init(Table, Capacity) ->
    %% Idempotent within an owner's lifetime: a fresh owner gets a fresh table
    %% because the previous owner's table died with it.
    Table = ets:new(Table, [
        ordered_set, public, named_table, {write_concurrency, true}, {read_concurrency, true}
    ]),
    ets:insert(Table, {'$seq', 0}),
    ets:insert(Table, {'$capacity', Capacity}),
    nil.

hot_buffer_push(Table, Metric) ->
    Seq = ets:update_counter(Table, '$seq', {2, 1}),
    ets:insert(Table, {Seq, Metric}),
    Capacity = ets:lookup_element(Table, '$capacity', 2),
    case hot_buffer_size(Table) > Capacity of
        true ->
            %% Oldest data row = smallest integer key. The two atom-keyed
            %% bookkeeping rows sort after all integers, so `first/1` is safe
            %% to delete here (there is at least one data row over capacity).
            ets:delete(Table, ets:first(Table)),
            true;
        false ->
            false
    end.

hot_buffer_drain(Table) ->
    Metrics = hot_buffer_peek(Table),
    ets:select_delete(Table, [{{'$1', '_'}, [{is_integer, '$1'}], [true]}]),
    Metrics.

%% ordered_set `select` yields rows in key (= sequence = insertion) order, so
%% the result is oldest-first. The `is_integer` guard skips the bookkeeping rows.
hot_buffer_peek(Table) ->
    ets:select(Table, [{{'$1', '$2'}, [{is_integer, '$1'}], ['$2']}]).

%% Number of buffered metrics: total rows minus the two bookkeeping rows.
hot_buffer_size(Table) ->
    ets:info(Table, size) - 2.

%% ===========================================================================
%% Spillover buffer: an unbounded, disk-backed cold tier in Dets.
%%
%% When the Brain connection is lost and the hot (ETS) buffer fills, the
%% connection manager drains the ring into this Dets table so a long partition
%% never evicts data. On reconnect the manager drains Dets first (oldest data),
%% then the ring, then resumes real-time streaming.
%%
%% Like the hot buffer, rows are `{Seq, Metric}` keyed by a monotonically
%% increasing integer, with one bookkeeping row `{'$seq', N}` holding the next
%% sequence. Unlike ETS `ordered_set`, Dets is a hash table with no iteration
%% order, so `spillover_drain/1` sorts by `Seq` to restore oldest-first.
%% Integer keys sort before the `'$seq'` atom in Erlang term order, and reads
%% select on the `{Seq, Metric}` shape with an `is_integer` guard, so the
%% bookkeeping row is never returned.
%%
%% The Dets file holds non-sensitive host/VM metrics (no secrets), so its
%% on-disk presence does not expose sensitive data (rule MSC-004). The table
%% name reuses the owner's `new_name` atom (no extra atoms minted, rule
%% DSG-003). The owner process opens the file and holds it for its lifetime; an
%% unclean exit leaves the file to Dets's repair-on-open, which recovers it.
%% ===========================================================================

spillover_open(Table, Path) when is_binary(Path) ->
    %% A failed open is unexpected (bad path/permissions) and is left to
    %% propagate so the supervisor surfaces it (rule STL-001).
    {ok, Table} = dets:open_file(Table, [
        {file, erlang:binary_to_list(Path)}, {type, set}, {auto_save, 60000}
    ]),
    %% Seed the sequence counter only on first open; a reopened file keeps its
    %% existing counter (and any unflushed rows) so replay survives a restart.
    dets:insert_new(Table, {'$seq', 0}),
    nil.

spillover_push(Table, Metric) ->
    Seq = dets:update_counter(Table, '$seq', {2, 1}),
    dets:insert(Table, {Seq, Metric}),
    nil.

spillover_push_all(Table, Metrics) ->
    lists:foreach(fun(Metric) -> spillover_push(Table, Metric) end, Metrics),
    nil.

spillover_drain(Table) ->
    Rows = dets:select(Table, [{{'$1', '$2'}, [{is_integer, '$1'}], [{{'$1', '$2'}}]}]),
    %% Dets is unordered; sort by the integer sequence to get oldest-first.
    Sorted = lists:keysort(1, Rows),
    dets:select_delete(Table, [{{'$1', '_'}, [{is_integer, '$1'}], [true]}]),
    [Metric || {_Seq, Metric} <- Sorted].

%% Number of buffered metrics: total rows minus the one bookkeeping row.
spillover_size(Table) ->
    dets:info(Table, size) - 1.

%% ===========================================================================
%% Connection manager: Brain (BEAM-to-BEAM ETF) transport.
%% ===========================================================================

%% Resolve a Brain target's node and registered-process name to atoms ONCE, at
%% transport construction. `Node` and `Name` are operator-supplied config
%% (trusted and bounded, see `services.pharos` / `config.with_brain_stream`),
%% never network input. Resolving here rather than per message keeps atom
%% creation a single, auditable, construction-time event per configured target:
%% message/alert volume can never drive `binary_to_atom`, honouring secure
%% coding rule DSG-003 ("Do Not Abuse Atoms"). `binary_to_existing_atom` is not
%% usable here because a remote node or registered-name atom may legitimately
%% not yet exist locally on first contact. An empty node targets the local node.
brain_target(Node, Name) when is_binary(Node), is_binary(Name) ->
    {brain_target, node_atom(Node), erlang:binary_to_atom(Name, utf8)}.

%% "Connect" to the Brain: confirm the target node is reachable.
brain_connect({brain_target, NodeAtom, _NameAtom}) ->
    case NodeAtom =:= node() orelse net_adm:ping(NodeAtom) =:= pong of
        true -> {ok, nil};
        false -> {error, <<"brain node unreachable">>}
    end.

%% Deliver an already-ETF-encoded payload to the Brain's registered process.
%% Best-effort: `erlang:send/2` to `{Name, Node}` never blocks on the remote.
%% `badarg` (a malformed destination) is the only failure `send/2` raises; any
%% other error is unexpected and is left to propagate (rule STL-001).
brain_deliver({brain_target, NodeAtom, NameAtom}, Payload) when is_binary(Payload) ->
    try
        erlang:send({NameAtom, NodeAtom}, {pharos_alert, Payload}),
        {ok, nil}
    catch
        error:badarg -> {error, <<"invalid brain destination">>}
    end.

node_atom(<<>>) -> node();
node_atom(Node) when is_binary(Node) -> erlang:binary_to_atom(Node, utf8).

%% ===========================================================================
%% Sink delivery helpers.
%% ===========================================================================

%% ETF-encode any term to a binary for the Brain sink.
encode_etf(Term) ->
    erlang:term_to_binary(Term).

%% Fire-and-forget HTTP POST for the notification/webhook sink, using the
%% built-in `httpc` client (inets). Returns ok/error without blocking on a
%% slow endpoint beyond a short timeout. `inets`/`ssl` are started lazily.
webhook_post(Url, Body) when is_binary(Url), is_binary(Body) ->
    _ = application:ensure_all_started(inets),
    _ = application:ensure_all_started(ssl),
    Request = {erlang:binary_to_list(Url), [], "application/json", Body},
    case httpc:request(post, Request, [{timeout, 5000}], [{body_format, binary}]) of
        {ok, _Result} -> {ok, nil};
        {error, Reason} -> {error, format_reason(Reason)}
    end.

%% Encode a string->string map to a JSON object binary using OTP's built-in
%% `json` module (OTP 27+), which handles all escaping. A Gleam
%% `Dict(String, String)` is already an Erlang map of binaries.
json_encode(Map) when is_map(Map) ->
    iolist_to_binary(json:encode(Map)).

%% ===========================================================================
%% Internal
%% ===========================================================================

format_reason(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).

%% Read `MemTotal` and `MemAvailable` from `/proc/meminfo`, returning their
%% values in bytes. `/proc/meminfo` reports sizes in kB (1024-byte units), so
%% each value is scaled by 1024. Returns `error` if the file is unreadable or
%% either field is absent (e.g. on non-Linux hosts, or kernels predating
%% `MemAvailable`).
read_meminfo() ->
    case file:read_file("/proc/meminfo") of
        {ok, Bin} ->
            Lines = binary:split(Bin, <<"\n">>, [global]),
            Map = lists:foldl(fun parse_meminfo_line/2, #{}, Lines),
            case {maps:find(<<"MemTotal">>, Map), maps:find(<<"MemAvailable">>, Map)} of
                {{ok, TotalKb}, {ok, AvailKb}} ->
                    {ok, TotalKb * 1024, AvailKb * 1024};
                _ ->
                    error
            end;
        {error, _Reason} ->
            error
    end.

%% Fold one `/proc/meminfo` line (e.g. `<<"MemTotal:    16384256 kB">>`) into
%% the accumulator map as `Key => Kb`. Lines that don't parse are skipped.
parse_meminfo_line(Line, Acc) ->
    case binary:split(Line, <<":">>) of
        [Key, Rest] ->
            case parse_kb(Rest) of
                {ok, Kb} -> maps:put(Key, Kb, Acc);
                error -> Acc
            end;
        _Other ->
            Acc
    end.

%% Parse the leading integer (in kB) from a `/proc/meminfo` value such as
%% `<<"    16384256 kB">>`, ignoring surrounding whitespace and the unit suffix.
parse_kb(Rest) ->
    case string:to_integer(string:trim(Rest)) of
        {Kb, _Tail} when is_integer(Kb) -> {ok, Kb};
        _Other -> error
    end.

# Aerospike Spike

`aerospike_driver_spike/` is an architecture playground for the Aerospike
Elixir client. It is not the publishable library and it does not promise a
stable public API.

## Supported Commands

The spike currently proves one shared unary execution path across:

- `Aerospike.get/3` for full-record reads
- `Aerospike.put/4` for simple bin-map writes
- `Aerospike.exists/2` for header-only existence checks
- `Aerospike.touch/2` for metadata-only writes
- `Aerospike.delete/2` for record removal
- `Aerospike.operate/4` for a narrow write-scoped unary operate list
- `Aerospike.batch_get/4` for ordered homogeneous batch reads
- `Aerospike.stream!/3`, `Aerospike.all/3`, and `Aerospike.count/3` for
  scan fan-out through the shared scan/query setup path
- `Aerospike.scan_stream_node!/4`, `Aerospike.scan_all_node/4`, and
  `Aerospike.scan_count_node/4` for one named node
- `Aerospike.query_stream!/3`, `Aerospike.query_all/3`,
  `Aerospike.query_count/3`, and `Aerospike.query_page/3` for the same
  setup path with cursor-backed query pagination
- `Aerospike.query_stream_node!/4`, `Aerospike.query_all_node/4`,
  `Aerospike.query_count_node/4`, and `Aerospike.query_page_node/4`
  for one named node
- `Aerospike.create_index/4` and `Aerospike.drop_index/4` for the
  minimal secondary-index lifecycle needed to provision live query proof
- `Aerospike.query_aggregate/6` for aggregate streams on the shared
  query runtime
- `Aerospike.query_execute/4`, `Aerospike.query_execute_node/5`,
  `Aerospike.query_udf/6`, and `Aerospike.query_udf_node/7` for
  background query jobs returned as pollable task handles
- `Aerospike.transaction/2`, `Aerospike.transaction/3`,
  `Aerospike.commit/2`, `Aerospike.abort/2`, and
  `Aerospike.txn_status/2` for the first transaction lifecycle proof

These commands intentionally expose only a narrow option surface. The
supported `operate` surface includes the simple tuple forms
`{:write, bin, value}`, `{:read, bin}`, `{:add, bin, delta}`,
`{:append, bin, suffix}`, `{:prepend, bin, prefix}`, `:touch`, and
`:delete`, plus the list/map CDT helpers under `Aerospike.Op`,
`Aerospike.Op.List`, and `Aerospike.Op.Map`.
`Aerospike.batch_get/4` currently supports only `bins: :all` and
`:timeout`; grouped retries remain deferred until a failed grouped node
request can be regrouped honestly across nodes.
The scan and query surfaces are still intentionally narrow. Query now
accepts `Aerospike.Filter` values in `Query.where/2` instead of
pre-encoded predicate bytes, but the broader expression-filter and UDF
package-management surfaces remain deferred.
The public scan/query `Stream` helpers are lazy only at the outer
Enumerable boundary. The current runtime still drains each node stream
into memory before yielding that node's records downstream, so the spike
does not claim frame-by-frame cross-node backpressure or an explicit
scan/query cancellation API. `query_all/3`, `query_all_node/4`,
`query_page/3`, and `query_page_node/4` require `query.max_records`
because the query collector advances in repeated page-sized steps. For
multi-node queries that budget is distributed across active nodes, so a
single page is resumable but not guaranteed to contain exactly
`query.max_records` records. Query cursors resume partition progress
from the prior page; they are not snapshot tokens.

`%Aerospike.Txn{}` is also intentionally narrow. It is an immutable handle, not
an owned transaction process. Runtime tracking lives in the started cluster's
ETS tables and is initialized when `Aerospike.transaction/2` or
`Aerospike.transaction/3` enters its callback. A fresh handle on its own is
not an open transaction, and reusing one concurrently or against another
cluster is unsupported. `commit/2`, `abort/2`, and `txn_status/2` only apply
while that runtime tracking row still exists.

The current live proof set covers the shared unary read/write path without a
positive `:ttl` override. Real-server TTL handling is not yet part of the
claimed evidence.

For the supported `operate` subset, replies are returned as
`%Aerospike.Record{}` values with generation and TTL metadata from the
server reply. When the same bin appears more than once, the decoded value
is accumulated in that bin's value list.

For the current architecture boundaries and the Phase 1 evidence gate, start
with:

- [`../spike-docs/phase-1-contracts.md`](../spike-docs/phase-1-contracts.md)
- [`../spike-docs/testing-taxonomy.md`](../spike-docs/testing-taxonomy.md)

## Tests

Run tests from this directory.

Fast default coverage excludes live integration and enterprise suites:

```bash
mix test
```

Live integration is opt-in:

```bash
mix test --include integration test/integration/get_test.exs
```

Write-family live proof is also opt-in:

```bash
mix test --include integration test/integration/write_family_test.exs
```

Enterprise-only integration is opt-in on top of that:

```bash
mix test --include integration --include enterprise test/integration/tls_test.exs
```

The full taxonomy and the Phase 1 minimum evidence gate are documented in
`../spike-docs/testing-taxonomy.md`.

For the current unary read/write proof set, run:

```bash
mix test test/aerospike/write_family_test.exs test/aerospike/operate_test.exs test/aerospike/get_test.exs test/aerospike/get_retry_test.exs test/aerospike/unary_executor_test.exs test/aerospike/protocol/asm_msg_test.exs test/aerospike/protocol/response_test.exs --seed 0 --max-cases 1
mix test --include integration test/integration/get_test.exs test/integration/write_family_test.exs --seed 0
```

For the current batch proof set, run:

```bash
mix test test/aerospike/batch_router_test.exs test/aerospike/batch_executor_test.exs test/aerospike/batch_get_test.exs test/aerospike/protocol/batch_read_test.exs test/aerospike/transport/tcp_command_stream_test.exs --seed 0 --max-cases 1
mix test --include integration --include cluster test/integration/batch_get_test.exs --seed 0
```

For the current scan proof set, run:

```bash
mix test test/aerospike/scan_ops_test.exs --seed 0 --max-cases 1
mix test --include integration --include cluster test/integration/scan_test.exs --seed 0
```

For the current query proof set, run:

```bash
mix test test/aerospike/scan_ops_test.exs test/aerospike/filter_test.exs test/aerospike/protocol/filter_test.exs test/aerospike/execute_task_test.exs test/aerospike/txn_test.exs --seed 0
mix test --include integration test/integration/index_query_test.exs --seed 0
mix test --include integration --include enterprise test/integration/txn_test.exs --seed 0
```

## Package Metadata

If this spike is ever published separately, the package can be installed by
adding `aerospike_spike` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:aerospike_spike, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/aerospike_spike>.

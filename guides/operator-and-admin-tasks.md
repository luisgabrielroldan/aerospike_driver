# Operator And Admin Tasks

This guide covers the small set of operator-facing helpers exposed through
`Aerospike`. These commands are useful for readiness checks, node inspection,
info probes, index cleanup, truncation, and connection-pool warm-up. They are
not a replacement for the full Aerospike admin tools.

## Discover Active Nodes

`node_names/1` returns the active node names from the client's published
cluster view. Use those names when a command accepts `node: node_name` or when
you need to call `info_node/4`.

```elixir
{:ok, node_names} = Aerospike.node_names(:aerospike)

{:ok, nodes} = Aerospike.nodes(:aerospike)

Enum.each(nodes, fn %{name: name, host: host, port: port} ->
  {name, host, port}
end)
```

The view is populated by the client tend cycle. Check
`Aerospike.Cluster.ready?/1` before treating the view as routable.

## Info Commands

`info/3` sends one info command to one active node selected by the client.
`info_node/4` sends the same command to a specific active node. Unknown or
stale node names return an `:invalid_node` error instead of falling back to a
different node.

```elixir
{:ok, namespaces} = Aerospike.info(:aerospike, "namespaces")
{:ok, [node_name | _]} = Aerospike.node_names(:aerospike)
{:ok, statistics} = Aerospike.info_node(:aerospike, node_name, "statistics")
```

Both helpers use `pool_checkout_timeout:` for the connection-pool checkout.
The info command itself is a server command string; do not build it from
untrusted input.

```elixir
{:ok, response} =
  Aerospike.info_node(:aerospike, node_name, "sets/test/users",
    pool_checkout_timeout: 2_000
  )
```

## Pool Warm-Up

`warm_up/2` verifies that active node pools can hand out connections through
the normal checkout path. It is useful after startup when an application wants
to pay connection setup cost before accepting traffic.

```elixir
true = Aerospike.Cluster.ready?(:aerospike)
{:ok, warmed} = Aerospike.warm_up(:aerospike, count: 2)

Enum.each(warmed, fn {node_name, count} ->
  {node_name, count}
end)
```

`:count` defaults to the configured pool size and is capped at that size.
`:pool_checkout_timeout` controls each checkout attempt.

## Truncate Data

`truncate/3` removes all records in a namespace. `truncate/4` narrows the
operation to one set. Both helpers send one truncate info command and the
server distributes the truncate across the cluster.

```elixir
:ok = Aerospike.truncate(:aerospike, "test", "scratch")
```

Use `before:` to truncate only records older than a last-update cutoff:

```elixir
cutoff = DateTime.add(DateTime.utc_now(), -3600, :second)
:ok = Aerospike.truncate(:aerospike, "test", "scratch", before: cutoff)
```

Truncate is destructive. Prefer a dedicated set or a time cutoff for cleanup
jobs so production data is not removed accidentally.

## Drop Secondary Indexes

Drop an index by namespace and index name:

```elixir
:ok = Aerospike.drop_index(:aerospike, "test", "users_age_idx")
```

Index creation returns an `Aerospike.IndexTask` because the server builds the
index asynchronously. Dropping an index is exposed as a direct admin operation;
callers that need to observe completion should issue an appropriate follow-up
info probe for their deployment.

```elixir
{:ok, sindex} = Aerospike.info(:aerospike, "sindex/test")
```

## Cleanup Pattern

Tests, one-off jobs, and maintenance tasks commonly combine these helpers:

```elixir
with true <- Aerospike.Cluster.ready?(:aerospike),
     {:ok, [node_name | _]} <- Aerospike.node_names(:aerospike),
     {:ok, _stats} <- Aerospike.info_node(:aerospike, node_name, "statistics"),
     :ok <- Aerospike.truncate(:aerospike, "test", "scratch"),
     :ok <- Aerospike.drop_index(:aerospike, "test", "scratch_created_idx") do
  :ok
end
```

For full return shapes and validation rules, see `Aerospike.info/3`,
`Aerospike.info_node/4`, `Aerospike.nodes/1`, `Aerospike.node_names/1`,
`Aerospike.warm_up/2`, `Aerospike.truncate/3`, `Aerospike.truncate/4`, and
`Aerospike.drop_index/4`.

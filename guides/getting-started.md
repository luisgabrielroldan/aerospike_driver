# Getting Started

This guide starts one supervised Aerospike cluster connection, writes a record,
reads it back, and shuts the connection down.

## Start Aerospike

From the repository root, start the default Community Edition container:

```bash
docker compose up -d
```

Then run `iex -S mix` from the package directory.

## Start A Client

`Aerospike.start_link/1` validates the connection options before the cluster
runtime is started. The required options are the registered `:name`, transport
module, host list, and namespace list.

```elixir
{:ok, _pid} =
  Aerospike.start_link(
    name: :aerospike,
    transport: Aerospike.Transport.Tcp,
    hosts: ["127.0.0.1:3000"],
    namespaces: ["test"],
    pool_size: 2
  )
```

Use `Aerospike.Cluster.ready?/1` when a caller needs to observe that the
published cluster view is ready for routing:

```elixir
true = Aerospike.Cluster.ready?(:aerospike)
```

## Write And Read

Build keys with `Aerospike.key/3`. Bin maps may use string or atom keys, but
string bin names are the clearest match for server-side data.

```elixir
key = Aerospike.key("test", "users", "user:1")

{:ok, metadata} =
  Aerospike.put(:aerospike, key, %{
    "name" => "Ada",
    "visits" => 1
  })

{:ok, record} = Aerospike.get(:aerospike, key)

metadata.generation
record.bins["name"]
```

Read only selected bins by passing a list as the third argument:

```elixir
{:ok, record} = Aerospike.get(:aerospike, key, ["name"])
```

Use `get_header/3` when only metadata is needed:

```elixir
{:ok, header} = Aerospike.get_header(:aerospike, key)
```

## Close The Client

`close/2` currently resolves the running cluster by its registered atom name.

```elixir
:ok = Aerospike.close(:aerospike)
```

## Next Steps

- [Record Operations](record-operations.html) covers single-record helpers and
  operation lists.
- [Batch Operations](batch-operations.html) covers multi-key helpers and
  heterogeneous batch requests.
- [Operate, CDT, And Geo](operate-cdt-and-geo.html) covers server-side list,
  map, bit, HyperLogLog, expression, and geo patterns.
- [Queries And Scans](queries-and-scans.html) covers scan streams,
  secondary-index queries, paging, and node targeting.
- [Transactions](transactions.html) covers Enterprise multi-record transaction
  usage and current lifecycle boundaries.

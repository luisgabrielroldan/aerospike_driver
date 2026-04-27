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

For application code, define a Repo module that owns one Aerospike cluster
name:

```elixir
defmodule MyApp.Repo do
  use Aerospike.Repo, otp_app: :my_app
end
```

Configure the Repo under its module name:

```elixir
config :my_app, MyApp.Repo,
  transport: Aerospike.Transport.Tcp,
  hosts: ["127.0.0.1:3000"],
  namespaces: ["test"],
  pool_size: 2
```

Then add the Repo to your supervision tree:

```elixir
def start(_type, _args) do
  children = [
    MyApp.Repo
  ]

  Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
end
```

The Repo uses its module name as the default cluster name. To use a different
registered name, pass `:name` to `use Aerospike.Repo`.

Readiness is separate from supervision startup. Startup validates the options
and starts the processes; `Aerospike.Cluster.ready?/1` reports whether the
client has published a routable cluster view.

```elixir
true = Aerospike.Cluster.ready?(MyApp.Repo.conn())
```

`Aerospike.Repo` is a thin facade over the canonical `Aerospike` API. It does
not perform schema mapping, changeset validation, or object reflection.

For scripts, experiments, or custom lifecycle management, you can still start a
cluster directly:

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

## Write And Read

Build keys with `MyApp.Repo.key/3`. Bin maps may use string or atom keys, but
string bin names are the clearest match for server-side data.

```elixir
key = MyApp.Repo.key("test", "users", "user:1")

{:ok, metadata} =
  MyApp.Repo.put(key, %{
    "name" => "Ada",
    "visits" => 1
  })

{:ok, record} = MyApp.Repo.get(key)

metadata.generation
record.bins["name"]
```

Read only selected bins by passing a list as the second argument:

```elixir
{:ok, record} = MyApp.Repo.get(key, ["name"])
```

Use `get_header/1` when only metadata is needed:

```elixir
{:ok, header} = MyApp.Repo.get_header(key)
```

## Close The Client

The Repo closes its supervised cluster by the configured registered name.

```elixir
:ok = MyApp.Repo.close()
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

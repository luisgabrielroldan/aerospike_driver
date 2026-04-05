# CRUD Example

Basic Create, Read, Update, Delete examples for the Aerospike Elixir client.

## Prerequisites

An Aerospike server running on `localhost:3000`. From the repository root:

```bash
docker compose up -d
```

## Running

```bash
cd aerospike_driver/examples/crud_example
mix deps.get
iex -S mix
```

Then try the examples in IEx:

```elixir
iex> CrudExample.example_1()
iex> CrudExample.example_2()
iex> CrudExample.example_3()
```

## `example_1` — Ad-hoc connection

Starts a standalone client connection (not using the application supervisor), then
runs put, get, update, exists, and delete operations on a single record.

## `example_2` — Supervised connection

Uses the pooled connection `:aero` started by the application supervisor. Demonstrates
full reads, bin-filtered reads, header-only reads, update, and delete.

## `example_3` — Policies, TTL & CAS

Demonstrates advanced features:

- **TTL** — write a record with a 5-minute expiration
- **Touch** — refresh TTL without modifying data
- **Create-only** — write that fails if the record already exists
- **Optimistic concurrency (CAS)** — generation-based compare-and-swap
- **Integer keys** — using an integer as the user key

## Tests

With Aerospike running:

```bash
mix test
```

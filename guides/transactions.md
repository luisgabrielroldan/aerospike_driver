# Transactions

Transactions use `%Aerospike.Txn{}` handles plus runtime tracking in the
started cluster. Creating a handle does not start a transaction by itself;
tracking is initialized when `Aerospike.transaction/2` or
`Aerospike.transaction/3` enters the callback.

The current transaction surface is proved against the Enterprise profile and
covers transaction-aware single-record commands. Do not use scans or queries
inside a transaction callback.

## Callback Transactions

The callback owns the lifecycle. The client commits when the callback returns
successfully and aborts when it raises an `Aerospike.Error`, raises another
exception, throws, or exits. A returned `{:error, reason}` tuple is still an
ordinary callback return value, so raise when the transaction should abort.

```elixir
key1 = Aerospike.key("test", "orders", "order:1")
key2 = Aerospike.key("test", "orders", "order:2")

{:ok, :written} =
  Aerospike.transaction(:aerospike, fn txn ->
    {:ok, _} =
      Aerospike.put(:aerospike, key1, %{"status" => "open"}, txn: txn)

    {:ok, _} =
      Aerospike.put(:aerospike, key2, %{"status" => "open"}, txn: txn)

    :written
  end)
```

The cluster argument must currently be the registered atom name because
transaction tracking and supervisor lookups resolve from that name.

## Explicit Handles

Create a handle with options when the caller needs to choose a transaction
timeout.

```elixir
txn = Aerospike.Txn.new(timeout: 5_000)

{:ok, result} =
  Aerospike.transaction(:aerospike, txn, fn tx ->
    {:ok, :open} = Aerospike.txn_status(:aerospike, tx)
    {:ok, _} = Aerospike.add(:aerospike, key1, %{"attempts" => 1}, txn: tx)
    :updated
  end)

result
```

A transaction handle is valid only against the cluster that initialized its
tracking row. Reusing one handle concurrently or against another cluster is
unsupported.

## Manual Lifecycle Helpers

`commit/2`, `abort/2`, and `txn_status/2` operate on an already-open tracked
transaction. They do not initialize a fresh `%Aerospike.Txn{}` by themselves.

```elixir
{:ok, :checked} =
  Aerospike.transaction(:aerospike, fn tx ->
    {:ok, :open} = Aerospike.txn_status(:aerospike, tx)
    :checked
  end)
```

Most callers should let `transaction/2` or `transaction/3` commit or abort the
callback. Calling manual lifecycle helpers requires an initialized tracking
row and should stay sequential.

After commit or abort, the runtime tracking row is cleaned up. A later
`txn_status/2` call can therefore return an error instead of a terminal state.

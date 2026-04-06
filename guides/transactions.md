# Transactions (MRT)

Multi-Record Transactions (MRT) let you group several reads and writes across multiple keys into a single atomic unit. Either all writes are committed or none are — other clients never see a partially applied transaction.

> #### Enterprise Edition Required {: .warning}
>
> MRT requires **Aerospike Enterprise Edition** with server version 8.0 or later. Connecting to
> Community Edition and calling transaction APIs will return an error.

## Creating a Transaction

A transaction starts as a plain struct — no I/O, no server contact yet:

```elixir
alias Aerospike.Txn

txn = Txn.new()
# %Aerospike.Txn{id: -3489123456789012345, timeout: 0}
```

`Txn.new/0` generates a random signed 64-bit ID. Pass `timeout:` (in milliseconds) to set a
server-side MRT record expiry. If the client crashes before committing, the server releases
write locks after this window:

```elixir
txn = Txn.new(timeout: 30_000)   # 30-second server-side timeout
```

## Using `transaction/2` — the Recommended Approach

`Aerospike.transaction/2` is the highest-level API. It creates a transaction, runs your
callback, and commits or aborts automatically:

```elixir
{:ok, _} =
  Aerospike.transaction(:aero, fn txn ->
    key1 = Aerospike.key("inventory", "items", "widget-A")
    key2 = Aerospike.key("accounts", "users", "user-42")

    {:ok, item} = Aerospike.get(:aero, key1, txn: txn)

    if item.bins["stock"] < 1 do
      raise Aerospike.Error.from_result_code(:parameter_error, message: "out of stock")
    end

    :ok = Aerospike.put(:aero, key1, %{"stock" => item.bins["stock"] - 1}, txn: txn)
    :ok = Aerospike.put(:aero, key2, %{"last_order" => "widget-A"}, txn: txn)
  end)
```

On any `Aerospike.Error` raised inside the callback, the transaction aborts and
`transaction/2` returns `{:error, e}`. On any other exception, the transaction aborts and
the exception is re-raised — server-side write locks are always released immediately.

### Setting a Timeout

```elixir
{:ok, _} =
  Aerospike.transaction(:aero, [timeout: 10_000], fn txn ->
    Aerospike.put(:aero, key, %{"x" => 1}, txn: txn)
  end)
```

### Returning a Value

The return value of the callback is wrapped in `{:ok, value}`:

```elixir
{:ok, balance} =
  Aerospike.transaction(:aero, fn txn ->
    {:ok, rec} = Aerospike.get(:aero, account_key, txn: txn)
    rec.bins["balance"]
  end)
```

## Using `txn:` on CRUD Operations

Pass `txn: txn` to any CRUD operation to enlist it in the transaction. All operations in one
transaction must touch the **same namespace**:

```elixir
txn = Txn.new()

# Reads — tracked for version verification at commit time
{:ok, rec} = Aerospike.get(:aero, key1, txn: txn)

# Writes — registered with the server-side monitor record
:ok = Aerospike.put(:aero, key2, %{"score" => 99}, txn: txn)
:ok = Aerospike.delete(:aero, key3, txn: txn)
```

Supported operations: `get`, `exists`, `put`, `delete`, `touch`, `append`, `prepend`, `add`,
`operate`, `batch_get`, `batch_exists`, `batch_operate`.

## Manual Commit and Abort

When you need full control over the transaction lifecycle, call `commit/2` and `abort/2`
directly. You must also initialize ETS tracking explicitly via `Aerospike.TxnOps.init_tracking/2`:

```elixir
alias Aerospike.{Txn, TxnOps}

txn = Txn.new()
TxnOps.init_tracking(:aero, txn)

:ok = Aerospike.put(:aero, key1, %{"a" => 1}, txn: txn)
:ok = Aerospike.put(:aero, key2, %{"b" => 2}, txn: txn)

case Aerospike.commit(:aero, txn) do
  {:ok, :committed} ->
    IO.puts("committed")

  {:error, %Aerospike.Error{in_doubt: true} = err} ->
    # Mark-roll-forward timed out — may or may not have committed.
    # Safe to retry commit/2; the server returns :mrt_committed if already done.
    IO.puts("in-doubt, retrying: #{err.message}")
    Aerospike.commit(:aero, txn)

  {:error, err} ->
    Aerospike.abort(:aero, txn)
    IO.puts("commit failed, rolled back: #{err.message}")
end
```

### Aborting

```elixir
{:ok, :aborted} = Aerospike.abort(:aero, txn)
```

`abort/2` rolls back all tracked writes and deletes the server-side monitor record.
Roll-back is best-effort: if some writes fail (e.g., a network partition), the server
releases locks automatically when the MRT timeout expires.

## Commit Protocol

Understanding the commit phases helps diagnose errors:

| Phase | What happens |
|-------|-------------|
| **Verify** | For each read tracked in the transaction, the server checks that the record version has not changed since the read. If any version mismatches, commit fails. |
| **Mark roll-forward** | The server-side monitor record is flagged for roll-forward. After this point, the transaction is durably committed from the server's perspective. |
| **Roll forward** | Each write is finalized (locks released, record versions updated). |
| **Close** | The monitor record is deleted. |

## Error Handling

### Verify Failure

A read was invalidated by a concurrent external write between your read and commit:

```elixir
case Aerospike.commit(:aero, txn) do
  {:ok, :committed} ->
    :ok

  {:error, %Aerospike.Error{code: :txn_verify_failed}} ->
    # Transaction was auto-rolled-back.
    # Retry the whole transaction from scratch.
    :retry
end
```

### In-Doubt Commit

The mark-roll-forward message timed out before the server responded. The transaction
**may or may not** have committed server-side:

```elixir
case Aerospike.commit(:aero, txn) do
  {:ok, :committed} ->
    :ok

  {:error, %Aerospike.Error{in_doubt: true}} ->
    # Retry commit — the server returns :mrt_committed if it already committed,
    # which is accepted as success. Do NOT call abort here.
    Aerospike.commit(:aero, txn)

  {:error, err} ->
    # Definitive failure — safe to abort and retry from scratch.
    Aerospike.abort(:aero, txn)
    {:error, err}
end
```

### Server Aborted

The server aborted the transaction (MRT timeout expired while it was open):

```elixir
case Aerospike.commit(:aero, txn) do
  {:error, %Aerospike.Error{code: :mrt_aborted}} ->
    # The server timed out the transaction. Retry from scratch.
    :retry
end
```

> #### `transaction/2` Handles This For You {: .tip}
>
> `Aerospike.transaction/2` calls `best_effort_abort` on any commit failure, cleans up
> server-side state, and returns `{:error, e}`. For most use cases you do not need to
> handle these error cases manually.

## Constraints

- **Same namespace**: all keys in one transaction must belong to the same namespace.
- **No scans or queries**: `scan/3` and `query/3` do not accept a `txn:` option.
- **Writes are durable deletes**: `delete/3` in a transaction uses durable delete semantics
  automatically.
- **Concurrent use is undefined**: passing the same `%Txn{}` to operations from multiple
  processes concurrently is not supported. The ETS tracking state is not protected by a lock.
- **One transaction at a time per handle**: do not nest `transaction/2` calls with the same
  `%Txn{}` struct.

## Checking Transaction State

Inside a `transaction/2` callback you can inspect the current state:

```elixir
Aerospike.transaction(:aero, fn txn ->
  {:ok, :open} = Aerospike.txn_status(:aero, txn)
  Aerospike.put(:aero, key, %{"x" => 1}, txn: txn)
end)
```

After `commit/2` or `abort/2` returns, the ETS tracking entry is deleted, so
`txn_status/2` returns `{:error, %Aerospike.Error{}}` rather than `{:ok, :committed}`.
The state is only observable during the commit/abort execution window.

## Next Steps

- `Aerospike.Txn` — transaction handle struct reference
- `Aerospike` — facade functions: `commit/2`, `abort/2`, `transaction/2`, `transaction/3`
- [Batch Operations](batch-operations.md) — combining batch with transactions

# Policies

Policies control how commands are executed: timeouts, record-existence behavior,
generation checks (CAS), replica routing, and other per-command options.

This client uses trailing keyword options (for example `Aerospike.put(conn, key, bins, opts)`)
instead of dedicated `WritePolicy`/`ReadPolicy` structs. You can set defaults once at
`start_link/1` and override them per call.

## How Policy Resolution Works

For each command, the client merges policy options in this order:

1. Connection defaults from `start_link(defaults: [...])`
2. Per-call options passed to the command

Per-call values always win on key conflicts.

```elixir
{:ok, _pid} =
  Aerospike.start_link(
    name: :aero,
    hosts: ["127.0.0.1:3000"],
    defaults: [
      write: [timeout: 2_000, ttl: 3_600],
      read: [timeout: 1_500]
    ]
  )

# timeout is overridden to 500 ms for this call only.
:ok = Aerospike.put(:aero, key, %{"name" => "Ada"}, timeout: 500)
```

## Write Policies

Used by `put/4`, `add/4`, `append/4`, `prepend/4`.

| Option | Type | Meaning |
| --- | --- | --- |
| `:ttl` | non-negative integer | Record TTL in seconds |
| `:timeout` | non-negative integer | Command timeout in milliseconds |
| `:generation` | non-negative integer | Expected record generation |
| `:gen_policy` | `:none` \| `:expect_gen_equal` \| `:expect_gen_gt` | Generation check strategy |
| `:exists` | `:create_only` \| `:update_only` \| `:replace_only` \| `:create_or_replace` | Record existence behavior |
| `:send_key` | boolean | Store the user key on the server |
| `:durable_delete` | boolean | Tombstone-based delete behavior where applicable |
| `:filter` | `%Aerospike.Exp{}` | Server-side expression filter |
| `:pool_checkout_timeout` | non-negative integer | Pool checkout timeout in milliseconds |
| `:replica` | `:master` \| `:sequence` \| `:any` \| non-negative integer | Replica routing |
| `:txn` | `%Aerospike.Txn{}` | Transaction handle |

### Existence Policies

```elixir
# Create only if missing.
:ok = Aerospike.put(:aero, key, bins, exists: :create_only)

# Update only if present.
:ok = Aerospike.put(:aero, key, bins, exists: :update_only)

# Replace existing record bins only (no merge).
:ok = Aerospike.put(:aero, key, bins, exists: :replace_only)

# Create if absent, replace if present.
:ok = Aerospike.put(:aero, key, bins, exists: :create_or_replace)
```

### Generation Policy (CAS)

When `:generation` is provided and `:gen_policy` is omitted, the client defaults to
`gen_policy: :expect_gen_equal`.

```elixir
{:ok, rec} = Aerospike.get(:aero, key)

case Aerospike.put(:aero, key, %{"counter" => 1}, generation: rec.generation) do
  :ok ->
    :updated

  {:error, %Aerospike.Error{code: :generation_error}} ->
    :conflict
end
```

## Read Policies

Used by `get/3`.

| Option | Type | Meaning |
| --- | --- | --- |
| `:timeout` | non-negative integer | Command timeout in milliseconds |
| `:bins` | list of strings/atoms | Read only selected bins |
| `:header_only` | boolean | Read metadata (generation/ttl) without bins |
| `:read_touch_ttl_percent` | non-negative integer | Read-touch TTL percent |
| `:filter` | `%Aerospike.Exp{}` | Server-side expression filter |
| `:pool_checkout_timeout` | non-negative integer | Pool checkout timeout in milliseconds |
| `:replica` | `:master` \| `:sequence` \| `:any` \| non-negative integer | Replica routing |
| `:txn` | `%Aerospike.Txn{}` | Transaction handle |

If you pass `:bins`, it must be a non-empty list. Omit `:bins` to read all bins.

## Delete, Exists, and Touch Policies

### `delete/3`

`delete/3` accepts: `:timeout`, `:durable_delete`, `:filter`, `:pool_checkout_timeout`, `:replica`, `:txn`.

### `exists/3`

`exists/3` accepts: `:timeout`, `:filter`, `:pool_checkout_timeout`, `:replica`, `:txn`.

### `touch/3`

`touch/3` accepts: `:ttl`, `:timeout`, `:filter`, `:pool_checkout_timeout`, `:replica`, `:txn`.

## Operate Policies

`operate/4` combines read and write semantics. Supported options:

- `:ttl`
- `:timeout`
- `:generation`
- `:gen_policy`
- `:exists`
- `:send_key`
- `:durable_delete`
- `:respond_per_each_op`
- `:pool_checkout_timeout`
- `:replica`
- `:txn`

`respond_per_each_op` is specific to operate responses when multiple operations are present.

## Batch, Scan, Query, and Info Policies

### Batch (`batch_get/3`, `batch_exists/3`, `batch_operate/3`)

Supported options:

- `:timeout`
- `:pool_checkout_timeout`
- `:replica`
- `:respond_all_keys`
- `:filter`
- `:txn`

For `batch_get/3`, read options such as `:bins` and `:header_only` are also supported.

### Scan and Query (`stream!/3`, `all/3`, `count/3`, `page/3`)

Supported options:

- `:timeout`
- `:pool_checkout_timeout`
- `:replica`

### Info/Admin (`info/3`, `info_node/4`, truncate/index helpers)

Supported options:

- `:timeout`
- `:pool_checkout_timeout`

## Defaults at Startup

Set per-command defaults when starting the client:

```elixir
{:ok, _pid} =
  Aerospike.start_link(
    name: :aero,
    hosts: ["127.0.0.1:3000"],
    defaults: [
      write: [timeout: 2_000, ttl: 3_600],
      read: [timeout: 1_500],
      delete: [timeout: 1_500, durable_delete: true],
      operate: [timeout: 2_000],
      batch: [timeout: 4_000, respond_all_keys: true],
      scan: [timeout: 30_000],
      query: [timeout: 30_000]
    ]
  )
```

If a command has no configured defaults, it runs with only explicit per-call options.

## Validation and Error Shape

All policy options are validated. Invalid options return:

```elixir
{:error, %Aerospike.Error{code: :parameter_error}}
```

Use this to fail fast on typos and invalid values (for example unsupported atoms for `:exists`).

## Practical Recommendations

- Set conservative `:timeout` defaults at connection start.
- Use `:generation` (+ optional `:gen_policy`) for concurrent updates.
- Use `exists: :create_only` for idempotent create flows.
- Use `header_only: true` for metadata checks when bins are unnecessary.
- Keep policy overrides close to the callsite when behavior is operation-specific.

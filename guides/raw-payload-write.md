# Raw payload writes (advanced)

[`Aerospike.put_payload/4`](Aerospike.html#put_payload/4) and [`Aerospike.Repo.put_payload/3`](Aerospike.Repo.html#put_payload/3) send a **caller-built** Aerospike wire message for a **single** record key. The client routes the bytes to the node that owns the write partition for that key, reads the standard response header, and maps the result code the same way as normal writes.

> #### Advanced / unsafe escape hatch {: .warning}
>
> Prefer [`put/4`](Aerospike.html#put/4), [`delete/3`](Aerospike.html#delete/3), and [`operate/4`](Aerospike.html#operate/4) for application code. Misuse of raw payloads can cause server errors or subtle data corruption that the library cannot detect.

## Caller responsibilities

- **Full message buffer** — `payload` must already be a complete, correctly framed write or delete command for one record, in the same shape the server expects from a normal client write. The library does not wrap, patch, or inspect the body.
- **No client-side validation** — The only check is `is_binary(payload)`. There is no max-size guard, non-empty requirement, or structural validation; the server is the final authority.
- **Bin encoding bypass** — This path skips the usual bin maps, CDT builders, and record helpers. Anything in the payload is exactly what goes on the wire.

## Routing and write policies

Per-call options are merged with connection defaults and validated like other writes (`Policy.validate_write/1`). Values such as `:timeout`, `:replica`, and `:pool_checkout_timeout` affect **routing and I/O**. Flags that matter to the server (TTL, generation, write mode, etc.) must already be encoded **inside** the payload you build.

## Multi-record transactions (MRT)

Unlike [`put/4`](Aerospike.html#put/4), **`put_payload` does not register the key with the transaction monitor**, even if you pass `:txn` in options. This matches the Go client's `PutPayload`, which skips `txnMonitor.addKey` because the payload is opaque and may already carry MRT fields.

If you need MRT semantics with a raw payload, you must embed the correct transaction fields in the wire yourself. For the supported high-level MRT flow, see [Transactions](transactions.md).

## Intended use cases

- Replaying or forwarding captures of legitimate wire traffic
- Proxies, shims, or tooling that already produce Aerospike messages
- Experiments where you deliberately control every byte of the write

## Out of scope for this API

The client does not offer raw payload **batch**, **scan**, or **query** commands in this form, and does not ship XDR-specific payload helpers here. Those remain separate surfaces (batch APIs, scan/query builders, XDR filter management).

## See also

- [`put_payload/4`](Aerospike.html#put_payload/4) / [`put_payload!/4`](Aerospike.html#put_payload!/4) — full option list and return type
- [Policies](policies.md) — write policy keywords shared with `put/4`

# Aerospike Spike

`aerospike_driver_spike/` is an architecture playground for the Aerospike
Elixir client. It is not the publishable library and it does not promise a
stable public API.

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

Enterprise-only integration is opt-in on top of that:

```bash
mix test --include integration --include enterprise test/integration/tls_test.exs
```

The full taxonomy and the Phase 1 minimum evidence gate are documented in
`../spike-docs/testing-taxonomy.md`.

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

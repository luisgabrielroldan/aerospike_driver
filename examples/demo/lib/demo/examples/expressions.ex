defmodule Demo.Examples.Expressions do
  @moduledoc """
  Demonstrates server-side filter expressions using `Aerospike.Exp`.

  Shows four expression use-cases:

  1. **Filter on get** — `filter:` expr on a single-record read filters the
     record out server-side instead of returning it.
  2. **Filter on scan** — `Scan.filter/2` restricts which records are streamed
     back from a full set scan.
  3. **Op.Exp.read** — compute a boolean predicate server-side and surface the
     result in the response record's bins without a separate read round-trip.
  4. **Op.Exp.write** — evaluate an expression server-side and persist the
     result to a bin in the same atomic operate command.
  """

  require Logger

  alias Aerospike.Exp
  alias Aerospike.Op
  alias Aerospike.Scan

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_expr"

  def run do
    write_records()
    filter_on_get()
    filter_on_scan()
    exp_read_op()
    exp_write_op()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing records: alice(age=25), bob(age=40), carol(age=15), dave(age=35)")

    records = [
      {"alice", %{"age" => 25, "score" => 80}},
      {"bob", %{"age" => 40, "score" => 90}},
      {"carol", %{"age" => 15, "score" => 70}},
      {"dave", %{"age" => 35, "score" => 85}}
    ]

    for {name, bins} <- records do
      key = Aerospike.key(@namespace, @set, name)
      :ok = @repo.put!(key, bins)
    end
  end

  defp filter_on_get do
    Logger.info("  Filter expression on get (age > 20)...")

    expr = Exp.gt(Exp.int_bin("age"), Exp.val(20))

    alice_key = Aerospike.key(@namespace, @set, "alice")
    {:ok, record} = @repo.get(alice_key, filter: expr)

    unless record.bins["age"] == 25 do
      raise "Expected alice age=25, got #{record.bins["age"]}"
    end

    Logger.info("    alice (age=25) matched — returned normally")

    carol_key = Aerospike.key(@namespace, @set, "carol")
    {:error, error} = @repo.get(carol_key, filter: expr)

    unless error.code == :filtered_out do
      raise "Expected :filtered_out for carol, got #{error.code}"
    end

    Logger.info("    carol (age=15) did not match — filtered_out returned")
  end

  defp filter_on_scan do
    Logger.info("  Filter expression on scan (age >= 35)...")

    scan =
      Scan.new(@namespace, @set)
      |> Scan.filter(Exp.gte(Exp.int_bin("age"), Exp.val(35)))
      |> Scan.max_records(20)

    {:ok, records} = @repo.all(scan)

    for r <- records do
      age = r.bins["age"]
      unless age >= 35, do: raise("Unexpected record: age=#{age} is below filter threshold")
    end

    Logger.info(
      "    #{length(records)} records returned with age>=35 (expected: bob=40, dave=35)"
    )
  end

  defp exp_read_op do
    Logger.info("  Op.Exp.read — compute is_senior (age >= 40) server-side...")

    key = Aerospike.key(@namespace, @set, "bob")

    record =
      @repo.operate!(key, [
        Op.Exp.read("is_senior", Exp.gte(Exp.int_bin("age"), Exp.val(40)))
      ])

    unless record && record.bins["is_senior"] == true do
      raise "Expected is_senior=true for bob (age=40), got #{inspect(record && record.bins["is_senior"])}"
    end

    Logger.info(
      "    bob is_senior=#{record.bins["is_senior"]} computed server-side (age=40 >= 40)"
    )
  end

  defp exp_write_op do
    Logger.info("  Op.Exp.write — persist expression result to a bin...")

    key = Aerospike.key(@namespace, @set, "alice")

    # Write a constant expression result to a new bin
    @repo.operate!(key, [
      Op.Exp.write("computed", Exp.val(42))
    ])

    {:ok, record} = @repo.get(key)

    unless record.bins["computed"] == 42 do
      raise "Expected computed=42, got #{inspect(record.bins["computed"])}"
    end

    Logger.info("    alice computed=#{record.bins["computed"]} written by Op.Exp.write")
  end

  defp cleanup do
    for name <- ["alice", "bob", "carol", "dave"] do
      key = Aerospike.key(@namespace, @set, name)
      @repo.delete(key)
    end
  end
end

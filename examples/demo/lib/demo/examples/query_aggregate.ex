defmodule Demo.Examples.QueryAggregate do
  @moduledoc """
  Demonstrates secondary-index queries with client-side aggregation.

  Creates a numeric index on an "age" bin, writes sample records, then runs
  a query with a range filter and computes aggregate statistics (count, sum,
  average, min, max) on the client side using `Enum` reductions.

  Server-side aggregation via Lua stream UDFs is an advanced Aerospike feature.
  This example uses the idiomatic Elixir approach: let the server filter via
  secondary index, then aggregate results in the client using standard Elixir
  stream/enum operations.
  """

  require Logger

  alias Aerospike.Filter
  alias Aerospike.Query

  @conn :aero
  @namespace "test"
  @set "demo_qagg"
  @index_name "demo_qagg_age_idx"
  @size 20

  @people [
    {"alice", 25, "engineering"},
    {"bob", 40, "marketing"},
    {"carol", 15, "intern"},
    {"dave", 35, "engineering"},
    {"eve", 28, "marketing"},
    {"frank", 45, "engineering"},
    {"grace", 32, "engineering"},
    {"henry", 22, "intern"},
    {"iris", 38, "marketing"},
    {"jack", 50, "engineering"},
    {"kate", 27, "marketing"},
    {"leo", 33, "engineering"},
    {"mona", 41, "marketing"},
    {"nick", 19, "intern"},
    {"olivia", 36, "engineering"},
    {"paul", 29, "marketing"},
    {"quinn", 44, "engineering"},
    {"rachel", 31, "engineering"},
    {"sam", 26, "intern"},
    {"tina", 48, "marketing"}
  ]

  def run do
    write_records()
    create_index()
    query_range_aggregate()
    query_with_expression_filter()
    stream_aggregate()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing #{@size} employee records...")

    for {name, age, dept} <- @people do
      key = Aerospike.key(@namespace, @set, name)
      bins = %{"name" => name, "age" => age, "dept" => dept, "salary" => age * 1_000}
      :ok = Aerospike.put!(@conn, key, bins)
    end
  end

  defp create_index do
    Logger.info("  Creating numeric index on 'age'...")

    {:ok, task} =
      Aerospike.create_index(@conn, @namespace, @set,
        bin: "age",
        name: @index_name,
        type: :numeric
      )

    :ok = Aerospike.IndexTask.wait(task, timeout: 15_000)
    Logger.info("  Index ready.")
  end

  defp query_range_aggregate do
    Logger.info("  Query: age 25..40, then aggregate client-side...")

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.range("age", 25, 40))
      |> Query.max_records(50)

    {:ok, records} = Aerospike.all(@conn, query)

    ages = Enum.map(records, fn r -> r.bins["age"] end)
    count = length(ages)
    sum = Enum.sum(ages)
    avg = if count > 0, do: sum / count, else: 0
    min_age = Enum.min(ages, fn -> 0 end)
    max_age = Enum.max(ages, fn -> 0 end)

    Logger.info("    Records in range: #{count}")

    Logger.info(
      "    Age stats: sum=#{sum}, avg=#{Float.round(avg, 1)}, min=#{min_age}, max=#{max_age}"
    )

    for age <- ages do
      unless age >= 25 and age <= 40 do
        raise "Record with age=#{age} outside range 25..40"
      end
    end

    unless count > 0 do
      raise "Expected at least one record in range 25..40"
    end
  end

  defp query_with_expression_filter do
    Logger.info("  Query: age 20..50 + expression filter dept='engineering'...")

    expr = Aerospike.Exp.eq(Aerospike.Exp.str_bin("dept"), Aerospike.Exp.val("engineering"))

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.range("age", 20, 50))
      |> Query.filter(expr)
      |> Query.max_records(50)

    case Aerospike.all(@conn, query) do
      {:ok, records} ->
        for r <- records do
          unless r.bins["dept"] == "engineering" do
            raise "Expected dept=engineering, got #{r.bins["dept"]}"
          end
        end

        salaries = Enum.map(records, fn r -> r.bins["salary"] end)
        total_salary = Enum.sum(salaries)

        Logger.info(
          "    Engineering employees (age 20-50): #{length(records)}, total salary: #{total_salary}"
        )

      {:error, %Aerospike.Error{} = e} ->
        Logger.warning(
          "    Query with expression filter failed (known SC-mode limitation): #{e.message}"
        )
    end
  end

  defp stream_aggregate do
    Logger.info("  Stream aggregate: sum all salaries via stream!/2 pipeline...")

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.range("age", 0, 100))

    try do
      total =
        Aerospike.stream!(@conn, query)
        |> Stream.map(fn r -> r.bins["salary"] end)
        |> Stream.reject(&is_nil/1)
        |> Enum.sum()

      if total > 0 do
        Logger.info("    Total salary across all employees: #{total}")
      else
        Logger.warning(
          "    Stream returned zero salary total (SC-mode may limit query stream results)"
        )
      end
    rescue
      e in Aerospike.Error ->
        Logger.warning("    Stream aggregate failed (known SC-mode limitation): #{e.message}")
    end
  end

  defp cleanup do
    Aerospike.drop_index(@conn, @namespace, @index_name)

    for {name, _, _} <- @people do
      Aerospike.delete(@conn, Aerospike.key(@namespace, @set, name))
    end
  end
end

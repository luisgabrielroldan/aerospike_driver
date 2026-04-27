defmodule Demo.Examples.SecondaryIndex do
  @moduledoc """
  Demonstrates secondary-index lifecycle: create, poll status, query, and drop.

  Uses `Aerospike.create_index/4`, `Aerospike.IndexTask.wait/2`,
  `Aerospike.IndexTask.status/1`, `Aerospike.drop_index/3`, and
  `Aerospike.Filter.equal/2`.
  """

  require Logger

  alias Aerospike.Filter
  alias Aerospike.Query

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_sindex"
  @string_idx "demo_sindex_city"
  @numeric_idx "demo_sindex_score"
  @size 10

  @cities ~w(portland seattle denver austin portland seattle denver austin portland seattle)

  def run do
    write_records()
    create_and_poll_index()
    query_with_equal()
    query_with_range()
    drop_indexes()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing #{@size} records...")

    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "idx_#{i}")
      city = Enum.at(@cities, i - 1)
      bins = %{"name" => "user_#{i}", "city" => city, "score" => i * 10}
      :ok = @repo.put!(key, bins)
    end
  end

  defp create_and_poll_index do
    Logger.info("  Creating string index on 'city'...")

    {:ok, task} =
      @repo.create_index(@namespace, @set,
        bin: "city",
        name: @string_idx,
        type: :string
      )

    status = Aerospike.IndexTask.status(task)
    Logger.info("  Index task status before wait: #{inspect(status)}")

    :ok = Aerospike.IndexTask.wait(task, timeout: 15_000)
    Logger.info("  String index '#{@string_idx}' ready.")

    Logger.info("  Creating numeric index on 'score'...")

    {:ok, task2} =
      @repo.create_index(@namespace, @set,
        bin: "score",
        name: @numeric_idx,
        type: :numeric
      )

    :ok = Aerospike.IndexTask.wait(task2, timeout: 15_000)
    Logger.info("  Numeric index '#{@numeric_idx}' ready.")
  end

  defp query_with_equal do
    Logger.info("  Query: city = 'portland' (Filter.equal)...")

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.equal("city", "portland"))
      |> Query.max_records(20)

    {:ok, records} = @repo.all(query)

    names = Enum.map(records, fn r -> r.bins["name"] end)
    Logger.info("    Found #{length(records)} portland records: #{Enum.join(names, ", ")}")

    for r <- records do
      unless r.bins["city"] == "portland" do
        raise "Expected city=portland, got #{r.bins["city"]}"
      end
    end

    if records == [] do
      raise "Expected at least one portland record"
    end
  end

  defp query_with_range do
    Logger.info("  Query: score 50..80 (Filter.range)...")

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.range("score", 50, 80))
      |> Query.max_records(50)

    {:ok, records} = @repo.all(query)

    for r <- records do
      score = r.bins["score"]

      unless score >= 50 and score <= 80 do
        raise "Score #{score} outside 50..80"
      end
    end

    Logger.info("    Found #{length(records)} records with score 50..80")
  end

  defp drop_indexes do
    Logger.info("  Dropping indexes...")
    @repo.drop_index(@namespace, @string_idx)
    Logger.info("  Dropped '#{@string_idx}'.")
    @repo.drop_index(@namespace, @numeric_idx)
    Logger.info("  Dropped '#{@numeric_idx}'.")
  end

  defp cleanup do
    for i <- 1..@size do
      @repo.delete(Aerospike.key(@namespace, @set, "idx_#{i}"))
    end
  end
end

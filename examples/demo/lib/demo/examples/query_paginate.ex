defmodule Demo.Examples.QueryPaginate do
  @moduledoc """
  Demonstrates paginated queries using `Aerospike.page/3` with a `Query`.

  Creates a secondary index, writes records, then paginates through matching
  results using cursor-based `page/3` — the same API used for scan pagination
  but driven by a secondary-index query.
  """

  require Logger

  alias Aerospike.Filter
  alias Aerospike.Page
  alias Aerospike.Query

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_qpage"
  @index_name "demo_qpage_qval_idx"
  @bin "qval"
  @size 20
  @page_size 5

  def run do
    cleanup_stale()
    write_records()
    create_index()
    paginate_query()
    stream_query()
    cleanup()
  end

  defp cleanup_stale do
    @repo.drop_index(@namespace, @index_name)

    for i <- 1..@size do
      @repo.delete(Aerospike.key(@namespace, @set, "qp_#{i}"))
    end
  end

  defp write_records do
    Logger.info("  Writing #{@size} records...")

    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "qp_#{i}")
      bins = %{"name" => "item_#{i}", @bin => i * 5}
      :ok = @repo.put!(key, bins)
    end
  end

  defp create_index do
    Logger.info("  Creating numeric index on '#{@bin}'...")

    {:ok, task} =
      @repo.create_index(@namespace, @set,
        bin: @bin,
        name: @index_name,
        type: :numeric
      )

    :ok = Aerospike.IndexTask.wait(task, timeout: 15_000)
    Process.sleep(500)
    Logger.info("  Index ready.")
  end

  defp paginate_query do
    Logger.info("  Paginating query (#{@bin} 10..80, page_size=#{@page_size})...")

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.range(@bin, 10, 80))
      |> Query.max_records(@page_size)

    {:ok, %Page{records: records, cursor: cursor, done?: done?}} = @repo.page(query)
    log_page(1, records, length(records), done?)

    if cursor && !done? do
      {:ok, %Page{records: records2, done?: done2?}} = @repo.page(query, cursor: cursor)
      log_page(2, records2, length(records) + length(records2), done2?)
    end
  end

  defp log_page(page_num, records, total, done?) do
    count = length(records)

    Logger.info("  Page #{page_num}: #{count} records (total so far: #{total}, done?: #{done?})")

    for r <- records do
      val = r.bins[@bin]

      unless val >= 10 and val <= 80 do
        raise "#{@bin} #{val} outside query range 10..80"
      end
    end

    if total == 0 do
      raise "Expected at least one record in range"
    end
  end

  defp stream_query do
    Logger.info("  Streaming query results with stream!/2 (#{@bin} 50..100)...")

    query =
      Query.new(@namespace, @set)
      |> Query.where(Filter.range(@bin, 50, 100))
      |> Query.max_records(50)

    try do
      results =
        @repo.stream!(query)
        |> Stream.map(fn r -> {r.bins["name"], r.bins[@bin]} end)
        |> Enum.to_list()

      Logger.info("    Streamed #{length(results)} records with #{@bin} 50..100")

      for {_name, val} <- results do
        unless val >= 50 and val <= 100 do
          raise "#{@bin} #{val} outside stream range 50..100"
        end
      end
    rescue
      e in Aerospike.Error ->
        Logger.warning("    Stream query failed: #{e.message}")
    end
  end

  defp cleanup do
    @repo.drop_index(@namespace, @index_name)

    for i <- 1..@size do
      @repo.delete(Aerospike.key(@namespace, @set, "qp_#{i}"))
    end
  end
end

defmodule Demo.Examples.ScanPaginate do
  @moduledoc """
  Demonstrates paginated scan using `Aerospike.page/3` and `Aerospike.Cursor`.

  Fetches records in fixed-size pages, using the cursor from each page to
  resume where the previous page left off. Also demonstrates serializing
  cursors with `Cursor.encode/1` and `Cursor.decode/1` for external storage.
  """

  require Logger

  alias Aerospike.Cursor
  alias Aerospike.Page
  alias Aerospike.Scan

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_scan_page"
  @size 12
  @page_size 5

  def run do
    write_records()
    paginate_all()
    cursor_serialization()
    cleanup()
  end

  defp write_records do
    Logger.info("  Writing #{@size} records for paginated scan...")

    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "page_#{i}")
      bins = %{"name" => "item_#{i}", "idx" => i}
      :ok = @repo.put!(key, bins)
    end
  end

  defp paginate_all do
    Logger.info("  Paginating with page_size=#{@page_size}...")

    scan = Scan.new(@namespace, @set) |> Scan.max_records(@page_size)
    paginate_loop(scan, nil, 1, 0)
  end

  defp paginate_loop(scan, cursor, page_num, total) do
    opts = if cursor, do: [cursor: cursor], else: []

    {:ok, %Page{records: records, cursor: next_cursor, done?: done?}} =
      @repo.page(scan, opts)

    count = length(records)
    new_total = total + count

    Logger.info(
      "  Page #{page_num}: #{count} records (total so far: #{new_total}, done?: #{done?})"
    )

    for r <- records do
      Logger.info("    name=#{r.bins["name"]} idx=#{r.bins["idx"]}")
    end

    if done? do
      Logger.info("  Pagination complete: #{new_total} records across #{page_num} pages.")

      unless new_total >= @size do
        raise "Expected at least #{@size} total records, got #{new_total}"
      end
    else
      paginate_loop(scan, next_cursor, page_num + 1, new_total)
    end
  end

  defp cursor_serialization do
    Logger.info("  Demonstrating cursor serialization...")

    scan = Scan.new(@namespace, @set) |> Scan.max_records(3)
    {:ok, %Page{cursor: cursor, done?: done?}} = @repo.page(scan)

    if done? do
      Logger.info("  All records fit in one page — no cursor to serialize.")
    else
      encoded = Cursor.encode(cursor)

      Logger.info(
        "  Encoded cursor (#{byte_size(encoded)} bytes): #{String.slice(encoded, 0, 40)}..."
      )

      {:ok, decoded} = Cursor.decode(encoded)
      {:ok, %Page{records: records}} = @repo.page(scan, cursor: decoded)

      Logger.info("  Resumed from decoded cursor: got #{length(records)} more records.")
    end
  end

  defp cleanup do
    for i <- 1..@size do
      key = Aerospike.key(@namespace, @set, "page_#{i}")
      @repo.delete(key)
    end
  end
end

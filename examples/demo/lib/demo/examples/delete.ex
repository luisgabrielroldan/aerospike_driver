defmodule Demo.Examples.Delete do
  @moduledoc """
  Demonstrates `Aerospike.delete/3`: removing records, idempotent deletes,
  and the `durable_delete` option for on-disk namespaces.
  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo_del"

  def run do
    delete_existing()
    delete_nonexistent()
    delete_with_filter()
    cleanup()
  end

  defp delete_existing do
    key = key("existing")
    :ok = Aerospike.put!(@conn, key, %{"status" => "active"})

    {:ok, true} = Aerospike.delete(@conn, key)
    Logger.info("  Delete existing record: returned true")

    {:ok, false} = Aerospike.exists(@conn, key)
    Logger.info("  Verified: record no longer exists")
  end

  defp delete_nonexistent do
    key = key("never_created")

    {:ok, false} = Aerospike.delete(@conn, key)
    Logger.info("  Delete non-existent record: returned false (idempotent)")
  end

  defp delete_with_filter do
    key = key("filtered")
    :ok = Aerospike.put!(@conn, key, %{"age" => 25})

    expr = Aerospike.Exp.gt(Aerospike.Exp.int_bin("age"), Aerospike.Exp.val(30))
    {:error, err} = Aerospike.delete(@conn, key, filter: expr)

    unless err.code == :filtered_out do
      raise "Expected :filtered_out, got #{err.code}"
    end

    Logger.info("  Delete with non-matching filter: rejected (filtered_out)")

    expr2 = Aerospike.Exp.gt(Aerospike.Exp.int_bin("age"), Aerospike.Exp.val(20))
    {:ok, true} = Aerospike.delete(@conn, key, filter: expr2)
    Logger.info("  Delete with matching filter (age > 20): success")
  end

  defp cleanup do
    for id <- ["existing", "never_created", "filtered"] do
      Aerospike.delete(@conn, key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end

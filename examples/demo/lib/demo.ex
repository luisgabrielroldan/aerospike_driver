defmodule Demo do
  @moduledoc """
  Runs all Aerospike Go client examples ported to Elixir.

  Each example is a module with a `run/0` function under `Demo.Examples.*`.
  Call `Demo.run_all/0` to execute them sequentially.
  """

  require Logger

  @examples [
    Demo.Examples.Simple,
    Demo.Examples.Put,
    Demo.Examples.Get,
    Demo.Examples.PutGet,
    Demo.Examples.Delete,
    Demo.Examples.Exists,
    Demo.Examples.Add,
    Demo.Examples.Append,
    Demo.Examples.Prepend,
    Demo.Examples.Operate,
    Demo.Examples.Replace,
    Demo.Examples.Generation,
    Demo.Examples.Expire,
    Demo.Examples.Touch,
    Demo.Examples.Batch,
    Demo.Examples.BatchOperate,
    Demo.Examples.BatchHelpers,
    Demo.Examples.PolicyOptions,
    Demo.Examples.ListMap,
    Demo.Examples.ListOps,
    Demo.Examples.MapOps,
    Demo.Examples.BitOps,
    Demo.Examples.HllOps,
    Demo.Examples.ScanSerial,
    Demo.Examples.ScanParallel,
    Demo.Examples.ScanPaginate,
    Demo.Examples.CountSetObjects,
    Demo.Examples.Expressions,
    Demo.Examples.QueryAggregate,
    Demo.Examples.QueryAggregateResult,
    Demo.Examples.QueryBackground,
    Demo.Examples.GeojsonQuery,
    Demo.Examples.Udf,
    Demo.Examples.Info,
    Demo.Examples.Truncate,
    Demo.Examples.SecondaryIndex,
    Demo.Examples.QueryPaginate,
    Demo.Examples.PartitionFilter,
    Demo.Examples.BatchUdf,
    Demo.Examples.NestedCdt,
    Demo.Examples.TxnConcurrent,
    Demo.Examples.RuntimeMetrics,
    Demo.Examples.NodeTargeted,
    # Require special infrastructure/configuration
    Demo.Examples.TlsSecureConnection,
    Demo.Examples.PkiAuth,
    Demo.Examples.SecurityAdmin,
    Demo.Examples.XdrFilter
  ]

  @doc """
  Runs all examples sequentially. Logs success/failure for each.
  """
  def run_all do
    # Allow cluster tend to complete
    Logger.info("Waiting for cluster discovery...")
    :ok = Demo.Repo.wait_until_ready(Demo.PrimaryClusterRepo.cluster())

    Logger.info("Starting #{length(@examples)} examples\n")

    {passed, failed, skipped} =
      Enum.reduce(@examples, {0, 0, 0}, fn mod, {p, f, s} ->
        name = mod |> Module.split() |> List.last()
        Logger.info("━━━ #{name} ━━━")

        try do
          case mod.run() do
            :skipped ->
              Logger.warning("  ⏭  #{name}: skipped (API not available)\n")
              {p, f, s + 1}

            _ ->
              Logger.info("  ✅ #{name}: passed\n")
              {p + 1, f, s}
          end
        rescue
          e ->
            Logger.error("  ❌ #{name}: FAILED — #{Exception.message(e)}\n")
            {p, f + 1, s}
        end
      end)

    Logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    Logger.info("Results: #{passed} passed, #{failed} failed, #{skipped} skipped")
    Logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
  end
end

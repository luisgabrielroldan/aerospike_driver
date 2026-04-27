defmodule Demo.Examples.TxnConcurrent do
  @moduledoc """
  Demonstrates multi-record transactions (MRT).

  Shows three transaction patterns:

  1. **`transaction/2` wrapper** — automatic commit on success, abort on failure.
  2. **Abort rollback** — verifies that an aborted transaction undoes writes.
  3. **`transaction/3` with pre-created handle** — use `Txn.new/1` with custom timeout.

  Transactions require Aerospike Enterprise Edition with strong-consistency
  namespaces. On Community Edition this example will report an error.

  Uses the app-started `Demo.EnterpriseRepo` (default `localhost:3100`).
  Override with `AEROSPIKE_EE_HOST` and `AEROSPIKE_EE_PORT` env vars.
  Requires `make demo-stack-up` (or `docker compose --profile enterprise up -d`).
  """

  require Logger

  alias Aerospike.Txn

  @repo Demo.EnterpriseRepo
  @namespace "test"
  @set "demo_txn"

  def run do
    {host, port} = ee_host_port()

    if ee_reachable?(host, port) do
      run_examples()
    else
      Logger.warning("  TxnConcurrent: skipped — EE not reachable on #{host}:#{port}")
      :skipped
    end
  end

  defp run_examples do
    case transaction_wrapper() do
      :ok ->
        abort_rollback()
        manual_commit()
        cleanup()

      {:error, reason} ->
        Logger.warning("  TxnConcurrent: skipped — #{reason}")
        cleanup()
        :skipped
    end
  end

  defp transaction_wrapper do
    Logger.info("  transaction/2 wrapper: atomic multi-key write...")

    result =
      @repo.transaction(fn txn ->
        :ok = @repo.put!(key("txn_a"), %{"val" => 100}, txn: txn)
        :ok = @repo.put!(key("txn_b"), %{"val" => 200}, txn: txn)
        :committed
      end)

    case result do
      {:ok, :committed} ->
        {:ok, rec_a} = @repo.get(key("txn_a"))
        {:ok, rec_b} = @repo.get(key("txn_b"))

        unless rec_a.bins["val"] == 100 and rec_b.bins["val"] == 200 do
          raise "Transaction records mismatch: a=#{rec_a.bins["val"]}, b=#{rec_b.bins["val"]}"
        end

        Logger.info("    Committed: txn_a=#{rec_a.bins["val"]}, txn_b=#{rec_b.bins["val"]}")
        :ok

      {:error, %Aerospike.Error{code: code}} ->
        {:error, "transaction not supported (#{code})"}
    end
  end

  defp abort_rollback do
    Logger.info("  Abort rollback: verify writes are undone...")

    :ok = @repo.put!(key("txn_c"), %{"val" => 1})

    result =
      @repo.transaction(fn txn ->
        :ok = @repo.put!(key("txn_c"), %{"val" => 999}, txn: txn)
        raise Aerospike.Error.from_result_code(:parameter_error, message: "deliberate abort")
      end)

    case result do
      {:error, %Aerospike.Error{}} ->
        Logger.info("    Transaction aborted as expected.")
    end

    {:ok, rec} = @repo.get(key("txn_c"))

    unless rec.bins["val"] == 1 do
      raise "Expected txn_c=1 after abort, got #{rec.bins["val"]}"
    end

    Logger.info("    txn_c still #{rec.bins["val"]} — rollback confirmed.")
  end

  defp manual_commit do
    Logger.info("  transaction/3 with pre-created Txn handle...")

    txn = Txn.new(timeout: 5_000)

    {:ok, _} =
      @repo.transaction(txn, fn txn ->
        :ok = @repo.put!(key("txn_d"), %{"val" => 50}, txn: txn)
        :ok = @repo.put!(key("txn_e"), %{"val" => 75}, txn: txn)
      end)

    {:ok, rec_d} = @repo.get(key("txn_d"))
    {:ok, rec_e} = @repo.get(key("txn_e"))

    unless rec_d.bins["val"] == 50 and rec_e.bins["val"] == 75 do
      raise "Transaction/3 mismatch: d=#{rec_d.bins["val"]}, e=#{rec_e.bins["val"]}"
    end

    Logger.info("    transaction/3: txn_d=#{rec_d.bins["val"]}, txn_e=#{rec_e.bins["val"]}")
  end

  defp cleanup do
    for suffix <- ["txn_a", "txn_b", "txn_c", "txn_d", "txn_e"] do
      @repo.delete(key(suffix))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)

  defp ee_host_port do
    host = System.get_env("AEROSPIKE_EE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_EE_PORT", "3100") |> String.to_integer()
    {host, port}
  end

  defp ee_reachable?(host, port) do
    case :gen_tcp.connect(~c"#{host}", port, [], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        true

      {:error, _} ->
        false
    end
  end
end

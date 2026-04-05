defmodule Aerospike.Test.SlowPoolWorker do
  @moduledoc false
  @behaviour NimblePool

  @impl NimblePool
  def init_worker(pool_state) do
    {:ok, :worker, pool_state}
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, worker, pool_state) do
    Process.sleep(:infinity)
    {:ok, worker, worker, pool_state}
  end

  @impl NimblePool
  def handle_checkin(_checkin, _worker, _from, pool_state) do
    {:ok, :worker, pool_state}
  end

  @impl NimblePool
  def terminate_worker(_reason, _worker, pool_state) do
    {:ok, pool_state}
  end
end

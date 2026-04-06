defmodule Aerospike.RegisterTask do
  @moduledoc """
  Tracks UDF package registration progress.

  When `Aerospike.register_udf/3` is called, the server accepts the request
  immediately and registers the package in the background. `RegisterTask` lets
  you check progress or block until the package is fully available.

  Implements `Aerospike.AsyncTask`. The default `wait/2` polls `status/1`
  every second until registration is complete or the timeout is exceeded.

  ## Usage

      {:ok, task} = Aerospike.register_udf(:aero, "/path/to/my_module.lua", "my_module.lua")

      # Poll manually
      {:ok, status} = Aerospike.RegisterTask.status(task)

      # Block until complete (default poll interval: 1 second)
      :ok = Aerospike.RegisterTask.wait(task, timeout: 30_000)

      # Remove the UDF when no longer needed
      :ok = Aerospike.remove_udf(:aero, "my_module.lua")

  """

  @enforce_keys [:conn, :package_name]
  defstruct [:conn, :package_name]

  @type t :: %__MODULE__{
          conn: atom(),
          package_name: String.t()
        }

  use Aerospike.AsyncTask

  alias Aerospike.Router

  @default_checkout_timeout 5_000

  @impl Aerospike.AsyncTask
  def status(%__MODULE__{conn: conn, package_name: package_name}) do
    command = "udf-list"

    with {:ok, pool_pid, _node} <- Router.random_node_pool(conn),
         {:ok, map} <- Router.checkout_and_info(pool_pid, [command], @default_checkout_timeout) do
      parse_udf_list(Map.get(map, command, ""), package_name)
    end
  end

  defp parse_udf_list("", _package_name), do: {:ok, :in_progress}

  defp parse_udf_list(response, package_name) do
    entries = String.split(response, ";", trim: true)

    registered =
      Enum.any?(entries, fn entry ->
        String.contains?(entry, "filename=#{package_name}")
      end)

    if registered, do: {:ok, :complete}, else: {:ok, :in_progress}
  end
end

defmodule Aerospike.IndexTask do
  @moduledoc """
  Tracks secondary index build progress.

  When `Aerospike.create_index/4` is called, the server accepts the request
  immediately and builds the index in the background. `IndexTask` lets you
  check progress or block until the index is fully available.

  Implements `Aerospike.AsyncTask`. The default `wait/2` polls `status/1`
  every second until the index is complete or the timeout is exceeded.

  ## Usage

      {:ok, task} = MyApp.Repo.create_index("test", "demo",
        bin: "age", name: "age_idx", type: :numeric
      )

      # Poll manually
      {:ok, status} = Aerospike.IndexTask.status(task)

      # Block until complete (default poll interval: 1 second)
      :ok = Aerospike.IndexTask.wait(task, timeout: 30_000)

      # Drop the index when no longer needed
      :ok = MyApp.Repo.drop_index("test", "age_idx")

  """

  @enforce_keys [:conn, :namespace, :index_name]
  defstruct [:conn, :namespace, :index_name]

  @type t :: %__MODULE__{
          conn: atom(),
          namespace: String.t(),
          index_name: String.t()
        }

  use Aerospike.AsyncTask

  @default_checkout_timeout 5_000

  @impl Aerospike.AsyncTask
  def status(%__MODULE__{conn: conn, namespace: namespace, index_name: index_name}) do
    with {:ok, response} <-
           Aerospike.Admin.index_status(
             conn,
             namespace,
             index_name,
             pool_checkout_timeout: @default_checkout_timeout
           ) do
      parse_sindex_status(response)
    end
  end

  defp parse_sindex_status(""), do: {:ok, :complete}

  defp parse_sindex_status(response) do
    case Regex.run(~r/load_pct=(\d+)/, response) do
      [_, "100"] -> {:ok, :complete}
      [_, _] -> {:ok, :in_progress}
      nil -> {:ok, :complete}
    end
  end
end

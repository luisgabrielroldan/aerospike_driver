defmodule Aerospike.IndexTask do
  @moduledoc """
  Tracks secondary-index build progress.
  """

  alias Aerospike.Command.Admin
  alias Aerospike.Error

  @enforce_keys [:conn, :namespace, :index_name]
  defstruct [:conn, :namespace, :index_name]

  @typedoc "Option accepted by `wait/2`."
  @type wait_opt :: {:poll_interval, non_neg_integer()} | {:timeout, non_neg_integer()}

  @typedoc "Keyword options accepted by `wait/2`."
  @type wait_opts :: [wait_opt()]

  @typedoc """
  Handle returned while a secondary index is being built.

  Use `status/1` for one poll or `wait/2` to block until the index is visible.
  `wait/2` accepts `:poll_interval` and `:timeout` in milliseconds.
  """
  @type t :: %__MODULE__{
          conn: GenServer.server(),
          namespace: String.t(),
          index_name: String.t()
        }

  use Aerospike.Runtime.AsyncTask

  @impl Aerospike.Runtime.AsyncTask
  @doc """
  Blocks until the secondary-index build completes or the timeout is exceeded.

  Supported options:

    * `:poll_interval` — milliseconds to sleep between status checks.
      Defaults to `1_000`.
    * `:timeout` — maximum milliseconds to wait. When omitted, polling
      continues until the index is complete or returns an error.
  """
  @spec wait(t(), wait_opts()) :: :ok | {:error, Error.t()}
  def wait(%__MODULE__{} = task, opts) do
    AsyncTask.poll(__MODULE__, task, opts)
  end

  @impl Aerospike.Runtime.AsyncTask
  @doc """
  Returns the current secondary-index build status.
  """
  @spec status(t()) :: {:ok, :complete | :in_progress} | {:error, Error.t()}
  def status(%__MODULE__{conn: conn, namespace: namespace, index_name: index_name}) do
    case Admin.index_status(conn, namespace, index_name, []) do
      {:ok, response} -> parse_status(response)
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp parse_status(""), do: {:ok, :complete}

  defp parse_status(response) do
    case Regex.run(~r/load_pct=(\d+)/, response) do
      [_, "100"] -> {:ok, :complete}
      [_, _] -> {:ok, :in_progress}
      nil -> {:ok, :complete}
    end
  end
end

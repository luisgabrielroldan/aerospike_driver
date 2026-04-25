defmodule Aerospike.IndexTask do
  @moduledoc """
  Tracks secondary-index build progress.
  """

  alias Aerospike.Command.Admin
  alias Aerospike.Error

  @enforce_keys [:conn, :namespace, :index_name]
  defstruct [:conn, :namespace, :index_name]

  @type t :: %__MODULE__{
          conn: GenServer.server(),
          namespace: String.t(),
          index_name: String.t()
        }

  use Aerospike.Runtime.AsyncTask

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

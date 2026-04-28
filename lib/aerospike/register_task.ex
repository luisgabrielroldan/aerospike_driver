defmodule Aerospike.RegisterTask do
  @moduledoc """
  Tracks UDF package registration progress across the currently active cluster nodes.

  `Aerospike.register_udf/3` and `register_udf/4` return this handle after the
  server accepts the upload. The package may still be propagating when the
  call returns, so use `status/1` or `wait/2` before relying on it from
  `apply_udf/6`, query UDFs, or other package consumers.
  """

  alias Aerospike.Cluster
  alias Aerospike.Command.Admin
  alias Aerospike.Error

  @enforce_keys [:conn, :package_name]
  defstruct [:conn, :package_name]

  @typedoc "Option accepted by `wait/2`."
  @type wait_opt :: {:poll_interval, non_neg_integer()} | {:timeout, non_neg_integer()}

  @typedoc "Keyword options accepted by `wait/2`."
  @type wait_opts :: [wait_opt()]

  @typedoc """
  Handle returned while a UDF package propagates through the cluster.

  Use `status/1` for one poll or `wait/2` to block until every active node
  reports the package. `wait/2` accepts `:poll_interval` and `:timeout` in
  milliseconds.
  """
  @type t :: %__MODULE__{
          conn: Aerospike.cluster(),
          package_name: String.t()
        }

  use Aerospike.Runtime.AsyncTask

  @impl Aerospike.Runtime.AsyncTask
  @doc """
  Blocks until the UDF package is visible on every active node.

  Supported options:

    * `:poll_interval` — milliseconds to sleep between status checks.
      Defaults to `1_000`.
    * `:timeout` — maximum milliseconds to wait. When omitted, polling
      continues until registration completes or returns an error.
  """
  @spec wait(t(), wait_opts()) :: :ok | {:error, Error.t()}
  def wait(%__MODULE__{} = task, opts) do
    AsyncTask.poll(__MODULE__, task, opts)
  end

  @impl Aerospike.Runtime.AsyncTask
  @doc """
  Returns whether the UDF package is visible on every active cluster node.
  """
  @spec status(t()) :: {:ok, :complete | :in_progress} | {:error, Error.t()}
  def status(%__MODULE__{conn: conn, package_name: package_name}) do
    with {:ok, node_names} <- target_node_names(conn) do
      poll_nodes(conn, node_names, package_name)
    end
  end

  defp poll_nodes(conn, node_names, package_name) do
    Enum.reduce_while(node_names, {:ok, :complete}, fn node_name, _acc ->
      case node_registered?(conn, node_name, package_name) do
        {:ok, true} -> {:cont, {:ok, :complete}}
        {:ok, false} -> {:halt, {:ok, :in_progress}}
        {:error, %Error{} = err} -> {:halt, {:error, err}}
      end
    end)
  end

  defp node_registered?(conn, node_name, package_name) do
    with {:ok, response} <- Admin.info_node(conn, node_name, "udf-list", []),
         {:ok, udfs} <- Admin.parse_udf_inventory(response) do
      {:ok, Enum.any?(udfs, &(&1.filename == package_name))}
    end
  end

  defp target_node_names(conn) do
    if cluster_ready?(conn) do
      case active_nodes(conn) do
        [] -> {:error, Error.from_result_code(:invalid_node)}
        node_names -> {:ok, node_names}
      end
    else
      {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  defp cluster_ready?(conn) do
    Cluster.ready?(conn)
  catch
    :exit, _ -> false
  end

  defp active_nodes(conn) do
    Cluster.active_nodes(conn)
  catch
    :exit, _ -> []
  end
end

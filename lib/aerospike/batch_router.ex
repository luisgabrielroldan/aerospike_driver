defmodule Aerospike.BatchRouter do
  @moduledoc """
  Pure batch grouping on top of published routing state.

  The batch executor consumes grouped node requests plus any indexed routing
  failures that were discovered before transport dispatch.
  """

  alias Aerospike.BatchCommand.Entry
  alias Aerospike.BatchCommand.NodeRequest
  alias Aerospike.Key
  alias Aerospike.Router

  defmodule RoutingFailure do
    @moduledoc false

    @enforce_keys [:entry, :reason]
    defstruct [:entry, :reason]

    @type t :: %__MODULE__{
            entry: Entry.t(),
            reason: Router.reason()
          }
  end

  defmodule Grouping do
    @moduledoc false

    @enforce_keys [:node_requests, :routing_failures]
    defstruct [:node_requests, :routing_failures]

    @type t :: %__MODULE__{
            node_requests: [NodeRequest.t()],
            routing_failures: [RoutingFailure.t()]
          }
  end

  @type dispatch :: :write | {:read, Router.replica_policy(), non_neg_integer()}
  @type payload_fun :: (Key.t(), non_neg_integer() -> term())
  @type reason :: :cluster_not_ready

  @doc """
  Groups `keys` into per-node batch requests while preserving input indices.

  Global readiness refusal (`:cluster_not_ready`) aborts the whole grouping
  because no key can be routed honestly against an incomplete partition view.
  Partition-local misses (`:no_master`) are retained as indexed failures so a
  later merge step can surface mixed outcomes deterministically.
  """
  @spec group_keys(Router.tables(), [Key.t()], keyword()) ::
          {:ok, Grouping.t()} | {:error, reason()}
  def group_keys(tables, keys, opts \\ []) when is_list(keys) and is_list(opts) do
    dispatch = Keyword.get(opts, :dispatch, {:read, :master, 0})
    payload_fun = Keyword.get(opts, :payload_fun, fn _key, _index -> nil end)

    case reduce_keys(keys, tables, dispatch, payload_fun) do
      {:ok, grouped, node_order, routing_failures} ->
        {:ok,
         %Grouping{
           node_requests: build_node_requests(grouped, node_order),
           routing_failures: Enum.reverse(routing_failures)
         }}

      {:error, :cluster_not_ready} = err ->
        err
    end
  end

  defp reduce_keys(keys, tables, dispatch, payload_fun) do
    keys
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{}, [], []}, fn {key, index},
                                                {:ok, grouped, node_order, failures} ->
      entry = %Entry{index: index, key: key, payload: payload_fun.(key, index)}

      case route_key(tables, key, dispatch) do
        {:ok, node_name} ->
          {next_grouped, next_node_order} = put_entry(grouped, node_order, node_name, entry)
          {:cont, {:ok, next_grouped, next_node_order, failures}}

        {:error, :cluster_not_ready} ->
          {:halt, {:error, :cluster_not_ready}}

        {:error, reason} ->
          failure = %RoutingFailure{entry: entry, reason: reason}
          {:cont, {:ok, grouped, node_order, [failure | failures]}}
      end
    end)
  end

  defp put_entry(grouped, node_order, node_name, entry) do
    case grouped do
      %{^node_name => entries} ->
        {Map.put(grouped, node_name, [entry | entries]), node_order}

      %{} ->
        {Map.put(grouped, node_name, [entry]), node_order ++ [node_name]}
    end
  end

  defp build_node_requests(grouped, node_order) do
    Enum.map(node_order, fn node_name ->
      %NodeRequest{
        node_name: node_name,
        entries: grouped |> Map.fetch!(node_name) |> Enum.reverse(),
        payload: nil
      }
    end)
  end

  defp route_key(tables, %Key{} = key, :write), do: Router.pick_for_write(tables, key)

  defp route_key(tables, %Key{} = key, {:read, replica_policy, attempt}) do
    Router.pick_for_read(tables, key, replica_policy, attempt)
  end
end

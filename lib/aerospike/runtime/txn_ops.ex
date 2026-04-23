defmodule Aerospike.Runtime.TxnOps do
  @moduledoc false

  alias Aerospike.Cluster
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Txn

  @type tracking :: %{
          state: :open | :verified | :committed | :aborted,
          namespace: String.t() | nil,
          deadline: integer(),
          reads: %{Key.t() => term()},
          writes: MapSet.t(Key.t()),
          write_in_doubt: boolean()
        }

  @doc false
  @spec init_tracking(atom(), Txn.t()) :: true
  def init_tracking(conn_name, %Txn{} = txn) do
    :ets.insert(tracking_table(conn_name), {txn.id, empty_tracking()})
  end

  @doc false
  @spec track_read(atom(), Txn.t(), Key.t(), term()) :: true
  def track_read(conn_name, %Txn{} = txn, %Key{} = key, version) do
    if version != nil do
      update_tracking(conn_name, txn, fn t -> %{t | reads: Map.put(t.reads, key, version)} end)
    else
      true
    end
  end

  @doc false
  @spec track_write(atom(), Txn.t(), Key.t(), term(), atom()) :: true
  def track_write(conn_name, %Txn{} = txn, %Key{} = key, version, _result_code)
      when version != nil do
    update_tracking(conn_name, txn, fn t -> %{t | reads: Map.put(t.reads, key, version)} end)
  end

  def track_write(conn_name, %Txn{} = txn, %Key{} = key, nil, :ok) do
    update_tracking(conn_name, txn, fn t ->
      %{t | reads: Map.delete(t.reads, key), writes: MapSet.put(t.writes, key)}
    end)
  end

  def track_write(_conn_name, %Txn{}, %Key{}, nil, _result_code), do: true

  @doc false
  @spec track_write_in_doubt(atom(), Txn.t(), Key.t()) :: true
  def track_write_in_doubt(conn_name, %Txn{} = txn, %Key{} = key) do
    update_tracking(conn_name, txn, fn t ->
      %{
        t
        | write_in_doubt: true,
          reads: Map.delete(t.reads, key),
          writes: MapSet.put(t.writes, key)
      }
    end)
  end

  @doc false
  @spec get_tracking(atom(), Txn.t()) :: {:ok, tracking()} | {:error, :not_found}
  def get_tracking(conn_name, %Txn{} = txn) do
    case :ets.lookup(tracking_table(conn_name), txn.id) do
      [{_, data}] -> {:ok, data}
      [] -> {:error, :not_found}
    end
  end

  @doc false
  @spec get_reads(atom(), Txn.t()) :: %{Key.t() => term()}
  def get_reads(conn_name, %Txn{} = txn) do
    case get_tracking(conn_name, txn) do
      {:ok, t} -> t.reads
      _ -> %{}
    end
  end

  @doc false
  @spec get_writes(atom(), Txn.t()) :: [Key.t()]
  def get_writes(conn_name, %Txn{} = txn) do
    case get_tracking(conn_name, txn) do
      {:ok, t} -> MapSet.to_list(t.writes)
      _ -> []
    end
  end

  @doc false
  @spec set_state(atom(), Txn.t(), :open | :verified | :committed | :aborted) :: true
  def set_state(conn_name, %Txn{} = txn, state)
      when state in [:open, :verified, :committed, :aborted] do
    update_tracking(conn_name, txn, fn t -> %{t | state: state} end)
  end

  @doc false
  @spec set_namespace(atom(), Txn.t(), String.t()) :: :ok | {:error, Error.t()}
  def set_namespace(conn_name, %Txn{} = txn, namespace) when is_binary(namespace) do
    case get_tracking(conn_name, txn) do
      {:ok, %{namespace: nil}} ->
        update_tracking(conn_name, txn, fn t -> %{t | namespace: namespace} end)
        :ok

      {:ok, %{namespace: ^namespace}} ->
        :ok

      {:ok, %{namespace: other}} ->
        {:error,
         %Error{
           code: :parameter_error,
           message:
             "transaction namespace mismatch: expected #{inspect(other)}, got #{inspect(namespace)}"
         }}

      {:error, :not_found} ->
        {:error, %Error{code: :parameter_error, message: "transaction not initialized"}}
    end
  end

  @doc false
  @spec set_deadline(atom(), Txn.t(), integer()) :: true
  def set_deadline(conn_name, %Txn{} = txn, deadline) when is_integer(deadline) do
    update_tracking(conn_name, txn, fn t -> %{t | deadline: deadline} end)
  end

  @doc false
  @spec get_deadline(atom(), Txn.t()) :: integer()
  def get_deadline(conn_name, %Txn{} = txn) do
    case get_tracking(conn_name, txn) do
      {:ok, t} -> t.deadline
      _ -> 0
    end
  end

  @doc false
  @spec monitor_exists?(atom(), Txn.t()) :: boolean()
  def monitor_exists?(conn_name, %Txn{} = txn) do
    get_deadline(conn_name, txn) != 0
  end

  @doc false
  @spec close_monitor?(atom(), Txn.t()) :: boolean()
  def close_monitor?(conn_name, %Txn{} = txn) do
    case get_tracking(conn_name, txn) do
      {:ok, t} -> t.deadline != 0 and not t.write_in_doubt
      _ -> false
    end
  end

  @doc false
  @spec write_exists?(atom(), Txn.t(), Key.t()) :: boolean()
  def write_exists?(conn_name, %Txn{} = txn, %Key{} = key) do
    case get_tracking(conn_name, txn) do
      {:ok, t} -> MapSet.member?(t.writes, key)
      _ -> false
    end
  end

  @doc false
  @spec get_read_version(atom(), Txn.t(), Key.t()) :: term() | nil
  def get_read_version(conn_name, %Txn{} = txn, %Key{} = key) do
    case get_tracking(conn_name, txn) do
      {:ok, t} -> Map.get(t.reads, key)
      _ -> nil
    end
  end

  @doc false
  @spec set_write_in_doubt(atom(), Txn.t()) :: true
  def set_write_in_doubt(conn_name, %Txn{} = txn) do
    update_tracking(conn_name, txn, fn t -> %{t | write_in_doubt: true} end)
  end

  @doc false
  @spec cleanup(atom(), Txn.t()) :: true
  def cleanup(conn_name, %Txn{} = txn) do
    :ets.delete(tracking_table(conn_name), txn.id)
  end

  @doc false
  @spec verify_command(atom(), Txn.t()) :: :ok | {:error, Error.t()}
  def verify_command(conn_name, %Txn{} = txn) do
    case get_tracking(conn_name, txn) do
      {:ok, %{state: :open}} ->
        :ok

      {:ok, %{state: state}} ->
        {:error,
         %Error{
           code: :parameter_error,
           message: "transaction is not open (state: #{state})"
         }}

      {:error, :not_found} ->
        {:error, %Error{code: :parameter_error, message: "transaction not initialized"}}
    end
  end

  defp empty_tracking do
    %{
      state: :open,
      namespace: nil,
      deadline: 0,
      reads: %{},
      writes: MapSet.new(),
      write_in_doubt: false
    }
  end

  defp update_tracking(conn_name, %Txn{} = txn, fun) do
    case :ets.lookup(tracking_table(conn_name), txn.id) do
      [{_, data}] ->
        :ets.insert(tracking_table(conn_name), {txn.id, fun.(data)})

      [] ->
        true
    end
  end

  defp tracking_table(conn_name) do
    table = Cluster.tables(conn_name).txn_tracking

    case :ets.info(table) do
      :undefined ->
        raise ArgumentError,
              "transaction tracking table is unavailable for #{inspect(conn_name)}"

      _info ->
        table
    end
  end
end

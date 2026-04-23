defmodule Aerospike.Command.Operate do
  @moduledoc """
  OPERATE command adapter for the spike.

  The supported surface stays intentionally narrow:

    * `{:write, bin, value}` for simple bin writes
    * `{:read, bin}` for bin reads that follow those writes
    * `{:add, bin, delta}` for integer or float increments
    * `{:append, bin, suffix}` for string suffix writes
    * `{:prepend, bin, prefix}` for string prefix writes
    * `:touch` for header-only metadata refresh
    * `:delete` for record removal

  The spike also accepts the `Aerospike.Op` helpers for read-only and
  CDT-style operations that share the same unary substrate.

  Routing is chosen per call from the built operations. Read-only
  batches route as read traffic; any batch that contains a write routes
  as write traffic.
  """

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.OperateFlags
  alias Aerospike.Protocol.Response
  alias Aerospike.Runtime.TxnSupport
  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport

  @type simple_operation ::
          {:read, String.t() | atom()}
          | {:write, String.t() | atom(), term()}
          | {:add, String.t() | atom(), integer() | float()}
          | {:append, String.t() | atom(), String.t()}
          | {:prepend, String.t() | atom(), String.t()}
          | :touch
          | :delete

  @type operation_input :: simple_operation() | Operation.t()

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:ttl, non_neg_integer()}
          | {:generation, non_neg_integer()}

  @type result ::
          {:ok, Aerospike.Record.t()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}

  @spec execute(GenServer.server(), Key.t(), [operation_input()], [option()]) :: result()
  def execute(tender, %Key{} = key, operations, opts \\ []) when is_list(opts) do
    with {:ok, txn} <- TxnSupport.txn_from_opts(opts),
         {:ok, built_operations} <- build_operations(operations),
         {:ok, policy} <- UnarySupport.operate_policy(tender, opts) do
      flags = OperateFlags.scan_ops(built_operations)
      input = command_input(key, tender, txn, opts, built_operations, flags, policy)

      with :ok <- TxnSupport.prepare_txn_for_operate(tender, txn, key, flags.has_write?, opts) do
        tender
        |> UnarySupport.run_command(key, policy, command(flags), input)
        |> maybe_track_txn_in_doubt(flags, tender, txn, key)
      end
    end
  end

  defp command_input(
         key,
         tender,
         txn,
         opts,
         built_operations,
         flags,
         %Policy.UnaryWrite{} = policy
       ) do
    %{
      key: key,
      conn: tender,
      txn: txn,
      opts: opts,
      kind: if(flags.has_write?, do: :write, else: :read),
      operations: built_operations,
      flags: flags,
      ttl: policy.ttl,
      generation: policy.generation
    }
  end

  defp maybe_track_txn_in_doubt(
         {:error, %Error{} = err} = result,
         %{has_write?: true},
         tender,
         txn,
         key
       ) do
    TxnSupport.track_txn_in_doubt(tender, txn, key, err)
    result
  end

  defp maybe_track_txn_in_doubt(result, _flags, _tender, _txn, _key), do: result

  defp command(%{has_write?: has_write?}) do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: if(has_write?, do: :write, else: :read),
      retry_transport: false,
      build_request: &encode_operate/1,
      parse_response: &parse_record_response/2
    )
  end

  defp build_operations(operations) when is_list(operations) and operations != [] do
    operations
    |> Enum.reduce_while({:ok, []}, fn operation, {:ok, acc} ->
      case build_operation(operation) do
        {:ok, built_operation} -> {:cont, {:ok, [built_operation | acc]}}
        {:error, %Error{}} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, built_operations} -> {:ok, Enum.reverse(built_operations)}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operations([]) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "operate requires at least one operation"
     )}
  end

  defp build_operations(other) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "operate operations must be a list, got: #{inspect(other)}"
     )}
  end

  defp build_operation(%Operation{} = operation), do: {:ok, operation}

  defp build_operation({:read, bin_name}) do
    case Operation.from_simple({:read, normalize_bin_name(bin_name)}) do
      {:ok, operation} -> {:ok, operation}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operation({:write, bin_name, value}) do
    case Operation.from_simple({:write, normalize_bin_name(bin_name), value}) do
      {:ok, operation} -> {:ok, operation}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operation({:add, bin_name, value}) do
    case Operation.from_simple({:add, normalize_bin_name(bin_name), value}) do
      {:ok, operation} -> {:ok, operation}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operation({:append, bin_name, value}) do
    case Operation.from_simple({:append, normalize_bin_name(bin_name), value}) do
      {:ok, operation} -> {:ok, operation}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operation({:prepend, bin_name, value}) do
    case Operation.from_simple({:prepend, normalize_bin_name(bin_name), value}) do
      {:ok, operation} -> {:ok, operation}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operation(:touch) do
    case Operation.from_simple(:touch) do
      {:ok, operation} -> {:ok, operation}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operation(:delete) do
    case Operation.from_simple(:delete) do
      {:ok, operation} -> {:ok, operation}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_operation(other) do
    Operation.from_simple(other)
  end

  defp normalize_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_bin_name(bin_name), do: bin_name

  defp encode_operate(%{
         key: %Key{} = key,
         conn: conn,
         opts: opts,
         operations: operations,
         flags: %{
           has_write?: has_write?,
           read_bin?: read_bin?,
           read_header?: read_header?,
           respond_all?: respond_all?
         },
         ttl: ttl,
         generation: generation
       }) do
    key
    |> AsmMsg.key_command(operations,
      read: read_bin? or read_header?,
      read_header: read_header?,
      write: has_write?,
      send_key: true,
      respond_all_ops: respond_all?,
      ttl: ttl,
      generation: generation
    )
    |> TxnSupport.maybe_add_mrt_fields(conn, key, opts, has_write?)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_record_response(body, %{key: %Key{} = key, conn: conn, txn: txn, kind: kind}) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case Response.parse_record_response(msg, key) do
        {:ok, _} = ok ->
          TxnSupport.track_txn_response(conn, txn, key, kind, msg, ok)
          ok

        {:error, _} = err ->
          err
      end
    end)
  end
end

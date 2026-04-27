defmodule Aerospike.Command.Get do
  @moduledoc false

  alias Aerospike.Command.UnaryCommand
  alias Aerospike.Command.UnarySupport
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response
  alias Aerospike.Record
  alias Aerospike.Runtime.TxnSupport

  @type bin_name :: String.t() | atom()
  @type mode :: :all | :header | [bin_name()]

  @type option ::
          {:timeout, non_neg_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:replica_policy, :master | :sequence}
          | {:filter, Aerospike.Exp.t() | nil}

  @doc """
  Reads `key` from the cluster identified by `tender`.

  `tender` is a running `Aerospike.Cluster.Tender` (pid or registered name). The
  `bins` may be `:all`, `:header`, or a non-empty list of string or atom bin
  names. Named-bin reads encode one read operation per requested bin and return
  the normal `%Aerospike.Record{}` response containing only the projected bins
  the server returns.

  Options:

    * `:timeout` — total op-budget milliseconds shared across the
      initial attempt and every retry. Default `5_000`.
    * `:max_retries` — overrides the cluster-default retry cap. `0`
      disables retry entirely.
    * `:sleep_between_retries_ms` — fixed delay between attempts.
    * `:replica_policy` — `:master` or `:sequence`.
    * `:filter` — non-empty `%Aerospike.Exp{}` server-side filter
      expression, or `nil` for no filter.

  Errors surface as typed `Aerospike.Error` structs or the router's
  `:cluster_not_ready` / `:no_master` atoms for routing failures.
  """
  @spec execute(GenServer.server(), Key.t(), mode() | term(), [option()]) ::
          {:ok, Record.t()}
          | {:error, Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def execute(tender, key, bins, opts \\ [])

  def execute(tender, %Key{} = key, mode, opts) when mode in [:all, :header] do
    execute_read(tender, key, mode, opts, [])
  end

  def execute(tender, %Key{} = key, bins, opts) when is_list(bins) do
    with {:ok, operations} <- read_operations(bins) do
      execute_read(tender, key, :bins, opts, operations)
    end
  end

  def execute(_tender, %Key{}, bins, _opts) do
    invalid_argument(
      "Aerospike.Command.Get expects :all, :header, or a non-empty list of string or atom bin names, got: #{inspect(bins)}"
    )
  end

  defp execute_read(tender, %Key{} = key, mode, opts, operations) do
    with {:ok, txn} <- TxnSupport.txn_from_opts(opts),
         {:ok, policy} <- UnarySupport.read_policy(tender, opts),
         :ok <- TxnSupport.prepare_txn_read(tender, txn, key) do
      UnarySupport.run_command(
        tender,
        key,
        policy,
        command(),
        %{
          key: key,
          conn: tender,
          txn: txn,
          opts: opts,
          mode: mode,
          operations: operations,
          filter: policy.filter
        }
      )
    end
  end

  defp read_operations([]) do
    invalid_argument("Aerospike.Command.Get expects at least one named bin")
  end

  defp read_operations(bins) do
    do_read_operations(bins, [])
  end

  defp do_read_operations([], acc), do: {:ok, Enum.reverse(acc)}

  defp do_read_operations([bin | rest], acc) when is_binary(bin) and byte_size(bin) > 0 do
    do_read_operations(rest, [Operation.read(bin) | acc])
  end

  defp do_read_operations([bin | rest], acc) when is_atom(bin) do
    do_read_operations([Atom.to_string(bin) | rest], acc)
  end

  defp do_read_operations([bin | _rest], _acc) do
    invalid_argument(
      "Aerospike.Command.Get bin names must be non-empty strings or atoms, got: #{inspect(bin)}"
    )
  end

  defp command do
    UnaryCommand.new!(
      name: __MODULE__,
      dispatch: :read,
      build_request: &encode_read/1,
      parse_response: &parse_record_response/2
    )
  end

  defp encode_read(%{
         key: %Key{} = key,
         conn: conn,
         opts: opts,
         mode: mode,
         operations: operations,
         filter: filter
       }) do
    key
    |> AsmMsg.key_command(operations,
      read: true,
      read_all: mode == :all,
      read_header: mode == :header
    )
    |> AsmMsg.maybe_add_filter_exp(filter)
    |> TxnSupport.maybe_add_mrt_fields(conn, key, opts, false)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
  end

  defp parse_record_response(body, %{key: key, conn: conn, txn: txn}) do
    UnarySupport.parse_as_msg(body, fn msg ->
      case Response.parse_record_response(msg, key) do
        {:ok, _} = ok ->
          TxnSupport.track_txn_response(conn, txn, key, :read, msg, ok)
          ok

        {:error, _} = err ->
          err
      end
    end)
  end

  defp invalid_argument(message) do
    {:error, Error.from_result_code(:invalid_argument, message: message)}
  end
end

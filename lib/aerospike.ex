defmodule Aerospike do
  @moduledoc """
  Public entry point for the spike client.

  Exposes the minimum surface needed to prove the stack connects:
  a single `get/3` that routes a `%Aerospike.Key{}` through the
  cluster's `Aerospike.Tender`, opens a transport connection to the
  chosen node, and returns a parsed `%Aerospike.Record{}`.

  Everything else (put, delete, exists, batch, scan, query, operate,
  expressions, policies, auth, TLS, compression, pooling) is out of
  scope for the spike.
  """

  alias Aerospike.Get
  alias Aerospike.Key
  alias Aerospike.Supervisor, as: ClusterSupervisor

  @typedoc """
  Identifier for a running cluster, i.e. an `Aerospike.Tender` process
  (pid or registered name).
  """
  @type cluster :: GenServer.server()

  @doc """
  Starts a supervised cluster under `Aerospike.Supervisor`.

  Forwards `opts` to `Aerospike.Supervisor.start_link/1`, which brings
  up `Aerospike.TableOwner`, `Aerospike.NodeSupervisor`, and
  `Aerospike.Tender` under a `rest_for_one` supervisor. The `:name`
  option is used both as the supervisor's registered name and as the
  Tender's identity for `get/3`.

  Required options: `:name`, `:transport`, `:seeds`, `:namespaces`.

  Pool-level knobs (forwarded to `Aerospike.NodeSupervisor.start_pool/2`
  on each pool start):

    * `:pool_size` — workers per node. Positive integer.
    * `:idle_timeout_ms` — milliseconds a worker may sit idle before
      `NimblePool.handle_ping/2` evicts it. Positive integer. Defaults
      stay under Aerospike's `proto-fd-idle-ms`.
    * `:max_idle_pings` — bound on how many idle workers NimblePool
      may drop per verification cycle. Positive integer.

  TCP-level tuning knobs (passed verbatim to
  `Aerospike.Transport.Tcp.connect/3` via the `:connect_opts` keyword):

    * `:connect_timeout_ms` — handshake + write-buffer drain deadline.
    * `:info_timeout` — read deadline applied to every `info/2` call.
      Defaults to `:connect_timeout_ms`.
    * `:tcp_nodelay` — boolean, default `true`.
    * `:tcp_keepalive` — boolean, default `true`.
    * `:tcp_sndbuf` / `:tcp_rcvbuf` — positive integer kernel buffer
      sizes. Unset lets the kernel pick.

  See `Aerospike.Supervisor` for the full option shape and validation
  rules.
  """
  @spec start_link([ClusterSupervisor.option()]) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    ClusterSupervisor.start_link(opts)
  end

  @doc """
  Reads `key` from `cluster`. `bins` must be `:all` in the spike —
  named-bin reads will be added with `put` in a later milestone.

  Options:

    * `:timeout` — total op-budget milliseconds for the call, shared
      across the initial send and every retry. Default `5_000`.
    * `:max_retries` — overrides the cluster-default retry cap for this
      call. `0` disables retry entirely. See `Aerospike.RetryPolicy`.
    * `:sleep_between_retries_ms` — fixed delay between retry attempts.
    * `:replica_policy` — `:master` (all attempts against the master)
      or `:sequence` (walk the replica list by attempt index).

  Returns `{:ok, %Aerospike.Record{}}` on hit,
  `{:error, %Aerospike.Error{code: :key_not_found}}` on miss, or a
  routing atom (`:cluster_not_ready`, `:no_master`, `:unknown_node`)
  when the cluster view cannot serve the request.
  """
  @spec get(cluster(), Key.t(), :all, keyword()) ::
          {:ok, Aerospike.Record.t()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def get(cluster, key, bins \\ :all, opts \\ [])

  def get(cluster, %Key{} = key, bins, opts) do
    Get.execute(cluster, key, bins, opts)
  end
end

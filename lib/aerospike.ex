defmodule Aerospike do
  @moduledoc """
  Public entry point for the Aerospike Elixir driver.

  Internally this codebase may still be called "the spike", but the
  public package identity is the new `aerospike_driver` intended to
  replace the older driver that remains in the workspace as a migration
  reference. The useful contract here is still narrower than "full
  client parity": start-up should be explicit, supported command
  families should be discoverable, and the named proof suites should
  match the public surface.

  The driver currently proves a small unary command family on one shared execution
  path plus batch, scan, query, and transaction helpers over the same
  supervised cluster runtime:

    * `get/3` reads all bins for a key
    * `put/4` writes a bin map
    * `exists/2` performs a header-only existence probe
    * `touch/2` updates record metadata
    * `delete/2` removes a record
    * `operate/4` runs simple and CDT-style unary operation lists built
      with `Aerospike.Op`, `Aerospike.Op.List`, `Aerospike.Op.Map`, and
      `Aerospike.Ctx`
    * `batch_get/4` reads multiple keys and returns per-key results in
      caller order
    * `child_spec/1`, `close/2`, `key/3`, and `key_digest/3` round out
      the root lifecycle and key-construction boundary
    * `query_stream!/3`, `query_all/3`, `query_count/3`, and
      `query_aggregate/6` run secondary-index queries through the same
      node-preparation pipeline, with lazy outer streams but
      node-buffered record delivery
    * `query_execute/4` and `query_udf/6` run background query jobs on
      that same setup path and return pollable task handles
    * `scan_stream/3`, `scan_stream!/3`, `scan_all/3`, `scan_all!/3`,
      `scan_count/3`, and `scan_count!/3` run scan fan-out across the same
      scan/runtime setup, again with lazy outer streams and node-buffered
      record delivery
    * scan/query helpers that already support node targeting accept
      `node: node_name` in `opts`

  Quick-start shape:

      {:ok, _sup} =
        Aerospike.start_link(
          name: :spike,
          transport: Aerospike.Transport.Tcp,
          hosts: ["127.0.0.1:3000"],
          namespaces: ["test"],
          pool_size: 2
        )

      Aerospike.Cluster.ready?(:spike)

  The repo README names the supported validation profiles:

    * Community Edition single-node on `localhost:3000`
    * Community Edition three-node cluster from `../aerospike_driver/`
    * Enterprise Edition variants from this repo's `docker-compose.yml`

  The public `Stream` helpers are lazy only at the API boundary. The
  current runtime still drains each node stream into memory before
  yielding that node's records downstream, so it does not promise
  frame-by-frame cross-node backpressure or cancellation coordination.

  Broader batch semantics, the remaining expression surface, and the
  wider policy surface remain out of scope until later work proves them.

  Caller-facing policy validation and default materialization now lives
  under `Aerospike.Policy`. Public command functions still accept
  keyword opts, but command paths no longer merge or validate those
  policy families ad hoc.
  """

  alias Aerospike.Command.Admin
  alias Aerospike.Command.BatchGet
  alias Aerospike.Command.Delete
  alias Aerospike.Error
  alias Aerospike.ExecuteTask
  alias Aerospike.Command.Exists
  alias Aerospike.Command.Get
  alias Aerospike.IndexTask
  alias Aerospike.Key
  alias Aerospike.Command.Operate
  alias Aerospike.Page
  alias Aerospike.Command.Put
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.Command.ScanOps
  alias Aerospike.Cluster.Supervisor, as: ClusterSupervisor
  alias Aerospike.Command.Touch
  alias Aerospike.Txn
  alias Aerospike.Runtime.TxnRoll

  @typedoc """
  Identifier for a running cluster for the read/write/query/scan facade,
  i.e. its registered name or a pid registered under that name.
  """
  @type cluster :: GenServer.server()

  @typedoc """
  Registered atom name for a running cluster.

  Lifecycle and transaction helpers resolve supervisor and ETS resources from
  this name, so they do not currently accept arbitrary `GenServer.server()`
  identities.
  """
  @type named_cluster :: atom()
  @doc """
  Starts a supervised cluster.

  Internally this delegates to `Aerospike.Cluster.Supervisor.start_link/1`,
  which boots the cluster-state owner, per-node pool supervisor, partition-map
  writer, and tend-cycle process under one `rest_for_one` tree. The `:name`
  option is the public cluster identity later passed to `get/3`,
  `Aerospike.Cluster.ready?/1`, and the other facade/read-side helpers.

  Required options: `:name`, `:transport`, `:hosts`, `:namespaces`.

  Startup validation happens synchronously at this boundary. Shape
  errors for required opts, retry/breaker knobs, TLS/connect opts, and
  auth pairs fail `start_link/1` immediately instead of surfacing later
  from the first tend cycle or pool worker.

  Cluster lifecycle knobs:

    * `:tend_trigger` — `:timer` (default) or `:manual`.
    * `:tend_interval_ms` — automatic tend period in milliseconds.
      Positive integer.
    * `:failure_threshold` — consecutive tend failures before the
      tend cycle demotes a node. Non-negative integer.

  Pool-level knobs (forwarded internally on each node-pool start):

    * `:pool_size` — workers per node. Positive integer.
    * `:idle_timeout_ms` — milliseconds a worker may sit idle before
      `NimblePool.handle_ping/2` evicts it. Positive integer. Defaults
      stay under Aerospike's `proto-fd-idle-ms`.
    * `:max_idle_pings` — bound on how many idle workers NimblePool
      may drop per verification cycle. Positive integer.

  Breaker and retry knobs:

    * `:circuit_open_threshold` — consecutive node failures tolerated
      before new commands are refused. Non-negative integer.
    * `:max_concurrent_ops_per_node` — in-flight plus queued command
      cap enforced per node. Positive integer.
    * `:max_retries` — retries after the initial attempt. Non-negative
      integer.
    * `:sleep_between_retries_ms` — fixed delay between retries.
      Non-negative integer.
    * `:replica_policy` — `:master` or `:sequence`.

  Cluster feature toggles:

    * `:use_compression` — boolean cluster-wide request-compression
      opt-in, gated per node by advertised capabilities.
    * `:use_services_alternate` — boolean toggle for
      `peers-clear-alt` discovery.

  Auth opts:

    * `:user` / `:password` — cluster-wide credentials. Must be passed
      together or omitted together.

  TCP-level tuning knobs (passed verbatim to
  `Aerospike.Transport.Tcp.connect/3` via the `:connect_opts` keyword):

    * `:connect_timeout_ms` — handshake + write-buffer drain deadline.
    * `:info_timeout` — read deadline applied to every `info/2` call.
      Defaults to `:connect_timeout_ms`.
    * `:tcp_nodelay` — boolean, default `true`.
    * `:tcp_keepalive` — boolean, default `true`.
    * `:tcp_sndbuf` / `:tcp_rcvbuf` — positive integer kernel buffer
      sizes. Unset lets the kernel pick.

  `Aerospike.Cluster.Supervisor` documents the underlying validation and child
  ownership details.
  """
  @spec start_link([ClusterSupervisor.option()]) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    ClusterSupervisor.start_link(opts)
  end

  @doc """
  Returns a child specification for one supervised cluster.

  This delegates to `Aerospike.Cluster.Supervisor.child_spec/1`, so the
  accepted options and validation boundary match `start_link/1`.
  """
  @spec child_spec([ClusterSupervisor.option()]) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    ClusterSupervisor.child_spec(opts)
  end

  @doc """
  Stops the supervised cluster registered under the atom `cluster`.

  This targets the registered cluster supervisor name derived from `cluster`
  and returns `:ok` when the supervisor exits or is already absent.
  """
  @spec close(named_cluster(), timeout :: non_neg_integer()) :: :ok
  def close(cluster, timeout \\ 15_000)
      when is_atom(cluster) and is_integer(timeout) and timeout >= 0 do
    case Process.whereis(ClusterSupervisor.sup_name(cluster)) do
      nil -> :ok
      pid -> Supervisor.stop(pid, :normal, timeout)
    end
  end

  @doc """
  Builds a key from namespace, set, and a user key.

  This is a thin wrapper over `Aerospike.Key.new/3`.
  """
  @spec key(String.t(), String.t(), String.t() | integer()) :: Key.t()
  def key(namespace, set, user_key), do: Key.new(namespace, set, user_key)

  @doc """
  Builds a key from namespace, set, and an existing 20-byte digest.

  This is a thin wrapper over `Aerospike.Key.from_digest/3`.
  """
  @spec key_digest(String.t(), String.t(), <<_::160>>) :: Key.t()
  def key_digest(namespace, set, digest), do: Key.from_digest(namespace, set, digest)

  @doc """
  Reads `key` from `cluster`.

  The driver currently supports only `bins: :all`. Named-bin reads remain
  out of scope until the unary command surface proves they fall out of
  the same request/parse contract without extra public API work.

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

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec get(cluster(), Key.key_input(), :all, keyword()) ::
          {:ok, Aerospike.Record.t()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def get(cluster, key, bins \\ :all, opts \\ [])

  def get(cluster, key, bins, opts) do
    with {:ok, key} <- coerce_key(key) do
      Get.execute(cluster, key, bins, opts)
    end
  end

  @doc """
  Reads multiple `keys` from `cluster` in one batch request per target node.

  The result list stays in the same order as `keys`. Each list item is
  either `{:ok, %Aerospike.Record{}}` for a hit or an indexed error for
  that key (`{:error, %Aerospike.Error{}}`, `{:error, :no_master}`, or
  `{:error, :unknown_node}`).

  The driver currently supports only `bins: :all` and only `:timeout` in
  `opts`. Retry-driven regrouping stays disabled until the batch reroute
  path can honestly split a failed grouped request back across nodes.

  Accepts `%Aerospike.Key{}` values or `{namespace, set, user_key}` tuples.
  """
  @spec batch_get(cluster(), [Key.key_input()], :all, keyword()) ::
          {:ok,
           [
             {:ok, Aerospike.Record.t()}
             | {:error, Aerospike.Error.t()}
             | {:error, :no_master | :unknown_node}
           ]}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready}
  def batch_get(cluster, keys, bins \\ :all, opts \\ [])

  def batch_get(cluster, keys, bins, opts) when is_list(keys) do
    with {:ok, keys} <- coerce_keys(keys) do
      BatchGet.execute(cluster, keys, bins, opts)
    end
  end

  @doc """
  Returns a lazy `Stream` of records from a scan.

  The returned stream is lazy at the Enumerable boundary, but the current
  runtime drains each node response fully before yielding that node's
  records downstream. It does not promise frame-by-frame backpressure or
  an explicit cancellation API. Node-targeted scans pass `node: node_name`
  in `opts`.
  """
  @spec scan_stream(cluster(), Scan.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Aerospike.Error.t()}
  def scan_stream(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    ScanOps.stream(cluster, scan, opts)
  end

  @doc """
  Same as `scan_stream/3` but raises on error.
  """
  @spec scan_stream!(cluster(), Scan.t(), keyword()) :: Enumerable.t()
  def scan_stream!(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    case scan_stream(cluster, scan, opts) do
      {:ok, stream} -> stream
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @deprecated "Use scan_stream!/3 instead."
  def stream!(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    scan_stream!(cluster, scan, opts)
  end

  @doc """
  Returns a lazy `Stream` of records from a secondary-index query.

  Like `scan_stream/3`, this is lazy only at the outer Enumerable boundary.
  The current runtime buffers each node's query results before yielding
  them to the caller. Node-targeted queries pass `node: node_name` in `opts`.
  """
  @spec query_stream(cluster(), Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Aerospike.Error.t()}
  def query_stream(cluster, %Query{} = query, opts \\ []) when is_list(opts) do
    ScanOps.query_stream(cluster, query, opts)
  end

  @doc """
  Same as `query_stream/3` but raises on error.
  """
  @spec query_stream!(cluster(), Query.t(), keyword()) :: Enumerable.t()
  def query_stream!(cluster, %Query{} = query, opts \\ []) when is_list(opts) do
    case query_stream(cluster, query, opts) do
      {:ok, stream} -> stream
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Eagerly collects scan records into a list.

  Node-targeted scans pass `node: node_name` in `opts`.
  """
  @spec scan_all(cluster(), Scan.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Aerospike.Error.t()}
  def scan_all(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    ScanOps.all(cluster, scan, opts)
  end

  @deprecated "Use scan_all/3 instead."
  def all(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    scan_all(cluster, scan, opts)
  end

  @doc """
  Eagerly collects query records into a list.

  `query.max_records` must be set because this helper advances through
  the query in repeated page-sized steps until the cursor is exhausted.
  Node-targeted queries pass `node: node_name` in `opts`.
  """
  @spec query_all(cluster(), Query.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Aerospike.Error.t()}
  def query_all(cluster, %Query{} = query, opts \\ []) when is_list(opts) do
    ScanOps.query_all(cluster, query, opts)
  end

  @doc """
  Same as `query_all/3` but returns the list or raises `Aerospike.Error`.
  """
  @spec query_all!(cluster(), Query.t(), keyword()) :: [Aerospike.Record.t()]
  def query_all!(cluster, %Query{} = query, opts \\ []) when is_list(opts) do
    case query_all(cluster, query, opts) do
      {:ok, records} -> records
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Same as `scan_all/3` but returns the list or raises `Aerospike.Error`.
  """
  @spec scan_all!(cluster(), Scan.t(), keyword()) :: [Aerospike.Record.t()]
  def scan_all!(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    case scan_all(cluster, scan, opts) do
      {:ok, records} -> records
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @deprecated "Use scan_all!/3 instead."
  def all!(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    scan_all!(cluster, scan, opts)
  end

  @doc """
  Counts scan matches without materializing the records.

  Node-targeted scans pass `node: node_name` in `opts`.
  """
  @spec scan_count(cluster(), Scan.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Aerospike.Error.t()}
  def scan_count(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    ScanOps.count(cluster, scan, opts)
  end

  @deprecated "Use scan_count/3 instead."
  def count(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    scan_count(cluster, scan, opts)
  end

  @doc """
  Counts query matches without materializing the records.

  This still walks the query stream and counts client-side. It is not a
  separate server-side count primitive. Node-targeted queries pass
  `node: node_name` in `opts`.
  """
  @spec query_count(cluster(), Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Aerospike.Error.t()}
  def query_count(cluster, %Query{} = query, opts \\ []) when is_list(opts) do
    ScanOps.query_count(cluster, query, opts)
  end

  @doc """
  Same as `query_count/3` but returns the count or raises `Aerospike.Error`.
  """
  @spec query_count!(cluster(), Query.t(), keyword()) :: non_neg_integer()
  def query_count!(cluster, %Query{} = query, opts \\ []) when is_list(opts) do
    case query_count(cluster, query, opts) do
      {:ok, count} -> count
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Creates a secondary index and returns a pollable task handle.
  """
  @spec create_index(cluster(), String.t(), String.t(), keyword()) ::
          {:ok, IndexTask.t()} | {:error, Aerospike.Error.t()}
  def create_index(cluster, namespace, set, opts \\ [])
      when is_binary(namespace) and is_binary(set) and is_list(opts) do
    Admin.create_index(cluster, namespace, set, opts)
  end

  @doc """
  Drops a secondary index.
  """
  @spec drop_index(cluster(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def drop_index(cluster, namespace, index_name, opts \\ [])
      when is_binary(namespace) and is_binary(index_name) and is_list(opts) do
    Admin.drop_index(cluster, namespace, index_name, opts)
  end

  @doc """
  Returns one collected query page and a resumable cursor when more
  records remain.

  `query.max_records` is required because it seeds the partition-tracker
  budget for the page walk. On multi-node queries that budget is
  distributed across active nodes, so a page is resumable but not
  guaranteed to contain exactly `query.max_records` records. The cursor
  resumes partition progress from the prior page; it is not a stable
  snapshot token. Node-targeted queries pass `node: node_name` in `opts`.
  """
  @spec query_page(cluster(), Query.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Aerospike.Error.t()}
  def query_page(cluster, %Query{} = query, opts \\ []) when is_list(opts) do
    ScanOps.query_page(cluster, query, opts)
  end

  @doc """
  Same as `query_page/3` but returns the page or raises `Aerospike.Error`.
  """
  @spec query_page!(cluster(), Query.t(), keyword()) :: Page.t()
  def query_page!(cluster, %Query{} = query, opts \\ []) when is_list(opts) do
    case query_page(cluster, query, opts) do
      {:ok, page} -> page
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Streams aggregate query values over the same node-buffered query
  runtime used by `query_stream/3`.
  """
  @spec query_aggregate(cluster(), Query.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Aerospike.Error.t()}
  def query_aggregate(cluster, %Query{} = query, package, function, args, opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    ScanOps.query_aggregate(cluster, query, package, function, args, opts)
  end

  @doc """
  Starts a background query write job that applies the given operations.

  This returns a pollable task handle, not a resumable record stream.
  Node-targeted jobs pass `node: node_name` in `opts`.
  """
  @spec query_execute(cluster(), Query.t(), list(), keyword()) ::
          {:ok, ExecuteTask.t()} | {:error, Aerospike.Error.t()}
  def query_execute(cluster, %Query{} = query, ops, opts \\ [])
      when is_list(ops) and is_list(opts) do
    ScanOps.query_execute(cluster, query, ops, opts)
  end

  @doc """
  Starts a background query UDF job.

  This returns a pollable task handle, not a resumable record stream.
  Node-targeted jobs pass `node: node_name` in `opts`.
  """
  @spec query_udf(cluster(), Query.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, ExecuteTask.t()} | {:error, Aerospike.Error.t()}
  def query_udf(cluster, %Query{} = query, package, function, args, opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    ScanOps.query_udf(cluster, query, package, function, args, opts)
  end

  @doc """
  Commits a transaction on the named cluster `cluster`.

  This only works for a transaction handle whose tracking row is already
  initialized on `cluster`. A fresh `%Aerospike.Txn{}` is not enough by itself.
  In the current spike, public code initializes that runtime state only when
  `transaction/2` or `transaction/3` enters its callback. Transaction
  tracking is keyed off the started cluster name, so this helper currently
  requires that registered atom.
  """
  @spec commit(named_cluster(), Txn.t()) ::
          {:ok, :committed | :already_committed} | {:error, Aerospike.Error.t()}
  def commit(cluster, %Txn{} = txn) when is_atom(cluster) do
    TxnRoll.commit(cluster, txn, [])
  end

  @doc """
  Aborts a transaction on the named cluster `cluster`.

  Like `commit/2`, this requires a handle with initialized runtime tracking on
  `cluster`. It is for an already-open transaction; it does not create one,
  and it currently requires the registered cluster atom.
  """
  @spec abort(named_cluster(), Txn.t()) ::
          {:ok, :aborted | :already_aborted} | {:error, Aerospike.Error.t()}
  def abort(cluster, %Txn{} = txn) when is_atom(cluster) do
    TxnRoll.abort(cluster, txn, [])
  end

  @doc """
  Returns the current state of a transaction on the named cluster `cluster`.

  This reflects only the in-flight states backed by the runtime tracking row.
  After commit or abort, the spike cleans that row up, so `txn_status/2`
  returns an error instead of a terminal `:committed` or `:aborted` state.
  Like the other transaction lifecycle helpers, this currently requires the
  registered cluster atom.
  """
  @spec txn_status(named_cluster(), Txn.t()) ::
          {:ok, :open | :verified | :committed | :aborted}
          | {:error, Aerospike.Error.t()}
  def txn_status(cluster, %Txn{} = txn) when is_atom(cluster) do
    TxnRoll.txn_status(cluster, txn)
  end

  @doc """
  Runs a function within a new transaction on the named cluster `cluster`.

  The callback owns the public transaction lifecycle. The spike initializes the
  runtime tracking row before invoking `fun`, then commits on success or aborts
  on any failure path. Do not call `commit/2` or `abort/2` from inside the
  callback.

  The `%Aerospike.Txn{}` passed to `fun` is safe only for sequential use within
  that transaction. Do not share it across concurrent processes, and do not use
  scans or queries with it; the current transaction proof covers only
  transaction-aware single-record commands. This helper currently requires the
  registered cluster atom.
  """
  @spec transaction(named_cluster(), (Txn.t() -> term())) ::
          {:ok, term()} | {:error, Aerospike.Error.t()}
  def transaction(cluster, fun) when is_atom(cluster) and is_function(fun, 1) do
    TxnRoll.transaction(cluster, [], fun)
  end

  @doc """
  Runs a function within a transaction on the named cluster `cluster` using a
  provided handle or options.

  When `txn_or_opts` is a `%Aerospike.Txn{}`, the spike initializes fresh
  runtime tracking for that handle on `cluster` at callback entry. Reusing the
  same handle concurrently or against another cluster is unsupported. This
  helper currently requires the registered cluster atom.
  """
  @spec transaction(named_cluster(), Txn.t() | keyword(), (Txn.t() -> term())) ::
          {:ok, term()} | {:error, Aerospike.Error.t()}
  def transaction(cluster, txn_or_opts, fun)
      when is_atom(cluster) and is_function(fun, 1) do
    TxnRoll.transaction(cluster, txn_or_opts, fun)
  end

  @doc """
  Same as `scan_count/3` but returns the count or raises `Aerospike.Error`.
  """
  @spec scan_count!(cluster(), Scan.t(), keyword()) :: non_neg_integer()
  def scan_count!(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    case scan_count(cluster, scan, opts) do
      {:ok, count} -> count
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @deprecated "Use scan_count!/3 instead."
  def count!(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    scan_count!(cluster, scan, opts)
  end

  @doc """
  Writes `bins` for `key` to `cluster`.

  The spike accepts only a non-empty bin map and only the narrow value
  subset supported by `Aerospike.Protocol.AsmMsg.Value`. Supported write
  opts are:

    * `:timeout`
    * `:max_retries`
    * `:sleep_between_retries_ms`
    * `:ttl`
    * `:generation` — expect generation equality when non-zero

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec put(cluster(), Key.key_input(), Aerospike.Record.bins_input(), keyword()) ::
          {:ok, Aerospike.Record.metadata()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def put(cluster, key, bins, opts \\ []) do
    with {:ok, key} <- coerce_key(key) do
      Put.execute(cluster, key, bins, opts)
    end
  end

  @doc """
  Returns whether `key` exists in `cluster` without reading bins.

  Supported read opts are `:timeout`, `:max_retries`,
  `:sleep_between_retries_ms`, and `:replica_policy`.

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec exists(cluster(), Key.key_input(), keyword()) ::
          {:ok, boolean()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def exists(cluster, key, opts \\ []) do
    with {:ok, key} <- coerce_key(key) do
      Exists.execute(cluster, key, opts)
    end
  end

  @doc """
  Updates `key`'s header metadata in `cluster`.

  Supported write opts are `:timeout`, `:max_retries`,
  `:sleep_between_retries_ms`, `:ttl`, and `:generation`.

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec touch(cluster(), Key.key_input(), keyword()) ::
          {:ok, Aerospike.Record.metadata()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def touch(cluster, key, opts \\ []) do
    with {:ok, key} <- coerce_key(key) do
      Touch.execute(cluster, key, opts)
    end
  end

  @doc """
  Deletes `key` from `cluster`.

  Returns `{:ok, true}` when a record was deleted and `{:ok, false}`
  when the key was already absent. Supported write opts are `:timeout`,
  `:max_retries`, `:sleep_between_retries_ms`, and `:generation`.

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec delete(cluster(), Key.key_input(), keyword()) ::
          {:ok, boolean()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def delete(cluster, key, opts \\ []) do
    with {:ok, key} <- coerce_key(key) do
      Delete.execute(cluster, key, opts)
    end
  end

  @doc """
  Runs a constrained unary operate list for `key`.

  Supported operations:

    * `{:write, bin, value}` — simple bin write
    * `{:read, bin}` — simple bin read
    * `{:add, bin, delta}` — numeric increment
    * `{:append, bin, suffix}` — string suffix mutation
    * `{:prepend, bin, prefix}` — string prefix mutation
    * `:touch` — refresh record metadata
    * `:delete` — remove the record

  The command routes per input batch: read-only lists use read routing;
  any list that includes a write uses write routing.

  Supported opts are `:timeout`, `:max_retries`,
  `:sleep_between_retries_ms`, `:ttl`, and `:generation`.

  Accepted operations include the simple tuple form plus the public
  `Aerospike.Op` helpers for primitive and CDT-style operations.

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec operate(cluster(), Key.key_input(), [Operate.operation_input()], keyword()) ::
          {:ok, Aerospike.Record.t()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def operate(cluster, key, operations, opts \\ []) do
    with {:ok, key} <- coerce_key(key) do
      Operate.execute(cluster, key, operations, opts)
    end
  end

  defp coerce_key(key) do
    {:ok, Key.coerce!(key)}
  rescue
    err in ArgumentError ->
      {:error, Error.from_result_code(:invalid_argument, message: err.message)}
  end

  defp coerce_keys(keys) do
    {:ok, Enum.map(keys, &Key.coerce!/1)}
  rescue
    err in ArgumentError ->
      {:error, Error.from_result_code(:invalid_argument, message: err.message)}
  end
end

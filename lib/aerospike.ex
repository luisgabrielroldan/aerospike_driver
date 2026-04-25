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
    * `get_header/2` reads only record metadata for a key
    * `put/4` writes a bin map
    * `put_payload/4` sends a caller-built single-record write/delete frame
    * `apply_udf/6` executes one record UDF against one key
    * `register_udf/3`, `register_udf/4`, `remove_udf/2`,
      `remove_udf/3`, and `list_udfs/1` / `list_udfs/2` manage
      server-side UDF packages and return explicit metadata/task
      handles
    * `truncate/2`, `truncate/3`, and `truncate/4` expose one-node
      operator truncation helpers with explicit namespace and set forms
    * `create_user/5`, `create_pki_user/4`, `drop_user/3`,
      `change_password/4`, `grant_roles/4`, `revoke_roles/4`, `query_user/3`,
      `query_users/2`, `create_role/4`, `drop_role/3`,
      `set_whitelist/4`, `set_quotas/5`, `grant_privileges/4`,
      `revoke_privileges/4`, `query_role/3`, and `query_roles/2` expose the
      enterprise security-admin seam
    * `add/4`, `append/4`, and `prepend/4` expose thin unary write
      helpers for common counter and string mutations
    * `metrics_enabled?/1`, `enable_metrics/2`, `disable_metrics/1`,
      `stats/1`, and `warm_up/2` expose opt-in runtime metrics and an
      explicit operator pool probe over the already-started workers
    * `touch/2` updates record metadata
    * `delete/2` removes a record
    * `exists/2` performs a header-only existence probe
    * `operate/4` runs simple, CDT-style, and expression operation lists built
      with `Aerospike.Op`, `Aerospike.Op.List`, `Aerospike.Op.Map`,
      `Aerospike.Op.Exp`, and `Aerospike.Ctx`
    * unary commands (`get/3`, `get_header/2`, `put/4`, `exists/2`,
      `touch/2`, `delete/2`, `operate/4`, `apply_udf/6`, `add/4`,
      `append/4`, and `prepend/4`) accept `%Aerospike.Exp{}` via `:filter`
      for server-side execution filtering
    * `Aerospike.Exp` builds server-side expression values, including values
      usable as expression-backed secondary-index sources
    * `create_expression_index/5` creates expression-backed secondary indexes
      on servers that support them and returns a pollable index task
    * `batch_get/4`, `batch_get_header/3`, `batch_exists/3`,
      `batch_get_operate/4`, `batch_delete/3`, and `batch_udf/6` operate on
      multiple keys and return per-key results in caller order
    * `Aerospike.Batch` and `batch_operate/3` expose a curated heterogeneous
      batch surface for mixing reads, writes, deletes, operations, and record
      UDF calls while returning one `%Aerospike.BatchResult{}` per input
    * `child_spec/1`, `close/2`, `key/3`, and `key_digest/3` round out
      the root lifecycle and key-construction boundary
    * `info/3`, `nodes/1`, and `node_names/1` expose one-node operator
      reads over the published cluster view
    * `set_xdr_filter/4` sets or clears Enterprise XDR expression filters
      through a one-node info command
    * `query_stream!/3`, `query_all/3`, `query_count/3`, and
      `query_aggregate/6` run secondary-index queries through the same
      node-preparation pipeline, with lazy outer streams but
      node-buffered record delivery
      and optional `%Aerospike.Exp{}` filters through `Query.filter/2`
      when a secondary-index predicate from `Query.where/2` is also used.
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

  Additional expression-builder families and the wider policy surface remain
  out of scope until supported command paths prove them.

  Caller-facing policy validation and default materialization now lives
  under `Aerospike.Policy`. Public command functions still accept
  keyword opts, but command paths no longer merge or validate those
  policy families ad hoc.
  """

  alias Aerospike.Batch
  alias Aerospike.Cluster
  alias Aerospike.Cluster.Supervisor, as: ClusterSupervisor
  alias Aerospike.Command.Admin
  alias Aerospike.Command.ApplyUdf
  alias Aerospike.Command.Batch, as: MixedBatch
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Command.BatchDelete
  alias Aerospike.Command.BatchGet
  alias Aerospike.Command.BatchUdf
  alias Aerospike.Command.Delete
  alias Aerospike.Command.Exists
  alias Aerospike.Command.Get
  alias Aerospike.Command.Operate
  alias Aerospike.Command.Put
  alias Aerospike.Command.PutPayload
  alias Aerospike.Command.ScanOps
  alias Aerospike.Command.Touch
  alias Aerospike.Command.WriteOp
  alias Aerospike.Error
  alias Aerospike.ExecuteTask
  alias Aerospike.Exp
  alias Aerospike.IndexTask
  alias Aerospike.Key
  alias Aerospike.Page
  alias Aerospike.Policy
  alias Aerospike.Privilege
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.OperateFlags
  alias Aerospike.Query
  alias Aerospike.RegisterTask
  alias Aerospike.Role
  alias Aerospike.Runtime.TxnRoll
  alias Aerospike.RuntimeMetrics
  alias Aerospike.Scan
  alias Aerospike.Txn
  alias Aerospike.UDF
  alias Aerospike.User

  @typedoc """
  Identifier for a running cluster facade.

  Read-side helpers accept the registered cluster name or a pid
  registered under that name. Arbitrary `GenServer.server()` forms are
  not supported.
  """
  @type cluster :: named_cluster() | pid()

  @typedoc """
  Registered atom name for a running cluster.

  Lifecycle and transaction helpers resolve supervisor and ETS resources from
  this name, so they do not currently accept arbitrary `GenServer.server()`
  identities.
  """
  @type named_cluster :: atom()

  @typedoc """
  One active cluster node as returned by `nodes/1`.
  """
  @type node_info :: %{name: String.t(), host: String.t(), port: :inet.port_number()}

  @typedoc "Security user metadata returned by `query_user/3` and `query_users/2`."
  @type user_info :: User.t()

  @typedoc "Security role metadata returned by `query_role/3` and `query_roles/2`."
  @type role_info :: Role.t()

  @typedoc "Security privilege metadata used by role administration APIs."
  @type privilege :: Privilege.t()

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
  from the first tend cycle or pool worker. The `:name` remains the
  public cluster identity for facade calls such as `get/3`, `info/3`,
  and `Aerospike.Cluster.ready?/1`.

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
    * `:filter` — non-empty `%Aerospike.Exp{}` server-side filter
      expression, or `nil` for no filter.

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
  Reads only record metadata for `key` from `cluster`.

  This is the explicit single-record header helper. It reuses the same unary
  read path as `get/3`, but requests only generation/TTL metadata and returns
  a `%Aerospike.Record{}` with `bins: %{}` on hit.

  Options:

    * `:timeout` — total op-budget milliseconds for the call, shared
      across the initial send and every retry. Default `5_000`.
    * `:max_retries` — overrides the cluster-default retry cap for this
      call. `0` disables retry entirely. See `Aerospike.RetryPolicy`.
    * `:sleep_between_retries_ms` — fixed delay between retry attempts.
    * `:replica_policy` — `:master` (all attempts against the master)
      or `:sequence` (walk the replica list by attempt index).
    * `:filter` — non-empty `%Aerospike.Exp{}` server-side filter
      expression, or `nil` for no filter.

  Returns `{:ok, %Aerospike.Record{bins: %{}}}` on hit,
  `{:error, %Aerospike.Error{code: :key_not_found}}` on miss, or a
  routing atom (`:cluster_not_ready`, `:no_master`, `:unknown_node`)
  when the cluster view cannot serve the request.

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec get_header(cluster(), Key.key_input(), keyword()) ::
          {:ok, Aerospike.Record.t()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def get_header(cluster, key, opts \\ [])

  def get_header(cluster, key, opts) when is_list(opts) do
    with {:ok, key} <- coerce_key(key) do
      Get.execute(cluster, key, :header, opts)
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
  Reads record headers for multiple `keys` from `cluster`.

  The result list stays in the same order as `keys`. Each list item is
  either `{:ok, %Aerospike.Record{bins: %{}}}` for a hit or an indexed
  error for that key (`{:error, %Aerospike.Error{}}`,
  `{:error, :no_master}`, or `{:error, :unknown_node}`).

  This helper keeps the same narrow batch policy surface as `batch_get/4`:
  only `:timeout` is currently accepted in `opts`.

  Accepts `%Aerospike.Key{}` values or `{namespace, set, user_key}` tuples.
  """
  @spec batch_get_header(cluster(), [Key.key_input()], keyword()) ::
          {:ok,
           [
             {:ok, Aerospike.Record.t()}
             | {:error, Aerospike.Error.t()}
             | {:error, :no_master | :unknown_node}
           ]}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready}
  def batch_get_header(cluster, keys, opts \\ [])

  def batch_get_header(cluster, keys, opts) when is_list(keys) and is_list(opts) do
    with {:ok, keys} <- coerce_keys(keys) do
      BatchGet.execute(cluster, keys, :header, opts)
    end
  end

  @doc """
  Checks existence for multiple `keys` from `cluster`.

  The result list stays in the same order as `keys`. Each list item is
  either `{:ok, true}` / `{:ok, false}` for the targeted key or an
  indexed error for that key (`{:error, %Aerospike.Error{}}`,
  `{:error, :no_master}`, or `{:error, :unknown_node}`).

  This helper keeps the same narrow batch policy surface as `batch_get/4`:
  only `:timeout` is currently accepted in `opts`.

  Accepts `%Aerospike.Key{}` values or `{namespace, set, user_key}` tuples.
  """
  @spec batch_exists(cluster(), [Key.key_input()], keyword()) ::
          {:ok,
           [
             {:ok, boolean()}
             | {:error, Aerospike.Error.t()}
             | {:error, :no_master | :unknown_node}
           ]}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready}
  def batch_exists(cluster, keys, opts \\ [])

  def batch_exists(cluster, keys, opts) when is_list(keys) and is_list(opts) do
    with {:ok, keys} <- coerce_keys(keys) do
      BatchGet.execute(cluster, keys, :exists, opts)
    end
  end

  @doc """
  Runs one read-only operation list for multiple `keys` from `cluster`.

  The result list stays in the same order as `keys`. Each list item is
  either `{:ok, %Aerospike.Record{}}` for a hit or an indexed error for
  that key (`{:error, %Aerospike.Error{}}`, `{:error, :no_master}`, or
  `{:error, :unknown_node}`). Missing keys remain explicit per-key
  errors; this helper does not collapse misses to `nil`.

  This helper keeps the same narrow batch policy surface as `batch_get/4`:
  only `:timeout` is currently accepted in `opts`.

  Accepts `%Aerospike.Key{}` values or `{namespace, set, user_key}` tuples.
  """
  @spec batch_get_operate(cluster(), [Key.key_input()], [Aerospike.Op.t()], keyword()) ::
          {:ok,
           [
             {:ok, Aerospike.Record.t()}
             | {:error, Aerospike.Error.t()}
             | {:error, :no_master | :unknown_node}
           ]}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready}
  def batch_get_operate(cluster, keys, operations, opts \\ [])

  def batch_get_operate(cluster, keys, operations, opts)
      when is_list(keys) and is_list(operations) and is_list(opts) do
    with {:ok, keys} <- coerce_keys(keys) do
      BatchGet.execute_operate(cluster, keys, operations, opts)
    end
  end

  @doc """
  Deletes multiple `keys` from `cluster`.

  The result list stays in the same order as `keys`. Each list item is a
  `%Aerospike.BatchResult{}` with `status: :ok` for a deleted record or
  `status: :error` for a per-key failure such as a missing key, routing
  failure, or node transport error. Missing keys remain explicit error
  results with the server result code; this helper does not collapse them to
  booleans.

  This helper keeps the same narrow batch policy surface as `batch_get/4`:
  only `:timeout` is currently accepted in `opts`.

  Accepts `%Aerospike.Key{}` values or `{namespace, set, user_key}` tuples.
  """
  @spec batch_delete(cluster(), [Key.key_input()], keyword()) ::
          {:ok, [Aerospike.BatchResult.t()]}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready}
  def batch_delete(cluster, keys, opts \\ [])

  def batch_delete(cluster, keys, opts) when is_list(keys) and is_list(opts) do
    with {:ok, keys} <- coerce_keys(keys) do
      BatchDelete.execute(cluster, keys, opts)
    end
  end

  @doc """
  Executes one record UDF for multiple `keys` from `cluster`.

  The result list stays in the same order as `keys`. Each list item is a
  `%Aerospike.BatchResult{}` with `status: :ok` for a successful UDF row or
  `status: :error` for a per-key failure such as a missing key, missing UDF,
  routing failure, or node transport error. Successful UDF rows may include a
  returned `%Aerospike.Record{}` when the server sends return bins.

  This helper keeps the same narrow batch policy surface as `batch_get/4`:
  only `:timeout` is currently accepted in `opts`.

  Accepts `%Aerospike.Key{}` values or `{namespace, set, user_key}` tuples.
  """
  @spec batch_udf(cluster(), [Key.key_input()], String.t(), String.t(), list(), keyword()) ::
          {:ok, [Aerospike.BatchResult.t()]}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready}
  def batch_udf(cluster, keys, package, function, args, opts \\ [])

  def batch_udf(cluster, keys, package, function, args, opts)
      when is_list(keys) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    with {:ok, keys} <- coerce_keys(keys) do
      BatchUdf.execute(cluster, keys, package, function, args, opts)
    end
  end

  @doc """
  Executes heterogeneous batch entries built with `Aerospike.Batch`.

  The result list stays in the same order as the input entries. Each list item
  is a `%Aerospike.BatchResult{}` with `status: :ok` for a successful row or
  `status: :error` for a per-key failure such as a missing key, routing
  failure, server error, or node transport error.

  The current batch policy surface is intentionally narrow: only `:timeout` is
  accepted in `opts`. Per-entry write policies are not exposed.
  """
  @spec batch_operate(cluster(), [Batch.t()], keyword()) ::
          {:ok, [Aerospike.BatchResult.t()]}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready}
  def batch_operate(cluster, entries, opts \\ [])

  def batch_operate(_cluster, [], opts) when is_list(opts) do
    with {:ok, _policy} <- Policy.batch(opts) do
      {:ok, []}
    end
  end

  def batch_operate(cluster, entries, opts) when is_list(entries) and is_list(opts) do
    with {:ok, _validated} <- Policy.batch(opts),
         {:ok, policy} <- batch_policy(cluster, opts),
         {:ok, command_entries} <- batch_command_entries(entries, policy),
         {:ok, results} <- MixedBatch.execute(cluster, command_entries, opts) do
      {:ok, MixedBatch.to_public_results(results)}
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
  Sends one info command to one active cluster node and returns that node's reply.
  """
  @spec info(cluster(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, Aerospike.Error.t()}
  def info(cluster, command, opts \\ [])
      when is_binary(command) and is_list(opts) do
    Admin.info(cluster, command, opts)
  end

  @doc """
  Returns the published active cluster nodes with their direct-connect host and port.
  """
  @spec nodes(cluster()) :: {:ok, [node_info()]}
  def nodes(cluster) do
    {:ok, Cluster.nodes(cluster)}
  end

  @doc """
  Returns the published active cluster node-name snapshot.
  """
  @spec node_names(cluster()) :: {:ok, [String.t()]}
  def node_names(cluster) do
    {:ok, Cluster.node_names(cluster)}
  end

  @doc """
  Returns whether internal runtime metrics are enabled for `cluster`.

  Metrics are opt-in. The collector is initialized at cluster start so the
  config rows exist early, but command, pool, and tender counters remain idle
  until `enable_metrics/2` is called.
  """
  @spec metrics_enabled?(cluster()) :: boolean()
  def metrics_enabled?(cluster) do
    RuntimeMetrics.metrics_enabled?(cluster)
  end

  @doc """
  Enables internal runtime metrics for `cluster`.

  Supported options:

    * `:reset` — boolean. When `true`, clears the existing runtime counters
      before enabling collection.
  """
  @spec enable_metrics(cluster(), keyword()) :: :ok | {:error, Aerospike.Error.t()}
  def enable_metrics(cluster, opts \\ []) when is_list(opts) do
    with :ok <- validate_enable_metrics_opts(opts) do
      RuntimeMetrics.enable(cluster, opts)
    end
  end

  @doc """
  Disables internal runtime metrics for `cluster`.

  Counter rows remain in place so `stats/1` still reports the last collected
  values until metrics are re-enabled or reset.
  """
  @spec disable_metrics(cluster()) :: :ok | {:error, Aerospike.Error.t()}
  def disable_metrics(cluster) do
    RuntimeMetrics.disable(cluster)
  end

  @doc """
  Returns the current internal runtime metrics snapshot for `cluster`.

  The returned map is intentionally limited to the counters and cluster
  metadata the runtime collector actually records today. It is not a
  generalized exporter surface.
  """
  @spec stats(cluster()) :: map()
  def stats(cluster) do
    RuntimeMetrics.stats(cluster)
  end

  @doc """
  Verifies that the active node pools can serve checkouts through the normal path.

  This is an explicit operator action; it does not toggle metrics and it does
  not change the pool startup mode. The current spike pools are already eager at
  cluster start. `warm_up/2` simply proves that the active pools can hand out
  the requested number of workers right now.

  Supported options:

    * `:count` — non-negative integer. `0` (default) means "up to the configured
      pool size per active node".
    * `:pool_checkout_timeout` — non-negative integer timeout in milliseconds
      for each checkout probe.
  """
  @spec warm_up(cluster(), keyword()) :: {:ok, map()} | {:error, Aerospike.Error.t()}
  def warm_up(cluster, opts \\ []) when is_list(opts) do
    with :ok <- validate_warm_up_opts(opts) do
      Cluster.warm_up(cluster, opts)
    end
  end

  @doc """
  Creates a secondary index and returns a pollable task handle.

  Required options:

    * `:bin` — non-empty bin name.
    * `:name` — non-empty index name.
    * `:type` — one of `:numeric`, `:string`, or `:geo2dsphere`.

  Optional options:

    * `:collection` — one of `:list`, `:mapkeys`, or `:mapvalues`.
    * `:pool_checkout_timeout` — non-negative pool checkout timeout in
      milliseconds.

  Geo indexes use `type: :geo2dsphere` and can be queried with
  `Aerospike.Filter.geo_within/2` or `Aerospike.Filter.geo_contains/2`.

      {:ok, task} =
        Aerospike.create_index(cluster, "test", "places",
          bin: "loc",
          name: "places_loc_geo_idx",
          type: :geo2dsphere
        )

      :ok = Aerospike.IndexTask.wait(task)
  """
  @spec create_index(cluster(), String.t(), String.t(), keyword()) ::
          {:ok, IndexTask.t()} | {:error, Aerospike.Error.t()}
  def create_index(cluster, namespace, set, opts \\ [])
      when is_binary(namespace) and is_binary(set) and is_list(opts) do
    Admin.create_index(cluster, namespace, set, opts)
  end

  @doc """
  Creates an expression-backed secondary index and returns a pollable task
  handle.

  Required options:

    * `:name` — non-empty index name.
    * `:type` — one of `:numeric`, `:string`, or `:geo2dsphere`.

  Optional options:

    * `:collection` — one of `:list`, `:mapkeys`, or `:mapvalues`.
    * `:pool_checkout_timeout` — non-negative pool checkout timeout in
      milliseconds.

  The source must be a `%Aerospike.Exp{}` with non-empty wire bytes. Expression
  indexes use the expression as the source and therefore do not accept `:bin`.
  Servers older than Aerospike 8.1 reject expression-backed index creation
  before the create command is sent.

      {:ok, task} =
        Aerospike.create_expression_index(cluster, "test", "users", Exp.int_bin("age"),
          name: "users_age_expr_idx",
          type: :numeric
        )

      :ok = Aerospike.IndexTask.wait(task)
  """
  @spec create_expression_index(cluster(), String.t(), String.t(), Exp.t(), keyword()) ::
          {:ok, IndexTask.t()} | {:error, Aerospike.Error.t()}
  def create_expression_index(cluster, namespace, set, %Exp{} = expression, opts \\ [])
      when is_binary(namespace) and is_binary(set) and is_list(opts) do
    with {:ok, _policy} <- Policy.expression_index_create(expression, opts) do
      Admin.create_expression_index(cluster, namespace, set, expression, opts)
    end
  end

  @doc """
  Sets or clears the Enterprise XDR filter for one datacenter and namespace.

  Pass a non-empty `%Aerospike.Exp{}` to set the filter, or `nil` to clear the
  current filter. `datacenter` and `namespace` must be non-empty info-command
  identifiers and cannot contain command delimiters.

  Live application requires an Enterprise server with XDR configured. Community
  Edition or unconfigured clusters may reject the command after local
  validation.

      filter = Exp.eq(Exp.int_bin("active"), Exp.int(1))
      :ok = Aerospike.set_xdr_filter(cluster, "dc-west", "test", filter)
      :ok = Aerospike.set_xdr_filter(cluster, "dc-west", "test", nil)
  """
  @spec set_xdr_filter(cluster(), String.t(), String.t(), Exp.t() | nil) ::
          :ok | {:error, Aerospike.Error.t()}
  def set_xdr_filter(cluster, datacenter, namespace, filter)
      when is_binary(datacenter) and is_binary(namespace) do
    with :ok <- Policy.xdr_filter(datacenter, namespace, filter) do
      Admin.set_xdr_filter(cluster, datacenter, namespace, filter)
    end
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
  Lists the registered server-side UDF packages visible from one active node.

  This is package lifecycle state, not record execution. Use `apply_udf/6`
  to invoke one function against one key.
  """
  @spec list_udfs(cluster(), keyword()) :: {:ok, [UDF.t()]} | {:error, Aerospike.Error.t()}
  def list_udfs(cluster, opts \\ []) when is_list(opts) do
    Admin.list_udfs(cluster, opts)
  end

  @doc """
  Uploads a UDF package from inline source or a readable local `.lua` path.

  Returns a pollable `Aerospike.RegisterTask` once the server accepts the
  upload. The package may still be propagating, so call `RegisterTask.wait/2`
  before relying on it from `apply_udf/6` or background UDF jobs.
  """
  @spec register_udf(cluster(), String.t(), String.t(), keyword()) ::
          {:ok, RegisterTask.t()} | {:error, Aerospike.Error.t()}
  def register_udf(cluster, path_or_content, server_name, opts \\ [])
      when is_binary(path_or_content) and is_binary(server_name) and is_list(opts) do
    Admin.register_udf(cluster, path_or_content, server_name, opts)
  end

  @doc """
  Removes a registered UDF package by server filename.

  This is idempotent: removing an already-absent package still returns `:ok`.
  """
  @spec remove_udf(cluster(), String.t(), keyword()) :: :ok | {:error, Aerospike.Error.t()}
  def remove_udf(cluster, server_name, opts \\ [])
      when is_binary(server_name) and is_list(opts) do
    Admin.remove_udf(cluster, server_name, opts)
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
  Truncates all records in `namespace`.

  This sends one truncate info command through the shared admin seam. The
  server then distributes the truncate across the cluster.

  Options:

    * `:before` — `%DateTime{}` — truncate only records whose last-update
      time is older than the provided timestamp
    * `:pool_checkout_timeout` — non-negative integer checkout timeout in
      milliseconds for the one-node admin request
  """
  @spec truncate(cluster(), String.t(), keyword()) :: :ok | {:error, Aerospike.Error.t()}
  def truncate(cluster, namespace, opts \\ [])

  def truncate(cluster, namespace, set)
      when is_binary(namespace) and is_binary(set) do
    truncate(cluster, namespace, set, [])
  end

  def truncate(cluster, namespace, opts)
      when is_binary(namespace) and is_list(opts) do
    with {:ok, opts} <- validate_truncate_opts(opts, "Aerospike.truncate/3") do
      Admin.truncate(cluster, namespace, opts)
    end
  end

  @doc """
  Truncates all records in `namespace` and `set`.

  Like `truncate/3`, this uses the shared one-node admin info seam. The optional
  `:before` filter is forwarded to the server as a last-update cutoff.

  Options:

    * `:before` — `%DateTime{}` — truncate only records whose last-update
      time is older than the provided timestamp
    * `:pool_checkout_timeout` — non-negative integer checkout timeout in
      milliseconds for the one-node admin request
  """
  @spec truncate(cluster(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def truncate(cluster, namespace, set, opts)
      when is_binary(namespace) and is_binary(set) and is_list(opts) do
    with {:ok, opts} <- validate_truncate_opts(opts, "Aerospike.truncate/4") do
      Admin.truncate(cluster, namespace, set, opts)
    end
  end

  @doc """
  Creates a password-authenticated security user.

  This requires Aerospike Enterprise with security enabled and a cluster
  connection authenticated as a user that holds the `user-admin` privilege.

  Supported opts are:

    * `:timeout`
    * `:pool_checkout_timeout`
  """
  @spec create_user(cluster(), String.t(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def create_user(cluster, user_name, password, roles, opts \\ [])
      when is_binary(user_name) and is_binary(password) and is_list(roles) and is_list(opts) do
    with {:ok, role_names} <- validate_role_names(roles),
         {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.create_user(cluster, user_name, password, role_names, opts)
    end
  end

  @doc """
  Creates a PKI-authenticated security user.

  The user is created with a no-password credential and is intended for TLS
  certificate authentication. This requires Aerospike Enterprise with security
  enabled, server support for PKI users, and a cluster connection
  authenticated as a user that holds the `user-admin` privilege.

  Supported opts are:

    * `:timeout`
    * `:pool_checkout_timeout`
  """
  @spec create_pki_user(cluster(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def create_pki_user(cluster, user_name, roles, opts \\ [])
      when is_binary(user_name) and is_list(roles) and is_list(opts) do
    with {:ok, role_names} <- validate_role_names(roles),
         {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.create_pki_user(cluster, user_name, role_names, opts)
    end
  end

  @doc """
  Drops a security user.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec drop_user(cluster(), String.t(), keyword()) :: :ok | {:error, Aerospike.Error.t()}
  def drop_user(cluster, user_name, opts \\ [])
      when is_binary(user_name) and is_list(opts) do
    with {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.drop_user(cluster, user_name, opts)
    end
  end

  @doc """
  Changes a security user's password.

  When `user_name` matches the credentials configured on `cluster`, the driver
  uses the self-service password-change command and rotates the running
  cluster's in-memory credentials for future reconnects. For other users it
  uses the user-admin password-set command.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec change_password(cluster(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def change_password(cluster, user_name, password, opts \\ [])
      when is_binary(user_name) and is_binary(password) and is_list(opts) do
    with {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.change_password(cluster, user_name, password, opts)
    end
  end

  @doc """
  Grants roles to a security user.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec grant_roles(cluster(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def grant_roles(cluster, user_name, roles, opts \\ [])
      when is_binary(user_name) and is_list(roles) and is_list(opts) do
    with {:ok, role_names} <- validate_role_names(roles),
         {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.grant_roles(cluster, user_name, role_names, opts)
    end
  end

  @doc """
  Revokes roles from a security user.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec revoke_roles(cluster(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def revoke_roles(cluster, user_name, roles, opts \\ [])
      when is_binary(user_name) and is_list(roles) and is_list(opts) do
    with {:ok, role_names} <- validate_role_names(roles),
         {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.revoke_roles(cluster, user_name, role_names, opts)
    end
  end

  @doc """
  Queries one security user.

  Returns `{:ok, nil}` when the named user does not exist.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec query_user(cluster(), String.t(), keyword()) ::
          {:ok, user_info() | nil} | {:error, Aerospike.Error.t()}
  def query_user(cluster, user_name, opts \\ [])
      when is_binary(user_name) and is_list(opts) do
    with {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.query_user(cluster, user_name, opts)
    end
  end

  @doc """
  Queries all security users visible to the authenticated cluster user.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec query_users(cluster(), keyword()) :: {:ok, [user_info()]} | {:error, Aerospike.Error.t()}
  def query_users(cluster, opts \\ []) when is_list(opts) do
    with {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.query_users(cluster, opts)
    end
  end

  @doc """
  Creates a security role.

  This requires Aerospike Enterprise with security enabled.

  Supported opts are:

    * `:whitelist` — list of client address strings
    * `:read_quota` — non-negative integer operations-per-second limit
    * `:write_quota` — non-negative integer operations-per-second limit
    * `:timeout`
    * `:pool_checkout_timeout`
  """
  @spec create_role(cluster(), String.t(), [privilege()], keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def create_role(cluster, role_name, privileges, opts \\ [])
      when is_binary(role_name) and is_list(privileges) and is_list(opts) do
    with {:ok, privileges} <- validate_privileges(privileges),
         {:ok, {whitelist, read_quota, write_quota, call_opts}} <-
           validate_create_role_opts(opts),
         {:ok, _policy} <- Policy.security_admin(call_opts) do
      Admin.create_role(
        cluster,
        role_name,
        privileges,
        whitelist,
        read_quota,
        write_quota,
        call_opts
      )
    end
  end

  @doc """
  Drops a security role.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec drop_role(cluster(), String.t(), keyword()) :: :ok | {:error, Aerospike.Error.t()}
  def drop_role(cluster, role_name, opts \\ [])
      when is_binary(role_name) and is_list(opts) do
    with {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.drop_role(cluster, role_name, opts)
    end
  end

  @doc """
  Sets or clears a security role's client-address whitelist.

  Pass an empty list to clear the role's whitelist. This requires Aerospike
  Enterprise with security enabled.
  """
  @spec set_whitelist(cluster(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def set_whitelist(cluster, role_name, whitelist, opts \\ [])
      when is_binary(role_name) and is_list(opts) do
    with {:ok, whitelist} <- validate_role_whitelist_value(whitelist),
         {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.set_whitelist(cluster, role_name, whitelist, opts)
    end
  end

  @doc """
  Sets read and write quota limits for a security role.

  Pass `0` for either quota to clear that limit. Quotas require server security
  configuration with quotas enabled.
  """
  @spec set_quotas(cluster(), String.t(), non_neg_integer(), non_neg_integer(), keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def set_quotas(cluster, role_name, read_quota, write_quota, opts \\ [])
      when is_binary(role_name) and is_list(opts) do
    with {:ok, read_quota} <- validate_role_quota_value(read_quota, :read_quota),
         {:ok, write_quota} <- validate_role_quota_value(write_quota, :write_quota),
         {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.set_quotas(cluster, role_name, read_quota, write_quota, opts)
    end
  end

  @doc """
  Grants privileges to a security role.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec grant_privileges(cluster(), String.t(), [privilege()], keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def grant_privileges(cluster, role_name, privileges, opts \\ [])
      when is_binary(role_name) and is_list(privileges) and is_list(opts) do
    with {:ok, privileges} <- validate_privileges(privileges),
         {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.grant_privileges(cluster, role_name, privileges, opts)
    end
  end

  @doc """
  Revokes privileges from a security role.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec revoke_privileges(cluster(), String.t(), [privilege()], keyword()) ::
          :ok | {:error, Aerospike.Error.t()}
  def revoke_privileges(cluster, role_name, privileges, opts \\ [])
      when is_binary(role_name) and is_list(privileges) and is_list(opts) do
    with {:ok, privileges} <- validate_privileges(privileges),
         {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.revoke_privileges(cluster, role_name, privileges, opts)
    end
  end

  @doc """
  Queries one security role.

  Returns `{:ok, nil}` when the named role does not exist.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec query_role(cluster(), String.t(), keyword()) ::
          {:ok, role_info() | nil} | {:error, Aerospike.Error.t()}
  def query_role(cluster, role_name, opts \\ [])
      when is_binary(role_name) and is_list(opts) do
    with {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.query_role(cluster, role_name, opts)
    end
  end

  @doc """
  Queries all security roles visible to the authenticated cluster user.

  This requires Aerospike Enterprise with security enabled.
  """
  @spec query_roles(cluster(), keyword()) :: {:ok, [role_info()]} | {:error, Aerospike.Error.t()}
  def query_roles(cluster, opts \\ []) when is_list(opts) do
    with {:ok, _policy} <- Policy.security_admin(opts) do
      Admin.query_roles(cluster, opts)
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
  Executes one record UDF against `key`.

  This is a single-record command, not a background query job. It accepts the
  same narrow write opts as the unary write family:

    * `:timeout`
    * `:max_retries`
    * `:sleep_between_retries_ms`
    * `:ttl`
    * `:generation`
    * `:filter`
    * `:txn`

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.

  Transport failures are not retried automatically once the request is on the
  wire, because record UDFs may already have produced server-side effects.
  Package lifecycle lives on `register_udf/*`, `remove_udf/*`, and `list_udfs/2`.
  """
  @spec apply_udf(cluster(), Key.key_input(), String.t(), String.t(), list(), keyword()) ::
          {:ok, term()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def apply_udf(cluster, key, package, function, args, opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    with {:ok, key} <- coerce_key(key) do
      ApplyUdf.execute(cluster, key, package, function, args, opts)
    end
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
    * `:filter` — non-empty `%Aerospike.Exp{}` server-side filter
      expression, or `nil` for no filter

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
  Sends a caller-built single-record write/delete frame for `key`.

  This is an advanced escape hatch for tooling, proxy, and replay scenarios.
  `payload` must be a complete Aerospike wire frame for one write-shaped
  command. The client uses `key` only to choose the write partition owner,
  forwards `payload` unchanged, and parses only the standard write response.

  The payload must already contain every server-visible write attribute, such
  as generation, TTL, send-key, delete flags, filters, and any transaction
  fields. Passing `:txn` validates the transaction option shape but does not
  register the key with the transaction monitor or add transaction fields.

  Supported write opts are validated for routing and I/O budgets:

    * `:timeout`
    * `:max_retries`
    * `:sleep_between_retries_ms`
    * `:ttl`
    * `:generation`
    * `:filter`
    * `:txn`

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec put_payload(cluster(), Key.key_input(), binary(), keyword()) ::
          :ok
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def put_payload(cluster, key, payload, opts \\ [])
      when is_binary(payload) and is_list(opts) do
    with {:ok, key} <- coerce_key(key) do
      PutPayload.execute(cluster, key, payload, opts)
    end
  end

  @doc """
  Same as `put_payload/4` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec put_payload!(cluster(), Key.key_input(), binary(), keyword()) :: :ok
  def put_payload!(cluster, key, payload, opts \\ [])
      when is_binary(payload) and is_list(opts) do
    case put_payload(cluster, key, payload, opts) do
      :ok -> :ok
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Atomically adds numeric deltas in `bins` for `key`.

  This is a thin unary write helper over the same routed write path as `put/4`.
  The return shape stays aligned with the write family and does not expose
  `operate/4` record results.

  Supported write opts are:

    * `:timeout`
    * `:max_retries`
    * `:sleep_between_retries_ms`
    * `:ttl`
    * `:generation` — expect generation equality when non-zero
    * `:filter` — non-empty `%Aerospike.Exp{}` server-side filter
      expression, or `nil` for no filter

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec add(cluster(), Key.key_input(), Aerospike.Record.bins_input(), keyword()) ::
          {:ok, Aerospike.Record.metadata()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def add(cluster, key, bins, opts \\ []) do
    with {:ok, key} <- coerce_key(key) do
      WriteOp.execute(cluster, key, :add, bins, opts)
    end
  end

  @doc """
  Atomically appends string suffixes in `bins` for `key`.

  This stays on the unary write path and returns write metadata, not an
  `operate/4` record payload.

  Supported write opts are:

    * `:timeout`
    * `:max_retries`
    * `:sleep_between_retries_ms`
    * `:ttl`
    * `:generation` — expect generation equality when non-zero
    * `:filter` — non-empty `%Aerospike.Exp{}` server-side filter
      expression, or `nil` for no filter

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec append(cluster(), Key.key_input(), Aerospike.Record.bins_input(), keyword()) ::
          {:ok, Aerospike.Record.metadata()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def append(cluster, key, bins, opts \\ []) do
    with {:ok, key} <- coerce_key(key) do
      WriteOp.execute(cluster, key, :append, bins, opts)
    end
  end

  @doc """
  Atomically prepends string prefixes in `bins` for `key`.

  This stays on the unary write path and returns write metadata, not an
  `operate/4` record payload.

  Supported write opts are:

    * `:timeout`
    * `:max_retries`
    * `:sleep_between_retries_ms`
    * `:ttl`
    * `:generation` — expect generation equality when non-zero
    * `:filter` — non-empty `%Aerospike.Exp{}` server-side filter
      expression, or `nil` for no filter

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}`.
  """
  @spec prepend(cluster(), Key.key_input(), Aerospike.Record.bins_input(), keyword()) ::
          {:ok, Aerospike.Record.metadata()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def prepend(cluster, key, bins, opts \\ []) do
    with {:ok, key} <- coerce_key(key) do
      WriteOp.execute(cluster, key, :prepend, bins, opts)
    end
  end

  @doc """
  Returns whether `key` exists in `cluster` without reading bins.

  Supported read opts are `:timeout`, `:max_retries`,
  `:sleep_between_retries_ms`, `:replica_policy`, and `:filter`.

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
  `:sleep_between_retries_ms`, `:ttl`, `:generation`, and `:filter`.

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
  `:max_retries`, `:sleep_between_retries_ms`, `:generation`, and
  `:filter`.

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
  `:sleep_between_retries_ms`, `:ttl`, `:generation`, and `:filter`.

  Accepted operations include the simple tuple form plus the public
  `Aerospike.Op` helpers for primitive, CDT, bit, HyperLogLog, and expression
  operations. Returned operation values are accumulated into
  `%Aerospike.Record{bins: map}` by bin name.

      Aerospike.operate(cluster, key, [
        Aerospike.Op.Exp.read("projected", Aerospike.Exp.int_bin("count")),
        Aerospike.Op.Exp.write("computed", Aerospike.Exp.int(99))
      ])

      Aerospike.operate(cluster, key, [
        Aerospike.Op.List.append("profile", "signed-in",
          ctx: [Aerospike.Ctx.map_key("events")]
        )
      ])

      Aerospike.operate(cluster, key, [
        Aerospike.Op.Bit.count("flags", 0, 8),
        Aerospike.Op.HLL.get_count("visitors")
      ])

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

  defp batch_policy(cluster, opts) do
    cluster
    |> Cluster.retry_policy()
    |> Policy.batch(opts)
  end

  defp batch_command_entries(entries, %Policy.Batch{} = policy) do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, &put_batch_command_entry(&1, &2, policy))
    |> case do
      {:ok, command_entries} -> {:ok, Enum.reverse(command_entries)}
      {:error, %Error{}} = err -> err
    end
  end

  defp put_batch_command_entry({entry, index}, {:ok, acc}, %Policy.Batch{} = policy) do
    case batch_command_entry(entry, index, policy) do
      {:ok, %Entry{} = command_entry} -> {:cont, {:ok, [command_entry | acc]}}
      {:error, %Error{}} = err -> {:halt, err}
    end
  end

  defp batch_command_entry(%Batch.Read{key: %Key{} = key}, index, %Policy.Batch{} = policy) do
    {:ok,
     %Entry{
       index: index,
       key: key,
       kind: :read,
       dispatch: {:read, policy.retry.replica_policy, 0},
       payload: nil
     }}
  end

  defp batch_command_entry(%Batch.Put{key: %Key{} = key, bins: bins}, index, %Policy.Batch{})
       when is_map(bins) do
    with {:ok, operations} <- batch_put_operations(bins) do
      {:ok,
       %Entry{
         index: index,
         key: key,
         kind: :put,
         dispatch: :write,
         payload: %{operations: operations}
       }}
    end
  end

  defp batch_command_entry(%Batch.Delete{key: %Key{} = key}, index, %Policy.Batch{}) do
    {:ok,
     %Entry{
       index: index,
       key: key,
       kind: :delete,
       dispatch: :write,
       payload: nil
     }}
  end

  defp batch_command_entry(
         %Batch.Operate{key: %Key{} = key, operations: operations},
         index,
         %Policy.Batch{} = policy
       )
       when is_list(operations) do
    with :ok <- validate_batch_operations(operations) do
      flags = OperateFlags.scan_ops(operations)

      {:ok,
       %Entry{
         index: index,
         key: key,
         kind: :operate,
         dispatch: batch_operate_dispatch(flags, policy),
         payload: %{operations: operations, flags: flags}
       }}
    end
  end

  defp batch_command_entry(
         %Batch.UDF{key: %Key{} = key, package: package, function: function, args: args},
         index,
         %Policy.Batch{}
       )
       when is_binary(package) and is_binary(function) and is_list(args) do
    {:ok,
     %Entry{
       index: index,
       key: key,
       kind: :udf,
       dispatch: :write,
       payload: %{package: package, function: function, args: args}
     }}
  end

  defp batch_command_entry(_entry, _index, %Policy.Batch{}) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "Aerospike.batch_operate/3 expects entries built by Aerospike.Batch"
     )}
  end

  defp batch_operate_dispatch(%{has_write?: true}, %Policy.Batch{}), do: :write

  defp batch_operate_dispatch(%{has_write?: false}, %Policy.Batch{} = policy) do
    {:read, policy.retry.replica_policy, 0}
  end

  defp batch_put_operations(bins) when map_size(bins) == 0 do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "batch put requires at least one bin write"
     )}
  end

  defp batch_put_operations(bins) do
    bins
    |> Enum.reduce_while({:ok, []}, fn {bin_name, value}, {:ok, acc} ->
      case Operation.write(normalize_batch_bin_name(bin_name), value) do
        {:ok, operation} -> {:cont, {:ok, [operation | acc]}}
        {:error, %Error{}} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, operations} -> {:ok, Enum.reverse(operations)}
      {:error, %Error{}} = err -> err
    end
  end

  defp validate_batch_operations([]) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: "batch operate requires at least one operation"
     )}
  end

  defp validate_batch_operations(operations) do
    if Enum.all?(operations, &match?(%Operation{}, &1)) do
      :ok
    else
      {:error,
       Error.from_result_code(:invalid_argument,
         message: "batch operate expects a list of Aerospike.Op operations"
       )}
    end
  end

  defp normalize_batch_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_batch_bin_name(bin_name), do: bin_name

  defp validate_truncate_opts(opts, callsite) when is_list(opts) do
    with :ok <- validate_truncate_opt_keys(opts, callsite),
         {:ok, _policy} <- Policy.admin_info(Keyword.delete(opts, :before)),
         :ok <- validate_truncate_before(opts) do
      {:ok, opts}
    end
  end

  defp validate_truncate_opt_keys([], _callsite), do: :ok

  defp validate_truncate_opt_keys([{key, _value} | rest], callsite)
       when key in [:before, :pool_checkout_timeout] do
    validate_truncate_opt_keys(rest, callsite)
  end

  defp validate_truncate_opt_keys([{key, _value} | _rest], callsite) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message:
         "#{callsite} supports only :before and :pool_checkout_timeout options, got #{inspect(key)}"
     )}
  end

  defp validate_truncate_before(opts) do
    case Keyword.get(opts, :before) do
      nil ->
        :ok

      %DateTime{} ->
        :ok

      other ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: ":before must be a DateTime, got: #{inspect(other)}"
         )}
    end
  end

  defp validate_role_names(roles) when is_list(roles) do
    if Enum.all?(roles, &is_binary/1) do
      {:ok, roles}
    else
      {:error,
       Error.from_result_code(:invalid_argument, message: "roles must be a list of strings")}
    end
  end

  defp validate_privileges(privileges) when is_list(privileges) do
    if Enum.all?(privileges, &match?(%Privilege{}, &1)) do
      {:ok, privileges}
    else
      {:error,
       Error.from_result_code(:invalid_argument,
         message: "privileges must be a list of %Aerospike.Privilege{} structs"
       )}
    end
  end

  defp validate_create_role_opts(opts) when is_list(opts) do
    supported = [:whitelist, :read_quota, :write_quota, :timeout, :pool_checkout_timeout]

    with :ok <- validate_supported_opts(opts, supported, "Aerospike.create_role/4"),
         {:ok, whitelist} <- validate_role_whitelist(opts),
         {:ok, read_quota} <- validate_role_quota(opts, :read_quota),
         {:ok, write_quota} <- validate_role_quota(opts, :write_quota) do
      call_opts = Keyword.drop(opts, [:whitelist, :read_quota, :write_quota])
      {:ok, {whitelist, read_quota, write_quota, call_opts}}
    end
  end

  defp validate_supported_opts(opts, supported, callsite) when is_list(opts) do
    case Enum.find(opts, fn {key, _value} -> key not in supported end) do
      nil ->
        :ok

      {key, _value} ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message:
             "#{callsite} supports only #{format_supported_opts(supported)} options, got #{inspect(key)}"
         )}
    end
  end

  defp validate_role_whitelist(opts) do
    Keyword.get(opts, :whitelist, [])
    |> validate_role_whitelist_value()
  end

  defp validate_role_whitelist_value(whitelist) when is_list(whitelist) do
    if Enum.all?(whitelist, &is_binary/1) do
      {:ok, whitelist}
    else
      invalid_role_whitelist(whitelist)
    end
  end

  defp validate_role_whitelist_value(other), do: invalid_role_whitelist(other)

  defp validate_role_quota(opts, key) when key in [:read_quota, :write_quota] do
    opts
    |> Keyword.get(key, 0)
    |> validate_role_quota_value(key)
  end

  defp validate_role_quota_value(quota, _key) when is_integer(quota) and quota >= 0 do
    {:ok, quota}
  end

  defp validate_role_quota_value(other, key) when key in [:read_quota, :write_quota] do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: ":#{key} must be a non-negative integer, got: #{inspect(other)}"
     )}
  end

  defp format_supported_opts([opt]), do: inspect(opt)
  defp format_supported_opts([left, right]), do: "#{inspect(left)} and #{inspect(right)}"

  defp format_supported_opts(opts) when is_list(opts) do
    {init, [last]} = Enum.split(opts, -1)
    Enum.map_join(init, ", ", &inspect/1) <> ", and " <> inspect(last)
  end

  defp invalid_role_whitelist(value) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message: ":whitelist must be a list of strings, got: #{inspect(value)}"
     )}
  end

  defp validate_enable_metrics_opts(opts) when is_list(opts) do
    case Enum.find(opts, fn {key, _value} -> key != :reset end) do
      nil ->
        validate_enable_metrics_reset(opts)

      {key, _value} ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: "Aerospike.enable_metrics/2 supports only :reset option, got #{inspect(key)}"
         )}
    end
  end

  defp validate_enable_metrics_reset(opts) do
    case Keyword.get(opts, :reset, false) do
      reset when is_boolean(reset) ->
        :ok

      other ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: ":reset must be a boolean, got: #{inspect(other)}"
         )}
    end
  end

  defp validate_warm_up_opts(opts) when is_list(opts) do
    with :ok <-
           validate_supported_opts(opts, [:count, :pool_checkout_timeout], "Aerospike.warm_up/2"),
         :ok <- validate_non_neg_opt(opts, :count) do
      validate_non_neg_opt(opts, :pool_checkout_timeout)
    end
  end

  defp validate_non_neg_opt(opts, key) do
    case Keyword.fetch(opts, key) do
      :error ->
        :ok

      {:ok, value} when is_integer(value) and value >= 0 ->
        :ok

      {:ok, value} ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: ":#{key} must be a non-negative integer, got: #{inspect(value)}"
         )}
    end
  end
end

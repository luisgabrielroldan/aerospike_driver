defmodule Aerospike do
  @moduledoc """
  Public entry point for the spike client.

  The spike now proves a small unary command family on one shared
  execution path plus one narrow batch read helper. It now also proves
  a narrow scan surface over the shared streaming substrate:

    * `get/3` reads all bins for a key
    * `put/4` writes a bin map
    * `exists/2` performs a header-only existence probe
    * `touch/2` updates record metadata
    * `delete/2` removes a record
    * `operate/4` runs a narrow write-plus-read operation list
    * `batch_get/4` reads multiple keys and returns per-key results in
      caller order
    * `stream!/3`, `all/3`, and `count/3` run scan fan-out across the
      shared stream substrate
    * `scan_stream_node!/4`, `scan_all_node/4`, and `scan_count_node/4`
      target one named node for the same scan surface

  Broader batch semantics, query, expressions, and the wider policy
  surface remain out of scope until later spike work proves them.
  """

  alias Aerospike.BatchGet
  alias Aerospike.Delete
  alias Aerospike.Exists
  alias Aerospike.Get
  alias Aerospike.Key
  alias Aerospike.Page
  alias Aerospike.Query
  alias Aerospike.Operate
  alias Aerospike.Put
  alias Aerospike.Scan
  alias Aerospike.ScanOps
  alias Aerospike.Supervisor, as: ClusterSupervisor
  alias Aerospike.Touch

  @typedoc """
  Identifier for a running cluster, i.e. an `Aerospike.Tender` process
  (pid or registered name).
  """
  @type cluster :: GenServer.server()
  @type node_name :: String.t()

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
  Reads `key` from `cluster`.

  The spike currently supports only `bins: :all`. Named-bin reads remain
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
  """
  @spec get(cluster(), Key.t(), :all, keyword()) ::
          {:ok, Aerospike.Record.t()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def get(cluster, key, bins \\ :all, opts \\ [])

  def get(cluster, %Key{} = key, bins, opts) do
    Get.execute(cluster, key, bins, opts)
  end

  @doc """
  Reads multiple `keys` from `cluster` in one batch request per target node.

  The result list stays in the same order as `keys`. Each list item is
  either `{:ok, %Aerospike.Record{}}` for a hit or an indexed error for
  that key (`{:error, %Aerospike.Error{}}`, `{:error, :no_master}`, or
  `{:error, :unknown_node}`).

  The spike currently supports only `bins: :all` and only `:timeout` in
  `opts`. Retry-driven regrouping stays disabled until the batch reroute
  path can honestly split a failed grouped request back across nodes.
  """
  @spec batch_get(cluster(), [Key.t()], :all, keyword()) ::
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
    BatchGet.execute(cluster, keys, bins, opts)
  end

  @doc """
  Returns a lazy `Stream` of records from a scan.

  The scan surface currently proves cluster-wide fan-out and node-targeted
  scan execution over the shared stream substrate. It accepts the narrow
  `Aerospike.Scan` builder and a small streaming policy surface.
  """
  @spec stream!(cluster(), Scan.t(), keyword()) :: Enumerable.t()
  def stream!(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    case ScanOps.stream(cluster, scan, opts) do
      {:ok, stream} -> stream
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Returns a lazy `Stream` of records from a secondary-index query.
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
  Returns a lazy `Stream` of query records from one named node.
  """
  @spec query_stream_node(cluster(), node_name(), Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Aerospike.Error.t()}
  def query_stream_node(cluster, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    ScanOps.query_stream_node(cluster, node_name, query, opts)
  end

  @doc """
  Same as `query_stream_node/4` but raises on error.
  """
  @spec query_stream_node!(cluster(), node_name(), Query.t(), keyword()) :: Enumerable.t()
  def query_stream_node!(cluster, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    case query_stream_node(cluster, node_name, query, opts) do
      {:ok, stream} -> stream
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Eagerly collects scan records into a list.
  """
  @spec all(cluster(), Scan.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Aerospike.Error.t()}
  def all(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    ScanOps.all(cluster, scan, opts)
  end

  @doc """
  Eagerly collects query records into a list.
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
  Eagerly collects query records from one named node.
  """
  @spec query_all_node(cluster(), node_name(), Query.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Aerospike.Error.t()}
  def query_all_node(cluster, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    ScanOps.query_all_node(cluster, node_name, query, opts)
  end

  @doc """
  Same as `query_all_node/4` but returns the list or raises `Aerospike.Error`.
  """
  @spec query_all_node!(cluster(), node_name(), Query.t(), keyword()) :: [Aerospike.Record.t()]
  def query_all_node!(cluster, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    case query_all_node(cluster, node_name, query, opts) do
      {:ok, records} -> records
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Same as `all/3` but returns the list or raises `Aerospike.Error`.
  """
  @spec all!(cluster(), Scan.t(), keyword()) :: [Aerospike.Record.t()]
  def all!(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    case all(cluster, scan, opts) do
      {:ok, records} -> records
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Counts scan matches without materializing the records.
  """
  @spec count(cluster(), Scan.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Aerospike.Error.t()}
  def count(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    ScanOps.count(cluster, scan, opts)
  end

  @doc """
  Counts query matches without materializing the records.
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
  Counts query matches on one named node.
  """
  @spec query_count_node(cluster(), node_name(), Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Aerospike.Error.t()}
  def query_count_node(cluster, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    ScanOps.query_count_node(cluster, node_name, query, opts)
  end

  @doc """
  Same as `query_count_node/4` but returns the count or raises `Aerospike.Error`.
  """
  @spec query_count_node!(cluster(), node_name(), Query.t(), keyword()) :: non_neg_integer()
  def query_count_node!(cluster, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    case query_count_node(cluster, node_name, query, opts) do
      {:ok, count} -> count
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Returns a single query page and a resumable cursor when more records remain.
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
  Returns a single query page from one named node.
  """
  @spec query_page_node(cluster(), node_name(), Query.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Aerospike.Error.t()}
  def query_page_node(cluster, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    ScanOps.query_page_node(cluster, node_name, query, opts)
  end

  @doc """
  Same as `query_page_node/4` but returns the page or raises `Aerospike.Error`.
  """
  @spec query_page_node!(cluster(), node_name(), Query.t(), keyword()) :: Page.t()
  def query_page_node!(cluster, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    case query_page_node(cluster, node_name, query, opts) do
      {:ok, page} -> page
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Same as `count/3` but returns the count or raises `Aerospike.Error`.
  """
  @spec count!(cluster(), Scan.t(), keyword()) :: non_neg_integer()
  def count!(cluster, %Scan{} = scan, opts \\ []) when is_list(opts) do
    case count(cluster, scan, opts) do
      {:ok, count} -> count
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Returns a lazy `Stream` of scan records from one named node.
  """
  @spec scan_stream_node!(cluster(), node_name(), Scan.t(), keyword()) :: Enumerable.t()
  def scan_stream_node!(cluster, node_name, %Scan{} = scan, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    case ScanOps.stream_node(cluster, node_name, scan, opts) do
      {:ok, stream} -> stream
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Eagerly collects scan records from one named node.
  """
  @spec scan_all_node(cluster(), node_name(), Scan.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Aerospike.Error.t()}
  def scan_all_node(cluster, node_name, %Scan{} = scan, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    ScanOps.all_node(cluster, node_name, scan, opts)
  end

  @doc """
  Same as `scan_all_node/4` but returns the list or raises `Aerospike.Error`.
  """
  @spec scan_all_node!(cluster(), node_name(), Scan.t(), keyword()) :: [Aerospike.Record.t()]
  def scan_all_node!(cluster, node_name, %Scan{} = scan, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    case scan_all_node(cluster, node_name, scan, opts) do
      {:ok, records} -> records
      {:error, %Aerospike.Error{} = err} -> raise err
    end
  end

  @doc """
  Counts scan matches on one named node.
  """
  @spec scan_count_node(cluster(), node_name(), Scan.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Aerospike.Error.t()}
  def scan_count_node(cluster, node_name, %Scan{} = scan, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    ScanOps.count_node(cluster, node_name, scan, opts)
  end

  @doc """
  Same as `scan_count_node/4` but returns the count or raises `Aerospike.Error`.
  """
  @spec scan_count_node!(cluster(), node_name(), Scan.t(), keyword()) :: non_neg_integer()
  def scan_count_node!(cluster, node_name, %Scan{} = scan, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    case scan_count_node(cluster, node_name, scan, opts) do
      {:ok, count} -> count
      {:error, %Aerospike.Error{} = err} -> raise err
    end
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
  """
  @spec put(cluster(), Key.t(), Aerospike.Record.bins_input(), keyword()) ::
          {:ok, Aerospike.Record.metadata()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def put(cluster, %Key{} = key, bins, opts \\ []) do
    Put.execute(cluster, key, bins, opts)
  end

  @doc """
  Returns whether `key` exists in `cluster` without reading bins.

  Supported read opts are `:timeout`, `:max_retries`,
  `:sleep_between_retries_ms`, and `:replica_policy`.
  """
  @spec exists(cluster(), Key.t(), keyword()) ::
          {:ok, boolean()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def exists(cluster, %Key{} = key, opts \\ []) do
    Exists.execute(cluster, key, opts)
  end

  @doc """
  Updates `key`'s header metadata in `cluster`.

  Supported write opts are `:timeout`, `:max_retries`,
  `:sleep_between_retries_ms`, `:ttl`, and `:generation`.
  """
  @spec touch(cluster(), Key.t(), keyword()) ::
          {:ok, Aerospike.Record.metadata()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def touch(cluster, %Key{} = key, opts \\ []) do
    Touch.execute(cluster, key, opts)
  end

  @doc """
  Deletes `key` from `cluster`.

  Returns `{:ok, true}` when a record was deleted and `{:ok, false}`
  when the key was already absent. Supported write opts are `:timeout`,
  `:max_retries`, `:sleep_between_retries_ms`, and `:generation`.
  """
  @spec delete(cluster(), Key.t(), keyword()) ::
          {:ok, boolean()}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def delete(cluster, %Key{} = key, opts \\ []) do
    Delete.execute(cluster, key, opts)
  end

  @doc """
  Runs a constrained unary operate list for `key`.

  Supported operations:

    * `{:write, bin, value}` — simple bin write
    * `{:read, bin}` — simple bin read

  The list must include at least one write operation. The spike uses
  write/master routing for `operate` in this phase, so read-only operate
  remains out of scope until dispatch can vary per command input.

  Supported opts are `:timeout`, `:max_retries`,
  `:sleep_between_retries_ms`, `:ttl`, and `:generation`.

  Returns decoded operation values in request order.
  """
  @spec operate(cluster(), Key.t(), [Operate.simple_operation()], keyword()) ::
          {:ok, [term()]}
          | {:error, Aerospike.Error.t()}
          | {:error, :cluster_not_ready | :no_master | :unknown_node}
  def operate(cluster, %Key{} = key, operations, opts \\ []) do
    Operate.execute(cluster, key, operations, opts)
  end
end

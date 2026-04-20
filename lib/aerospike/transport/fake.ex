defmodule Aerospike.Transport.Fake do
  @moduledoc """
  Scripted `Aerospike.NodeTransport` implementation for deterministic tests.

  A fake instance is a `GenServer` that holds scripted replies keyed by
  `{node_id, kind}` where `kind` is either an info-command list or the atom
  `:command` for AS_MSG requests. Each script entry is a queue: the first
  call consumes the head of the queue for that key, the next call gets the
  next entry, and so on. When a queue is empty, the instance's configured
  default reply is returned (by default, an `:no_script` error so tests
  surface missing scripting).

  The fake does not parse wire bytes. `info/2` matches on the exact list of
  info commands passed by the caller; `command/2` does not inspect request
  bytes at all. Tests drive the fake at the protocol seam the Tender and
  Router actually use, not a deeper wire-level simulation.

  ### Usage

      {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
      Fake.script_info(fake, "A1", ["node"], %{"node" => "BB9A1..."})
      Fake.script_command(fake, "A1", {:ok, <<0, 1, 2>>})

      # Hand `fake: fake` to code under test via connect opts:
      {:ok, conn} = Fake.connect("10.0.0.1", 3000, fake: fake)
      {:ok, %{"node" => _}} = Fake.info(conn, ["node"])

  `node_id` is the caller-chosen symbolic name the test asserts against
  (typically the Aerospike node id like `"BB9A1..."`, but any unique term
  works).
  """

  @behaviour Aerospike.NodeTransport

  use GenServer

  alias Aerospike.Error

  @typedoc "Symbolic node identifier used by tests to address scripted replies."
  @type node_id :: term()

  @typedoc "Scripted reply for a single info or command call."
  @type scripted_reply ::
          {:ok, %{String.t() => String.t()}}
          | {:ok, binary()}
          | {:error, Error.t()}

  @typedoc "Concrete connection handle returned by `connect/3`."
  @opaque conn :: %__MODULE__{fake: pid(), node_id: node_id(), ref: reference()}

  @enforce_keys [:fake, :node_id, :ref]
  defstruct [:fake, :node_id, :ref]

  ## Public scripting API

  @doc """
  Starts a fake transport instance.

  Options:

    * `:nodes` — a list of `{node_id, host, port}` triples to pre-register.
      Equivalent to calling `register_node/4` for each entry after start.
    * `:default_reply` — reply returned when a script queue is empty. Must
      be a `scripted_reply`. Defaults to an `:no_script` error.
    * `:name` — optional registered name for the GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Registers a node under `node_id` reachable at `host:port`. Later
  `connect/3` calls with that host and port resolve to `node_id`.
  """
  @spec register_node(pid(), node_id(), String.t(), :inet.port_number()) :: :ok
  def register_node(fake, node_id, host, port)
      when is_binary(host) and is_integer(port) do
    GenServer.call(fake, {:register_node, node_id, host, port})
  end

  @doc """
  Appends a scripted info reply for `node_id`. `commands` must match exactly
  the list passed to `info/2` (same order).
  """
  @spec script_info(pid(), node_id(), [String.t()], %{String.t() => String.t()}) :: :ok
  def script_info(fake, node_id, commands, %{} = reply) when is_list(commands) do
    GenServer.call(fake, {:script, {:info, node_id, commands}, {:ok, reply}})
  end

  @doc """
  Appends a scripted info error for `node_id` and the given command list.
  """
  @spec script_info_error(pid(), node_id(), [String.t()], Error.t()) :: :ok
  def script_info_error(fake, node_id, commands, %Error{} = error)
      when is_list(commands) do
    GenServer.call(fake, {:script, {:info, node_id, commands}, {:error, error}})
  end

  @doc """
  Appends a scripted command reply for `node_id`. The reply is returned
  verbatim by the next `command/2` call for a connection addressing that
  node, regardless of request bytes.
  """
  @spec script_command(pid(), node_id(), {:ok, binary()} | {:error, Error.t()}) :: :ok
  def script_command(fake, node_id, {:ok, bytes} = reply) when is_binary(bytes) do
    GenServer.call(fake, {:script, {:command, node_id}, reply})
  end

  def script_command(fake, node_id, {:error, %Error{}} = reply) do
    GenServer.call(fake, {:script, {:command, node_id}, reply})
  end

  @doc """
  Marks `node_id` as disconnected. Subsequent `info/2` and `command/2`
  calls on connections addressing that node return a `:network_error`
  without consuming any script entry.
  """
  @spec disconnect(pid(), node_id()) :: :ok
  def disconnect(fake, node_id) do
    GenServer.call(fake, {:disconnect, node_id})
  end

  @doc """
  Appends a scripted `connect/3` outcome for `node_id`. The next
  `connect/3` call resolving to that node consumes the head of the queue
  instead of the default success path. When the queue is empty the
  default behaviour resumes (success unless the node is `disconnect/2`-ed).

  This lets tests script per-connect-attempt failures during pool warm-up
  without flipping the global disconnected flag.
  """
  @spec script_connect(pid(), node_id(), :ok | {:error, Error.t()}) :: :ok
  def script_connect(fake, node_id, :ok) do
    GenServer.call(fake, {:script_connect, node_id, :ok})
  end

  def script_connect(fake, node_id, {:error, %Error{}} = reply) do
    GenServer.call(fake, {:script_connect, node_id, reply})
  end

  @doc """
  Returns the number of times `connect/3` was invoked for `node_id`,
  including failures from `script_connect/3` and `disconnect/2`.
  """
  @spec connect_count(pid(), node_id()) :: non_neg_integer()
  def connect_count(fake, node_id) do
    GenServer.call(fake, {:connect_count, node_id})
  end

  @doc """
  Returns the number of times `close/1` was invoked for a connection
  addressing `node_id`. Useful for asserting idle eviction: a connect
  followed by a close without a checkout in between indicates a
  `handle_ping/2` eviction.
  """
  @spec close_count(pid(), node_id()) :: non_neg_integer()
  def close_count(fake, node_id) do
    GenServer.call(fake, {:close_count, node_id})
  end

  @doc """
  Returns the deadline (in milliseconds) passed to the most recent
  `command/3` call for any connection addressing `node_id`, or `nil` if
  `command/3` has not been called for that node yet. Lets tests assert
  that callers plumbed a read deadline through the behaviour.
  """
  @spec last_command_deadline(pid(), node_id()) :: non_neg_integer() | nil
  def last_command_deadline(fake, node_id) do
    GenServer.call(fake, {:last_command_deadline, node_id})
  end

  @doc """
  Clears a prior `disconnect/2` for `node_id`. Scripted replies resume.
  """
  @spec reconnect(pid(), node_id()) :: :ok
  def reconnect(fake, node_id) do
    GenServer.call(fake, {:reconnect, node_id})
  end

  @doc """
  Overrides the default reply returned when a script queue is empty.
  """
  @spec set_default_reply(pid(), scripted_reply()) :: :ok
  def set_default_reply(fake, reply) do
    GenServer.call(fake, {:set_default_reply, reply})
  end

  ## NodeTransport callbacks

  @impl Aerospike.NodeTransport
  def connect(host, port, opts)
      when is_binary(host) and is_integer(port) and is_list(opts) do
    case Keyword.fetch(opts, :fake) do
      {:ok, fake} -> GenServer.call(fake, {:connect, host, port})
      :error -> {:error, fake_missing_error()}
    end
  end

  @impl Aerospike.NodeTransport
  def close(%__MODULE__{fake: fake, ref: ref}) do
    GenServer.call(fake, {:close, ref})
  end

  @impl Aerospike.NodeTransport
  def info(%__MODULE__{fake: fake, ref: ref}, commands) when is_list(commands) do
    GenServer.call(fake, {:consume, ref, {:info, commands}})
  end

  @impl Aerospike.NodeTransport
  def command(%__MODULE__{fake: fake, ref: ref}, request, deadline_ms, opts \\ [])
      when (is_binary(request) or is_list(request)) and is_integer(deadline_ms) and
             deadline_ms >= 0 and is_list(opts) do
    GenServer.call(fake, {:consume, ref, {:command, deadline_ms}})
  end

  ## GenServer callbacks

  @impl GenServer
  def init(opts) do
    state = %{
      nodes: %{},
      scripts: %{},
      connect_scripts: %{},
      connect_counts: %{},
      close_counts: %{},
      last_command_deadlines: %{},
      conns: %{},
      disconnected: MapSet.new(),
      default_reply: Keyword.get(opts, :default_reply, default_no_script_reply())
    }

    nodes = Keyword.get(opts, :nodes, [])
    {:ok, Enum.reduce(nodes, state, &register_node_reduce/2)}
  end

  @impl GenServer
  def handle_call({:register_node, node_id, host, port}, _from, state) do
    {:reply, :ok, put_node(state, node_id, host, port)}
  end

  def handle_call({:script, key, reply}, _from, state) do
    queue = Map.get(state.scripts, key, :queue.new())
    scripts = Map.put(state.scripts, key, :queue.in(reply, queue))
    {:reply, :ok, %{state | scripts: scripts}}
  end

  def handle_call({:disconnect, node_id}, _from, state) do
    {:reply, :ok, %{state | disconnected: MapSet.put(state.disconnected, node_id)}}
  end

  def handle_call({:reconnect, node_id}, _from, state) do
    {:reply, :ok, %{state | disconnected: MapSet.delete(state.disconnected, node_id)}}
  end

  def handle_call({:set_default_reply, reply}, _from, state) do
    {:reply, :ok, %{state | default_reply: reply}}
  end

  def handle_call({:script_connect, node_id, outcome}, _from, state) do
    queue = Map.get(state.connect_scripts, node_id, :queue.new())
    scripts = Map.put(state.connect_scripts, node_id, :queue.in(outcome, queue))
    {:reply, :ok, %{state | connect_scripts: scripts}}
  end

  def handle_call({:connect_count, node_id}, _from, state) do
    {:reply, Map.get(state.connect_counts, node_id, 0), state}
  end

  def handle_call({:close_count, node_id}, _from, state) do
    {:reply, Map.get(state.close_counts, node_id, 0), state}
  end

  def handle_call({:last_command_deadline, node_id}, _from, state) do
    {:reply, Map.get(state.last_command_deadlines, node_id), state}
  end

  def handle_call({:connect, host, port}, _from, state) do
    case Map.fetch(state.nodes, {host, port}) do
      {:ok, node_id} ->
        state = bump_connect_count(state, node_id)
        {outcome, state} = next_connect_outcome(state, node_id)
        do_connect(state, node_id, host, port, outcome)

      :error ->
        {:reply, {:error, unknown_host_error(host, port)}, state}
    end
  end

  def handle_call({:close, ref}, _from, state) do
    state =
      case Map.fetch(state.conns, ref) do
        {:ok, node_id} -> bump_close_count(state, node_id)
        :error -> state
      end

    {:reply, :ok, %{state | conns: Map.delete(state.conns, ref)}}
  end

  def handle_call({:consume, ref, kind}, _from, state) do
    with {:ok, node_id} <- fetch_conn(state, ref),
         :ok <- check_connected(state, node_id) do
      state = record_command_deadline(state, node_id, kind)
      {reply, state} = consume_script(state, node_id, kind)
      {:reply, reply, state}
    else
      {:error, %Error{}} = err -> {:reply, err, state}
    end
  end

  ## Helpers

  defp bump_connect_count(state, node_id) do
    counts = Map.update(state.connect_counts, node_id, 1, &(&1 + 1))
    %{state | connect_counts: counts}
  end

  defp bump_close_count(state, node_id) do
    counts = Map.update(state.close_counts, node_id, 1, &(&1 + 1))
    %{state | close_counts: counts}
  end

  defp next_connect_outcome(state, node_id) do
    case Map.get(state.connect_scripts, node_id) do
      nil ->
        {:default, state}

      queue ->
        case :queue.out(queue) do
          {{:value, outcome}, rest} ->
            {outcome, %{state | connect_scripts: Map.put(state.connect_scripts, node_id, rest)}}

          {:empty, _} ->
            {:default, state}
        end
    end
  end

  defp do_connect(state, _node_id, _host, _port, {:error, %Error{}} = err) do
    {:reply, err, state}
  end

  defp do_connect(state, node_id, host, port, :ok) do
    open_conn(state, node_id, host, port)
  end

  defp do_connect(state, node_id, host, port, :default) do
    if MapSet.member?(state.disconnected, node_id) do
      {:reply, {:error, connection_refused_error(host, port)}, state}
    else
      open_conn(state, node_id, host, port)
    end
  end

  defp open_conn(state, node_id, _host, _port) do
    ref = make_ref()
    conn = %__MODULE__{fake: self(), node_id: node_id, ref: ref}
    {:reply, {:ok, conn}, %{state | conns: Map.put(state.conns, ref, node_id)}}
  end

  defp register_node_reduce({node_id, host, port}, state) when is_binary(host) do
    put_node(state, node_id, host, port)
  end

  defp put_node(state, node_id, host, port) do
    %{state | nodes: Map.put(state.nodes, {host, port}, node_id)}
  end

  defp fetch_conn(state, ref) do
    case Map.fetch(state.conns, ref) do
      {:ok, node_id} -> {:ok, node_id}
      :error -> {:error, closed_conn_error()}
    end
  end

  defp check_connected(state, node_id) do
    if MapSet.member?(state.disconnected, node_id) do
      {:error, network_error("node #{inspect(node_id)} is disconnected")}
    else
      :ok
    end
  end

  defp consume_script(state, node_id, kind) do
    key = script_key(node_id, kind)

    case Map.get(state.scripts, key) do
      nil ->
        {state.default_reply, state}

      queue ->
        case :queue.out(queue) do
          {{:value, reply}, rest} ->
            {reply, %{state | scripts: Map.put(state.scripts, key, rest)}}

          {:empty, _} ->
            {state.default_reply, state}
        end
    end
  end

  defp script_key(node_id, {:info, commands}), do: {:info, node_id, commands}
  defp script_key(node_id, {:command, _deadline_ms}), do: {:command, node_id}

  defp record_command_deadline(state, node_id, {:command, deadline_ms}) do
    %{state | last_command_deadlines: Map.put(state.last_command_deadlines, node_id, deadline_ms)}
  end

  defp record_command_deadline(state, _node_id, {:info, _commands}), do: state

  defp default_no_script_reply do
    {:error,
     %Error{
       code: :no_script,
       message: "Aerospike.Transport.Fake: no scripted reply available"
     }}
  end

  defp fake_missing_error do
    %Error{
      code: :connection_error,
      message: "Aerospike.Transport.Fake: :fake option missing from connect opts"
    }
  end

  defp unknown_host_error(host, port) do
    %Error{
      code: :connection_error,
      message: "Aerospike.Transport.Fake: no node registered at #{host}:#{port}"
    }
  end

  defp connection_refused_error(host, port) do
    %Error{
      code: :connection_error,
      message: "Aerospike.Transport.Fake: connection refused at #{host}:#{port}"
    }
  end

  defp closed_conn_error do
    %Error{
      code: :network_error,
      message: "Aerospike.Transport.Fake: connection handle closed or unknown"
    }
  end

  defp network_error(message) do
    %Error{code: :network_error, message: message}
  end
end

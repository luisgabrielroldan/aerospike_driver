defmodule Aerospike.Query.AggregateReducer do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Geo

  @default_timeout 5_000
  @extra_sandbox_paths [
    [:_G, :bit32],
    [:_G, :collectgarbage],
    [:_G, :debug],
    [:_G, :dofile],
    [:_G, :eprint],
    [:_G, :file],
    [:_G, :getmetatable],
    [:_G, :io],
    [:_G, :load],
    [:_G, :loadfile],
    [:_G, :loadstring],
    [:_G, :os],
    [:_G, :package],
    [:_G, :print],
    [:_G, :rawget],
    [:_G, :rawlen],
    [:_G, :rawset],
    [:_G, :require],
    [:_G, :setmetatable],
    [:_G, :utf8]
  ]

  @enforce_keys [:function, :args, :state, :timeout]
  defstruct [:function, :args, :state, :timeout]

  @type t :: %__MODULE__{
          function: String.t(),
          args: [term()],
          state: term(),
          timeout: pos_integer()
        }

  @spec prepare(String.t(), String.t(), [term()], keyword()) :: {:ok, t()} | {:error, Error.t()}
  def prepare(package, function, args, opts)
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    with :ok <- reject_node_option(opts),
         {:ok, source} <- source(opts),
         {:ok, timeout} <- timeout(opts),
         :ok <- validate_values(args, :invalid_argument) do
      load(package, function, source, args, timeout)
    end
  end

  @spec run(t(), Enumerable.t()) :: {:ok, term() | nil} | {:error, Error.t()}
  def run(%__MODULE__{} = reducer, values) do
    bounded(reducer.timeout, fn ->
      with {:ok, values} <- Enum.reduce_while(values, {:ok, []}, &collect_value/2),
           {:ok, state} <- put_decoded(reducer.state, "__as_values", Enum.reverse(values)),
           {:ok, state} <- put_decoded(state, "__as_args", reducer.args),
           {:ok, state} <- put_decoded(state, "__as_function", reducer.function),
           {:ok, encoded_results, state} <- run_lua(finalize_source(), state, reducer.timeout),
           {:ok, results} <- decode_final_results(encoded_results, state) do
        one_result(results)
      end
    end)
  end

  defp load(package, function, source, args, timeout) do
    bounded(timeout, fn ->
      state =
        :luerl.init()
        |> :luerl_sandbox.init()
        |> :luerl_sandbox.init(@extra_sandbox_paths)

      with {:ok, state} <- put_decoded(state, "__as_package", package),
           {:ok, state} <- put_decoded(state, "__as_function", function),
           {:ok, state} <- put_decoded(state, "__as_args", args),
           {:ok, _result, state} <-
             run_lua([support_source(), "\n", source, "\n", prepare_source()], state, timeout) do
        {:ok, %__MODULE__{function: function, args: args, state: state, timeout: timeout}}
      end
    end)
  end

  defp source(opts) do
    inline? = Keyword.has_key?(opts, :source)
    path? = Keyword.has_key?(opts, :source_path)

    source(inline?, path?, opts)
  end

  defp source(true, false, opts) do
    case Keyword.fetch!(opts, :source) do
      source when is_binary(source) ->
        {:ok, source}

      other ->
        invalid_argument("aggregate Lua source must be a binary, got #{inspect(other)}")
    end
  end

  defp source(false, true, opts) do
    case Keyword.fetch!(opts, :source_path) do
      path when is_binary(path) ->
        read_source_path(path)

      other ->
        invalid_argument("aggregate Lua source_path must be a path binary, got #{inspect(other)}")
    end
  end

  defp source(true, true, _opts),
    do: invalid_argument("pass exactly one of :source or :source_path for aggregate Lua source")

  defp source(false, false, _opts),
    do: invalid_argument("missing aggregate Lua source; pass :source or :source_path")

  defp read_source_path(path) do
    case File.read(path) do
      {:ok, source} ->
        {:ok, source}

      {:error, reason} ->
        invalid_argument(
          "unable to read aggregate Lua source #{inspect(path)}: #{:file.format_error(reason)}"
        )
    end
  end

  defp timeout(opts) do
    case Keyword.get(opts, :timeout, @default_timeout) do
      timeout when is_integer(timeout) and timeout > 0 ->
        {:ok, timeout}

      _other ->
        {:ok, @default_timeout}
    end
  end

  defp reject_node_option(opts) do
    case Keyword.has_key?(opts, :node) do
      true -> invalid_argument("aggregate final reduction does not support the :node option")
      false -> :ok
    end
  end

  defp bounded(timeout, fun) when is_integer(timeout) and timeout > 0 and is_function(fun, 0) do
    task =
      Task.async(fn ->
        try do
          fun.()
        catch
          :error, %Error{} = error ->
            {:error, error}

          kind, reason ->
            query_error("aggregate Lua execution failed: #{inspect({kind, reason})}")
        end
      end)

    try do
      Task.await(task, timeout)
    catch
      :exit, {:timeout, _} ->
        Task.shutdown(task, :brutal_kill)
        {:error, Error.from_result_code(:timeout)}
    end
  end

  defp collect_value({:error, %Error{} = error}, _acc), do: {:halt, {:error, error}}

  defp collect_value(value, {:ok, acc}) do
    case validate_value(value, :query_generic) do
      :ok -> {:cont, {:ok, [value | acc]}}
      {:error, %Error{} = error} -> {:halt, {:error, error}}
    end
  end

  defp put_decoded(state, key, value) do
    case :luerl.set_table_keys_dec([key], value, state) do
      {:ok, state} ->
        {:ok, state}

      {:lua_error, reason, _state} ->
        query_error("unable to set Lua value #{key}: #{inspect(reason)}")
    end
  catch
    :error, reason -> query_error("unsupported Lua value #{key}: #{inspect(reason)}")
  end

  defp run_lua(source, state, timeout) do
    case :luerl_sandbox.run(IO.iodata_to_binary(source), %{max_time: timeout}, state) do
      {:ok, result, state} ->
        {:ok, result, state}

      {:error, :timeout} ->
        {:error, Error.from_result_code(:timeout)}

      {:error, reason} ->
        query_error("aggregate Lua execution failed: #{inspect(reason)}")

      {:lua_error, reason, _state} ->
        query_error("aggregate Lua execution failed: #{inspect(reason)}")
    end
  end

  defp validate_values(values, code) when is_list(values) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      case validate_value(value, code) do
        :ok -> {:cont, :ok}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp validate_value(nil, _code), do: :ok
  defp validate_value(value, _code) when is_boolean(value), do: :ok
  defp validate_value(value, _code) when is_integer(value), do: :ok
  defp validate_value(value, _code) when is_float(value), do: :ok
  defp validate_value(value, _code) when is_binary(value), do: :ok

  defp validate_value(values, code) when is_list(values) do
    validate_values(values, code)
  end

  defp validate_value(%{} = values, code) do
    Enum.reduce_while(values, :ok, fn {key, value}, :ok ->
      with :ok <- validate_map_key(key, code),
           :ok <- validate_value(value, code) do
        {:cont, :ok}
      else
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp validate_value(%Geo.Point{}, code), do: unsupported_value(code, "geo values")
  defp validate_value(%Geo.Polygon{}, code), do: unsupported_value(code, "geo values")
  defp validate_value(%Geo.Circle{}, code), do: unsupported_value(code, "geo values")
  defp validate_value({:blob, _binary}, code), do: unsupported_value(code, "blob values")

  defp validate_value({:raw, _particle_type, _data}, code),
    do: unsupported_value(code, "raw particles")

  defp validate_value({:geojson, _json}, code), do: unsupported_value(code, "geo values")

  defp validate_value(value, code),
    do: error(code, "unsupported aggregate Lua value #{inspect(value)}")

  defp validate_map_key(key, _code) when is_boolean(key), do: :ok
  defp validate_map_key(key, _code) when is_integer(key), do: :ok
  defp validate_map_key(key, _code) when is_float(key), do: :ok
  defp validate_map_key(key, _code) when is_binary(key), do: :ok

  defp validate_map_key(key, code),
    do: error(code, "unsupported aggregate Lua map key #{inspect(key)}")

  defp unsupported_value(code, label),
    do: error(code, "unsupported aggregate Lua value: #{label}")

  defp decode_final_results([encoded], state) do
    with {:ok, value} <- decode_lua_value(encoded, state) do
      {:ok, list_value(value)}
    end
  end

  defp decode_final_results(_other, _state),
    do: query_error("aggregate Lua finalizer returned an invalid result set")

  defp decode_lua_value(value, state) do
    value
    |> :luerl.decode(state)
    |> decode_decoded_value()
  catch
    :error, reason -> query_error("unsupported aggregate Lua output #{inspect(reason)}")
  end

  defp decode_decoded_value(nil), do: {:ok, nil}
  defp decode_decoded_value(value) when is_boolean(value), do: {:ok, value}
  defp decode_decoded_value(value) when is_integer(value), do: {:ok, value}
  defp decode_decoded_value(value) when is_float(value), do: {:ok, value}
  defp decode_decoded_value(value) when is_binary(value), do: {:ok, value}

  defp decode_decoded_value(pairs) when is_list(pairs) do
    case Enum.all?(pairs, &match?({_key, _value}, &1)) do
      true -> decode_lua_table(pairs)
      false -> query_error("aggregate Lua output list contains unsupported values")
    end
  end

  defp decode_decoded_value(value),
    do: query_error("unsupported aggregate Lua output #{inspect(value)}")

  defp decode_lua_table(pairs) do
    with {:ok, decoded_pairs} <- decode_lua_pairs(pairs) do
      table_value(decoded_pairs)
    end
  end

  defp decode_lua_pairs(pairs) do
    Enum.reduce_while(pairs, {:ok, []}, fn {key, value}, {:ok, acc} ->
      with {:ok, decoded_key} <- decode_decoded_value(key),
           :ok <- validate_map_key(decoded_key, :query_generic),
           {:ok, decoded_value} <- decode_decoded_value(value) do
        {:cont, {:ok, [{decoded_key, decoded_value} | acc]}}
      else
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp table_value(pairs) do
    pairs = Enum.reverse(pairs)
    keys = Enum.map(pairs, &elem(&1, 0))

    if contiguous_array_keys?(keys) do
      {:ok, pairs |> Enum.sort_by(&elem(&1, 0)) |> Enum.map(&elem(&1, 1))}
    else
      {:ok, Map.new(pairs)}
    end
  end

  defp contiguous_array_keys?([]), do: true

  defp contiguous_array_keys?(keys) do
    Enum.all?(keys, &is_integer/1) and Enum.sort(keys) == Enum.to_list(1..length(keys))
  end

  defp list_value(values) when is_list(values), do: values
  defp list_value(_other), do: :invalid

  defp one_result([]), do: {:ok, nil}
  defp one_result([result]), do: {:ok, result}

  defp one_result(:invalid),
    do: query_error("aggregate Lua finalizer returned an invalid result set")

  defp one_result(_results),
    do: query_error("aggregate Lua finalizer returned multiple results")

  defp invalid_argument(message), do: error(:invalid_argument, message)
  defp query_error(message), do: error(:query_generic, message)

  defp error(code, message) do
    {:error, Error.from_result_code(code, message: message)}
  end

  @support_source """
  local SCOPE_SERVER = 1
  local SCOPE_CLIENT = 2
  local SCOPE_EITHER = 3
  local SCOPE_BOTH = 4

  local function unsupported_helper(name)
    error("unsupported Aerospike Lua helper: " .. name, 2)
  end

  local function clone(value)
    if type(value) ~= "table" then
      return value
    end

    local out = {}
    for key, nested in pairs(value) do
      out[key] = clone(nested)
    end
    return out
  end

  local function stream_from_values(values)
    local i = 0

    return function()
      i = i + 1
      return values[i]
    end
  end

  local function filter(next_value, predicate)
    local done = false

    return function()
      if done then
        return nil
      end

      while true do
        local value = next_value()

        if value == nil then
          done = true
          return nil
        end

        if predicate(value) then
          return value
        end
      end
    end
  end

  local function transform(next_value, mapper)
    local done = false

    return function()
      if done then
        return nil
      end

      local value = next_value()

      if value == nil then
        done = true
        return nil
      end

      return mapper(value)
    end
  end

  local function reduce(next_value, reducer)
    local done = false

    return function()
      if done then
        return nil
      end

      local value = next_value()

      if value ~= nil then
        while true do
          local next = next_value()

          if next == nil then
            break
          end

          value = reducer(value, next)
        end
      end

      done = true
      return value
    end
  end

  local function aggregate(next_value, init, aggregator)
    local done = false

    return function()
      if done then
        return nil
      end

      local value = clone(init)

      while true do
        local next = next_value()

        if next == nil then
          break
        end

        value = aggregator(value, next)
      end

      done = true
      return value
    end
  end

  local function stream_ops_create()
    local self = {ops = {}}

    function self:aggregate(...)
      table.insert(self.ops, {scope = SCOPE_SERVER, func = aggregate, args = {...}})
      return self
    end

    function self:reduce(...)
      table.insert(self.ops, {scope = SCOPE_BOTH, func = reduce, args = {...}})
      return self
    end

    function self:map(...)
      table.insert(self.ops, {scope = SCOPE_EITHER, func = transform, args = {...}})
      return self
    end

    function self:filter(...)
      table.insert(self.ops, {scope = SCOPE_EITHER, func = filter, args = {...}})
      return self
    end

    function self:groupby()
      unsupported_helper("groupby")
    end

    return self
  end

  local function select_ops(stream_ops, scope)
    local server_ops = {}
    local client_ops = {}
    local phase = SCOPE_SERVER

    for _, op in ipairs(stream_ops) do
      if phase == SCOPE_SERVER then
        if op.scope == SCOPE_SERVER or op.scope == SCOPE_EITHER then
          table.insert(server_ops, op)
        elseif op.scope == SCOPE_BOTH then
          table.insert(server_ops, op)
          table.insert(client_ops, op)
          phase = SCOPE_CLIENT
        end
      elseif phase == SCOPE_CLIENT then
        table.insert(client_ops, op)
      end
    end

    if scope == SCOPE_CLIENT then
      return client_ops
    end

    return server_ops
  end

  local function apply_ops(values_stream, ops)
    local stream = values_stream

    for _, op in ipairs(ops) do
      stream = op.func(stream, table.unpack(op.args)) or stream
    end

    return stream
  end

  function map(value)
    return value or {}
  end

  function list()
    unsupported_helper("list")
  end

  function bytes()
    unsupported_helper("bytes")
  end

  function trace()
  end

  function debug()
  end

  function info()
  end

  function warn()
  end

  aerospike = {
    log = function()
    end
  }

  function __as_finalize(function_name, values, args)
    local aggregate_function = _G[function_name]

    if type(aggregate_function) ~= "function" then
      error("aggregate function not found: " .. tostring(function_name), 2)
    end

    local stream_ops = stream_ops_create()
    local result = aggregate_function(stream_ops, table.unpack(args))

    if type(result) ~= "table" or type(result.ops) ~= "table" then
      error("aggregate function must return a stream object", 2)
    end

    local ops = select_ops(result.ops, SCOPE_CLIENT)
    local out = {}
    local out_stream = apply_ops(stream_from_values(values), ops)

    while true do
      local value = out_stream()

      if value == nil then
        break
      end

      table.insert(out, value)
    end

    return out
  end
  """

  @prepare_source """
  local aggregate_function = _G[__as_function]

  if type(aggregate_function) ~= "function" then
    error("aggregate function not found: " .. tostring(__as_function), 2)
  end

  local stream_ops = stream_ops_create()
  local result = aggregate_function(stream_ops, table.unpack(__as_args))

  if type(result) ~= "table" or type(result.ops) ~= "table" then
    error("aggregate function must return a stream object", 2)
  end

  return true
  """

  @finalize_source """
  return __as_finalize(__as_function, __as_values, __as_args)
  """

  defp support_source, do: @support_source
  defp prepare_source, do: @prepare_source
  defp finalize_source, do: @finalize_source
end

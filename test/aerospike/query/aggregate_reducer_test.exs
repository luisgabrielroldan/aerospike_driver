defmodule Aerospike.Query.AggregateReducerTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Geo
  alias Aerospike.Query.AggregateReducer

  @sum_source """
  local function reducer(left, right)
    return left + right
  end

  function sum_values(stream)
    return stream : reduce(reducer)
  end
  """

  test "reduces server aggregate values with inline source" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    assert AggregateReducer.run(reducer, [1, 2, 3]) == {:ok, 6}
  end

  test "loads source from a local path" do
    path =
      Path.join(System.tmp_dir!(), "aggregate_reducer_#{System.unique_integer([:positive])}.lua")

    File.write!(path, @sum_source)
    on_exit(fn -> File.rm(path) end)

    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source_path: path)

    assert AggregateReducer.run(reducer, [4, 5]) == {:ok, 9}
  end

  test "validates source options" do
    assert {:error, %Error{code: :invalid_argument, message: missing}} =
             AggregateReducer.prepare("pkg", "sum_values", [], [])

    assert missing =~ "missing aggregate Lua source"

    assert {:error, %Error{code: :invalid_argument, message: both}} =
             AggregateReducer.prepare("pkg", "sum_values", [],
               source: @sum_source,
               source_path: "/tmp/source.lua"
             )

    assert both =~ "exactly one"

    assert {:error, %Error{code: :invalid_argument, message: non_binary}} =
             AggregateReducer.prepare("pkg", "sum_values", [], source: :bad)

    assert non_binary =~ "must be a binary"

    assert {:error, %Error{code: :invalid_argument, message: unreadable}} =
             AggregateReducer.prepare("pkg", "sum_values", [], source_path: "/no/such/source.lua")

    assert unreadable =~ "unable to read aggregate Lua source"

    assert {:error, %Error{code: :invalid_argument, message: non_binary_path}} =
             AggregateReducer.prepare("pkg", "sum_values", [], source_path: :bad)

    assert non_binary_path =~ "source_path must be a path binary"
  end

  test "falls back to the default timeout for invalid timeout options" do
    assert {:ok, reducer} =
             AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source, timeout: 0)

    assert reducer.timeout == 5_000
  end

  test "validates unsupported local arguments before reduction" do
    assert {:error, %Error{code: :invalid_argument, message: message}} =
             AggregateReducer.prepare("pkg", "sum_values", [{:blob, <<1>>}], source: @sum_source)

    assert message =~ "blob values"
  end

  test "rejects unsupported geo local arguments before reduction" do
    values = [
      Geo.point(1, 2),
      Geo.circle(1, 2, 3),
      Geo.polygon([[{0, 0}, {1, 0}, {1, 1}, {0, 0}]]),
      {:geojson, ~s({"type":"Point","coordinates":[1,2]})}
    ]

    Enum.each(values, fn value ->
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               AggregateReducer.prepare("pkg", "sum_values", [value], source: @sum_source)

      assert message =~ "geo values"
    end)
  end

  test "rejects unsupported node targeting" do
    assert {:error, %Error{code: :invalid_argument, message: message}} =
             AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source, node: "A1")

    assert message =~ ":node"
  end

  test "maps local Lua load and missing function errors to query errors" do
    assert {:error, %Error{code: :query_generic, message: bad_source}} =
             AggregateReducer.prepare("pkg", "sum_values", [], source: "function bad(")

    assert bad_source =~ "aggregate Lua execution failed"

    assert {:error, %Error{code: :query_generic, message: missing_function}} =
             AggregateReducer.prepare("pkg", "missing", [], source: @sum_source)

    assert missing_function =~ "aggregate Lua execution failed"
  end

  test "converts maps, lists, and function arguments across the Lua boundary" do
    source = """
    function total_named(stream, name)
      local function reducer(left, right)
        return {
          sum = left[name] + right[name],
          count = left["count"] + right["count"],
          labels = {left["labels"][1], right["labels"][1]}
        }
      end

      return stream : reduce(reducer)
    end
    """

    assert {:ok, reducer} =
             AggregateReducer.prepare("pkg", "total_named", ["score"], source: source)

    values = [
      %{"score" => 3, "count" => 1, "labels" => ["a"]},
      %{"score" => 5, "count" => 1, "labels" => ["b"]}
    ]

    assert AggregateReducer.run(reducer, values) ==
             {:ok, %{"count" => 2, "labels" => ["a", "b"], "sum" => 8}}
  end

  test "runs reduce, map, and filter helpers during final reduction" do
    source = """
    function final_total(stream)
      return stream
        : reduce(function(total, value) return total + value end)
        : map(function(value) return value * 2 end)
        : filter(function(value) return value > 10 end)
    end
    """

    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "final_total", [], source: source)

    assert AggregateReducer.run(reducer, [1, 2, 3]) == {:ok, 12}
  end

  test "decodes scalar and contiguous array outputs from Lua" do
    scalar_source = """
    function scalar(stream)
      return stream : reduce(function(_left, _right) return true end)
    end
    """

    array_source = """
    function array_value(stream)
      return stream : reduce(function(_left, _right) return {"a", false, 3.5} end)
    end
    """

    assert {:ok, scalar_reducer} =
             AggregateReducer.prepare("pkg", "scalar", [], source: scalar_source)

    assert {:ok, array_reducer} =
             AggregateReducer.prepare("pkg", "array_value", [], source: array_source)

    assert AggregateReducer.run(scalar_reducer, [1, 2]) == {:ok, true}
    assert AggregateReducer.run(array_reducer, [1, 2]) == {:ok, ["a", false, 3.5]}
  end

  test "returns nil for empty final output" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    assert AggregateReducer.run(reducer, []) == {:ok, nil}
  end

  test "rejects multiple final outputs" do
    source = """
    function passthrough(stream)
      return stream : map(function(value) return value end)
    end
    """

    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "passthrough", [], source: source)

    assert {:error, %Error{code: :query_generic, message: message}} =
             AggregateReducer.run(reducer, [1, 2])

    assert message =~ "multiple results"
  end

  test "maps unsupported helpers and runtime errors to query errors" do
    unsupported = """
    function bad(stream)
      return stream : groupby(function(value) return value end)
    end
    """

    assert {:error, %Error{code: :query_generic}} =
             AggregateReducer.prepare("pkg", "bad", [], source: unsupported)

    runtime_error = """
    function bad(stream)
      return stream : reduce(function(_left, _right) error("boom") end)
    end
    """

    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "bad", [], source: runtime_error)

    assert {:error, %Error{code: :query_generic, message: message}} =
             AggregateReducer.run(reducer, [1, 2])

    assert message =~ "aggregate Lua execution failed"
  end

  test "rejects unsupported stream values" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    assert {:error, %Error{code: :query_generic, message: message}} =
             AggregateReducer.run(reducer, [{:blob, <<1>>}])

    assert message =~ "blob values"
  end

  test "propagates stream error tuples without remapping them" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    assert {:error, %Error{code: :network_error}} =
             AggregateReducer.run(reducer, [{:error, Error.from_result_code(:network_error)}])
  end

  test "rejects unsupported nested stream values and map keys" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    assert {:error, %Error{code: :query_generic, message: nested_value}} =
             AggregateReducer.run(reducer, [%{"bad" => {:raw, 1, <<1>>}}])

    assert nested_value =~ "raw particles"

    assert {:error, %Error{code: :query_generic, message: bad_key}} =
             AggregateReducer.run(reducer, [%{{:tuple, :key} => 1}])

    assert bad_key =~ "unsupported aggregate Lua map key"
  end

  test "rejects unsupported Lua output shapes" do
    unsupported_key_source = """
    function bad_key(stream)
      return stream : reduce(function(_left, _right) return {[{}] = 1} end)
    end
    """

    unsupported_list_source = """
    function bad_list(stream)
      return stream : reduce(function(_left, _right) return function() end end)
    end
    """

    assert {:ok, key_reducer} =
             AggregateReducer.prepare("pkg", "bad_key", [], source: unsupported_key_source)

    assert {:ok, list_reducer} =
             AggregateReducer.prepare("pkg", "bad_list", [], source: unsupported_list_source)

    assert {:error, %Error{code: :query_generic, message: key_message}} =
             AggregateReducer.run(key_reducer, [1, 2])

    assert key_message =~ "unsupported aggregate Lua map key"

    assert {:error, %Error{code: :query_generic, message: list_message}} =
             AggregateReducer.run(list_reducer, [1, 2])

    assert list_message =~ "unsupported aggregate Lua output"
  end

  test "propagates stream errors without remapping them" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    stream =
      Stream.map([:raise], fn :raise ->
        raise Error.from_result_code(:network_error)
      end)

    assert {:error, %Error{code: :network_error}} = AggregateReducer.run(reducer, stream)
  end

  test "times out local reduction" do
    source = """
    function spin(stream)
      return stream : reduce(function(left, right)
        while true do
        end

        return left + right
      end)
    end
    """

    assert {:ok, reducer} =
             AggregateReducer.prepare("pkg", "spin", [], source: source, timeout: 50)

    assert {:error, %Error{code: :timeout}} = AggregateReducer.run(reducer, [1, 2])
  end
end

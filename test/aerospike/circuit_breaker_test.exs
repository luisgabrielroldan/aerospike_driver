defmodule Aerospike.CircuitBreakerTest do
  use ExUnit.Case, async: true

  alias Aerospike.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.Tables

  @increment_event [:aerospike, :circuit_breaker, :increment]
  @reject_event [:aerospike, :circuit_breaker, :reject]
  @reset_event [:aerospike, :circuit_breaker, :reset]

  setup do
    conn = :"test_cb_#{System.unique_integer([:positive])}"
    meta_table = :ets.new(Tables.meta(conn), [:set, :public, :named_table])

    breaker_table =
      :ets.new(Tables.breaker(conn), [:set, :public, :named_table, write_concurrency: true])

    :ets.insert(meta_table, {:breaker_config, %{max_error_rate: 2, error_rate_window: 2}})

    on_exit(fn ->
      if :ets.whereis(meta_table) != :undefined, do: :ets.delete(meta_table)
      if :ets.whereis(breaker_table) != :undefined, do: :ets.delete(breaker_table)
    end)

    %{conn: conn}
  end

  describe "enabled?/1" do
    test "returns false when max_error_rate is 0", %{conn: conn} do
      put_breaker_config(conn, max_error_rate: 0, error_rate_window: 2)
      refute CircuitBreaker.enabled?(conn)
    end
  end

  describe "allow_request?/2" do
    test "returns :ok when disabled", %{conn: conn} do
      put_breaker_config(conn, max_error_rate: 0, error_rate_window: 2)
      assert :ok = CircuitBreaker.allow_request?(conn, "node-a")
    end

    test "returns :ok when count is below threshold", %{conn: conn} do
      :ets.insert(Tables.breaker(conn), {"node-a", 1})
      assert :ok = CircuitBreaker.allow_request?(conn, "node-a")
    end

    test "returns max_error_rate error at threshold and emits reject telemetry", %{conn: conn} do
      :ets.insert(Tables.breaker(conn), {"node-a", 2})
      attach_telemetry_handler(@reject_event)

      assert {:error, %Error{code: :max_error_rate, node: "node-a"}} =
               CircuitBreaker.allow_request?(conn, "node-a")

      assert_receive {:telemetry_event, @reject_event, %{count: 2, max_error_rate: 2},
                      %{conn: ^conn, node: "node-a"}}
    end
  end

  describe "record_error/3" do
    test "increments counter and emits increment telemetry", %{conn: conn} do
      attach_telemetry_handler(@increment_event)

      assert :ok = CircuitBreaker.record_error(conn, "node-a", :network_error)
      assert :ok = CircuitBreaker.record_error(conn, "node-a", :timeout)

      assert [{"node-a", 2}] = :ets.lookup(Tables.breaker(conn), "node-a")

      assert_receive {:telemetry_event, @increment_event, %{count: 1},
                      %{conn: ^conn, node: "node-a", reason: :network_error}}

      assert_receive {:telemetry_event, @increment_event, %{count: 2},
                      %{conn: ^conn, node: "node-a", reason: :timeout}}
    end

    test "is a no-op when disabled", %{conn: conn} do
      put_breaker_config(conn, max_error_rate: 0, error_rate_window: 2)
      assert :ok = CircuitBreaker.record_error(conn, "node-a", :network_error)
      assert [] = :ets.lookup(Tables.breaker(conn), "node-a")
    end
  end

  describe "maybe_reset_window/3" do
    test "resets counters at boundary and emits reset telemetry", %{conn: conn} do
      attach_telemetry_handler(@reset_event)
      :ets.insert(Tables.breaker(conn), {"node-a", 3})
      :ets.insert(Tables.breaker(conn), {"node-b", 0})

      assert :ok = CircuitBreaker.maybe_reset_window(conn, 4, 2)
      assert [] = :ets.tab2list(Tables.breaker(conn))

      assert_receive {:telemetry_event, @reset_event, %{count_before_reset: 3},
                      %{conn: ^conn, node: "node-a"}}

      refute_receive {:telemetry_event, @reset_event, _, %{node: "node-b"}}
    end

    test "does not reset when not at boundary", %{conn: conn} do
      :ets.insert(Tables.breaker(conn), {"node-a", 3})
      assert :ok = CircuitBreaker.maybe_reset_window(conn, 3, 2)
      assert [{"node-a", 3}] = :ets.lookup(Tables.breaker(conn), "node-a")
    end
  end

  defp put_breaker_config(conn,
         max_error_rate: max_error_rate,
         error_rate_window: error_rate_window
       ) do
    :ets.insert(
      Tables.meta(conn),
      {:breaker_config, %{max_error_rate: max_error_rate, error_rate_window: error_rate_window}}
    )
  end

  defp attach_telemetry_handler(event_name) do
    handler_id = {:circuit_breaker_test, event_name, System.unique_integer([:positive])}
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        event_name,
        &__MODULE__.forward_telemetry/4,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  def forward_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry_event, event, measurements, metadata})
  end
end

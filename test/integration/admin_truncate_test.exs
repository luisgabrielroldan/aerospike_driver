defmodule Aerospike.Integration.AdminTruncateTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.Key
  alias Aerospike.Op
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3000
  @namespace "test"

  setup do
    IntegrationSupport.probe_aerospike!(@host, @port)
    IntegrationSupport.wait_for_seed_ready!(@host, @port, @namespace, 5_000)
    name = IntegrationSupport.unique_atom("spike_admin_truncate_cluster")

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2
      )

    IntegrationSupport.wait_for_tender_ready!(name, 5_000)

    on_exit(fn ->
      IntegrationSupport.stop_supervisor_quietly(sup)
    end)

    %{cluster: name}
  end

  test "truncate/3 respects the optional before cutoff", %{cluster: cluster} do
    older_set = IntegrationSupport.unique_name("truncate_ns")
    newer_set = "#{older_set}_new"
    older_key = Key.new(@namespace, older_set, "older")
    newer_key = Key.new(@namespace, newer_set, "newer")

    assert {:ok, _metadata} = Aerospike.put(cluster, older_key, %{"v" => 1})
    before = truncate_cutoff_after_lut!(cluster, older_key)
    put_after_cutoff!(cluster, newer_key, %{"v" => 2}, before)

    assert_truncate_accepted!(cluster, @namespace, before)

    IntegrationSupport.assert_eventually(
      "namespace truncate cutoff applied",
      fn ->
        older_state = Aerospike.get(cluster, older_key)
        newer_state = Aerospike.get(cluster, newer_key)

        match?({:error, %Error{code: :key_not_found}}, older_state) and
          match?({:ok, _}, newer_state)
      end,
      truncate_wait_timeout_ms(),
      200
    )
  end

  test "truncate/4 truncates only the targeted set", %{cluster: cluster} do
    set = IntegrationSupport.unique_name("truncate_set")
    other_set = "#{set}_keep"
    trunc_key = Key.new(@namespace, set, "trunc")
    keep_key = Key.new(@namespace, other_set, "keep")

    assert {:ok, _metadata} = Aerospike.put(cluster, trunc_key, %{"v" => 1})
    assert {:ok, _metadata} = Aerospike.put(cluster, keep_key, %{"v" => 2})

    before = truncate_cutoff_after_lut!(cluster, trunc_key)
    assert_truncate_accepted!(cluster, @namespace, set, before)

    IntegrationSupport.assert_eventually(
      "set truncate applied",
      fn ->
        trunc_state = Aerospike.get(cluster, trunc_key)
        keep_state = Aerospike.get(cluster, keep_key)

        match?({:error, %Error{code: :key_not_found}}, trunc_state) and
          match?({:ok, _}, keep_state)
      end,
      truncate_wait_timeout_ms(),
      200
    )
  end

  defp truncate_wait_timeout_ms, do: 15_000

  defp truncate_cutoff_after_lut!(cluster, key) do
    assert {:ok, %{bins: %{"last_update" => last_update}}} =
             Aerospike.operate(cluster, key, [
               Op.Exp.read("last_update", Exp.last_update())
             ])

    DateTime.from_unix!(last_update + 1_000_000, :nanosecond)
  end

  defp put_after_cutoff!(cluster, key, bins, cutoff) do
    IntegrationSupport.assert_eventually(
      "record written after truncate cutoff",
      fn ->
        assert {:ok, _metadata} = Aerospike.put(cluster, key, bins)

        assert {:ok, %{bins: %{"last_update" => last_update}}} =
                 Aerospike.operate(cluster, key, [
                   Op.Exp.read("last_update", Exp.last_update())
                 ])

        DateTime.compare(DateTime.from_unix!(last_update, :nanosecond), cutoff) == :gt
      end,
      truncate_wait_timeout_ms(),
      50
    )
  end

  defp assert_truncate_accepted!(cluster, namespace, before) do
    IntegrationSupport.assert_eventually(
      "namespace truncate cutoff accepted",
      fn ->
        case Aerospike.truncate(cluster, namespace, before: before) do
          :ok ->
            true

          {:error, %Error{message: message}} = error ->
            if String.contains?(message, "would truncate in the future") do
              false
            else
              flunk("truncate failed unexpectedly: #{inspect(error)}")
            end
        end
      end,
      truncate_wait_timeout_ms(),
      50
    )
  end

  defp assert_truncate_accepted!(cluster, namespace, set, before) do
    IntegrationSupport.assert_eventually(
      "set truncate cutoff accepted",
      fn ->
        case Aerospike.truncate(cluster, namespace, set, before: before) do
          :ok ->
            true

          {:error, %Error{message: message}} = error ->
            if String.contains?(message, "would truncate in the future") do
              false
            else
              flunk("truncate failed unexpectedly: #{inspect(error)}")
            end
        end
      end,
      truncate_wait_timeout_ms(),
      50
    )
  end
end

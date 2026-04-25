defmodule Aerospike.Integration.XdrFilterTest do
  @moduledoc """
  Exercises XDR filter set and clear commands against an Enterprise Edition
  server that is configured for XDR.

  This test is opt-in because the checked-in compose profiles provide
  Enterprise Edition services, but do not configure an XDR topology. To run it,
  point the environment at a server with XDR enabled:

      AEROSPIKE_XDR_ENABLED=true \
      AEROSPIKE_XDR_HOST=127.0.0.1 \
      AEROSPIKE_XDR_PORT=3100 \
      AEROSPIKE_XDR_DC=dc-west \
      AEROSPIKE_XDR_NAMESPACE=test \
      mix test test/integration/xdr_filter_test.exs --include integration --include enterprise --seed 0

  Tagged `:integration` and `:enterprise` so it is excluded from the default
  suite. When the opt-in environment is absent, ExUnit reports the file as
  skipped instead of failing Community Edition runs.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :enterprise

  alias Aerospike.Exp
  alias Aerospike.Test.IntegrationSupport

  @xdr_enabled? System.get_env("AEROSPIKE_XDR_ENABLED") in ["1", "true", "TRUE", "yes", "YES"]

  unless @xdr_enabled? do
    @moduletag skip:
                 "set AEROSPIKE_XDR_ENABLED=true and point AEROSPIKE_XDR_HOST/PORT at an Enterprise XDR profile"
  end

  @host System.get_env("AEROSPIKE_XDR_HOST", System.get_env("AEROSPIKE_EE_HOST", "127.0.0.1"))
  @port System.get_env("AEROSPIKE_XDR_PORT", System.get_env("AEROSPIKE_EE_PORT", "3100"))
        |> String.to_integer()
  @namespace System.get_env("AEROSPIKE_XDR_NAMESPACE", "test")
  @datacenter System.get_env("AEROSPIKE_XDR_DC")

  setup_all do
    IntegrationSupport.probe_aerospike!(
      @host,
      @port,
      "Run an Enterprise server with XDR configured, then set AEROSPIKE_XDR_ENABLED=true."
    )

    IntegrationSupport.wait_for_seed_ready!(@host, @port, @namespace, 5_000)

    name = IntegrationSupport.unique_atom("spike_xdr_filter")

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

    datacenter = configured_datacenter!()
    assert_xdr_available!(name, datacenter)

    %{cluster: name, datacenter: datacenter, namespace: @namespace}
  end

  test "sets and clears an XDR expression filter", %{
    cluster: cluster,
    datacenter: datacenter,
    namespace: namespace
  } do
    filter = Exp.eq(Exp.int_bin("xdr_filter_value"), Exp.int(1))

    assert :ok = Aerospike.set_xdr_filter(cluster, datacenter, namespace, filter)

    on_exit(fn ->
      _ = Aerospike.set_xdr_filter(cluster, datacenter, namespace, nil)
    end)

    assert :ok = Aerospike.set_xdr_filter(cluster, datacenter, namespace, nil)
  end

  defp configured_datacenter! do
    if is_binary(@datacenter) and @datacenter != "" do
      @datacenter
    else
      raise "AEROSPIKE_XDR_DC is required so the test targets a configured XDR datacenter"
    end
  end

  defp assert_xdr_available!(cluster, datacenter) do
    assert_xdr_command_ok!(cluster, "get-config:context=xdr")
    assert_xdr_command_ok!(cluster, "get-config:context=xdr;dc=#{datacenter}")
  end

  defp assert_xdr_command_ok!(cluster, command) do
    case Aerospike.info(cluster, command) do
      {:ok, response} when is_binary(response) and response != "" ->
        refute String.starts_with?(response, "ERROR"),
               "#{command} returned an error response: #{inspect(response)}"

      {:ok, response} ->
        flunk("#{command} did not return XDR configuration: #{inspect(response)}")

      {:error, error} ->
        flunk("#{command} failed while probing XDR configuration: #{inspect(error)}")
    end
  end
end

defmodule Demo.Examples.XdrFilter do
  @moduledoc """
  Demonstrates Enterprise XDR expression filter setup and clearing.

  This validates command setup and clearing, not cross-datacenter replication.
  When the Enterprise endpoint is unavailable or rejects the XDR command, this
  example reports a skip instead of failing the normal demo run.
  """

  require Logger

  alias Aerospike.Error
  alias Aerospike.Exp

  @repo Demo.EnterpriseRepo
  @default_datacenter "dc-west"
  @default_namespace "test"

  def run do
    {host, port} = enterprise_endpoint()

    if reachable?(host, port) do
      run_xdr_filter(datacenter(), namespace())
    else
      Logger.warning(
        "  XdrFilter: skipped -- Enterprise endpoint not reachable on #{host}:#{port}"
      )

      :skipped
    end
  end

  defp run_xdr_filter(datacenter, namespace) do
    Logger.info("  Setting XDR expression filter for #{datacenter}/#{namespace}...")

    filter = Exp.eq(Exp.int_bin("replicate"), Exp.int(1))

    case @repo.set_xdr_filter(datacenter, namespace, filter) do
      :ok ->
        Logger.info("    XDR filter set.")
        clear_filter!(datacenter, namespace)
        Logger.info("    XDR filter cleared.")
        :ok

      {:error, %Error{code: :invalid_argument} = error} ->
        raise "XDR filter example built an invalid command: #{Exception.message(error)}"

      {:error, %Error{} = error} ->
        Logger.warning("  XdrFilter: skipped -- XDR filter command rejected: #{error.message}")
        :skipped

      {:error, reason} ->
        Logger.warning(
          "  XdrFilter: skipped -- XDR filter command unavailable: #{inspect(reason)}"
        )

        :skipped
    end
  end

  defp clear_filter!(datacenter, namespace) do
    case @repo.set_xdr_filter(datacenter, namespace, nil) do
      :ok ->
        :ok

      {:error, %Error{} = error} ->
        raise "XDR filter clear failed after setup: #{Exception.message(error)}"

      {:error, reason} ->
        raise "XDR filter clear failed after setup: #{inspect(reason)}"
    end
  end

  defp datacenter do
    System.get_env("AEROSPIKE_XDR_DC", @default_datacenter)
  end

  defp namespace do
    System.get_env("AEROSPIKE_XDR_NAMESPACE", @default_namespace)
  end

  defp enterprise_endpoint do
    :demo
    |> Application.fetch_env!(@repo)
    |> Keyword.fetch!(:hosts)
    |> List.first()
    |> parse_host()
  end

  defp parse_host(host) do
    case String.split(host, ":", parts: 2) do
      [hostname, port] ->
        {port, ""} = Integer.parse(port)
        {hostname, port}

      [hostname] ->
        {hostname, 3000}
    end
  end

  defp reachable?(host, port) do
    case :gen_tcp.connect(String.to_charlist(host), port, [], 1_000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _reason} ->
        false
    end
  end
end

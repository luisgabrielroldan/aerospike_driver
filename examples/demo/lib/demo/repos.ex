defmodule Demo.Repo do
  @moduledoc false

  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Query
  alias Aerospike.Record
  alias Aerospike.Scan
  alias Aerospike.Transport.Tcp

  @ready_timeout_ms 5_000
  @ready_interval_ms 100

  defmacro __using__(opts) do
    name = Keyword.fetch!(opts, :name)

    quote bind_quoted: [name: name] do
      @cluster name

      def child_spec(_opts) do
        Demo.Repo.child_spec(__MODULE__, @cluster)
      end

      def cluster, do: @cluster

      def put(key, bins, opts \\ []), do: Demo.Repo.put(@cluster, key, bins, opts)
      def put!(key, bins, opts \\ []), do: Demo.Repo.put!(@cluster, key, bins, opts)
      def add!(key, bins, opts \\ []), do: Demo.Repo.add!(@cluster, key, bins, opts)
      def append!(key, bins, opts \\ []), do: Demo.Repo.append!(@cluster, key, bins, opts)
      def prepend!(key, bins, opts \\ []), do: Demo.Repo.prepend!(@cluster, key, bins, opts)
      def touch!(key, opts \\ []), do: Demo.Repo.touch!(@cluster, key, opts)
      def get(key, opts \\ []), do: Demo.Repo.get(@cluster, key, opts)
      def exists(key, opts \\ []), do: Demo.Repo.exists(@cluster, key, opts)
      def delete(key, opts \\ []), do: Demo.Repo.delete(@cluster, key, opts)

      def operate(key, operations, opts \\ []),
        do: Demo.Repo.operate(@cluster, key, operations, opts)

      def operate!(key, operations, opts \\ []),
        do: Demo.Repo.operate!(@cluster, key, operations, opts)

      def batch_exists(keys, opts \\ []), do: Demo.Repo.batch_exists(@cluster, keys, opts)
      def batch_get(keys, opts \\ []), do: Demo.Repo.batch_get(@cluster, keys, opts)

      def batch_get_operate(keys, operations, opts \\ []),
        do: Demo.Repo.batch_get_operate(@cluster, keys, operations, opts)

      def batch_delete(keys, opts \\ []), do: Demo.Repo.batch_delete(@cluster, keys, opts)

      def batch_udf(keys, package, function, args, opts \\ []),
        do: Demo.Repo.batch_udf(@cluster, keys, package, function, args, opts)

      def batch_operate(entries, opts \\ []), do: Demo.Repo.batch_operate(@cluster, entries, opts)

      def all(scannable, opts \\ []), do: Demo.Repo.all(@cluster, scannable, opts)
      def count(scannable, opts \\ []), do: Demo.Repo.count(@cluster, scannable, opts)
      def stream!(scannable, opts \\ []), do: Demo.Repo.stream!(@cluster, scannable, opts)
      def page(scannable, opts \\ []), do: Demo.Repo.page(@cluster, scannable, opts)

      def query_aggregate_result(query, package, function, args, opts \\ []),
        do: Demo.Repo.query_aggregate_result(@cluster, query, package, function, args, opts)

      def query_execute(query, ops, opts \\ []),
        do: Demo.Repo.query_execute(@cluster, query, ops, opts)

      def query_udf(query, package, function, args, opts \\ []),
        do: Demo.Repo.query_udf(@cluster, query, package, function, args, opts)

      def info(command, opts \\ []), do: Demo.Repo.info(@cluster, command, opts)

      def info_node(node_name, command, opts \\ []),
        do: Demo.Repo.info_node(@cluster, node_name, command, opts)

      def nodes, do: Demo.Repo.nodes(@cluster)
      def node_names, do: Demo.Repo.node_names(@cluster)
      def metrics_enabled?, do: Demo.Repo.metrics_enabled?(@cluster)
      def enable_metrics(opts \\ []), do: Demo.Repo.enable_metrics(@cluster, opts)
      def disable_metrics, do: Demo.Repo.disable_metrics(@cluster)
      def stats, do: Demo.Repo.stats(@cluster)
      def warm_up(opts \\ []), do: Demo.Repo.warm_up(@cluster, opts)

      def create_index(namespace, set, opts \\ []),
        do: Demo.Repo.create_index(@cluster, namespace, set, opts)

      def drop_index(namespace, index_name, opts \\ []),
        do: Demo.Repo.drop_index(@cluster, namespace, index_name, opts)

      def register_udf(source, server_name, opts \\ []),
        do: Demo.Repo.register_udf(@cluster, source, server_name, opts)

      def remove_udf(server_name, opts \\ []),
        do: Demo.Repo.remove_udf(@cluster, server_name, opts)

      def apply_udf(key, package, function, args, opts \\ []),
        do: Demo.Repo.apply_udf(@cluster, key, package, function, args, opts)

      def truncate(namespace, set, opts \\ []),
        do: Demo.Repo.truncate(@cluster, namespace, set, opts)

      def set_xdr_filter(datacenter, namespace, filter),
        do: Demo.Repo.set_xdr_filter(@cluster, datacenter, namespace, filter)

      def create_user(user_name, password, roles, opts \\ []),
        do: Demo.Repo.create_user(@cluster, user_name, password, roles, opts)

      def create_pki_user(user_name, roles, opts \\ []),
        do: Demo.Repo.create_pki_user(@cluster, user_name, roles, opts)

      def drop_user(user_name, opts \\ []),
        do: Demo.Repo.drop_user(@cluster, user_name, opts)

      def change_password(user_name, password, opts \\ []),
        do: Demo.Repo.change_password(@cluster, user_name, password, opts)

      def grant_roles(user_name, roles, opts \\ []),
        do: Demo.Repo.grant_roles(@cluster, user_name, roles, opts)

      def revoke_roles(user_name, roles, opts \\ []),
        do: Demo.Repo.revoke_roles(@cluster, user_name, roles, opts)

      def query_user(user_name, opts \\ []),
        do: Demo.Repo.query_user(@cluster, user_name, opts)

      def query_users(opts \\ []), do: Demo.Repo.query_users(@cluster, opts)

      def create_role(role_name, privileges, opts \\ []),
        do: Demo.Repo.create_role(@cluster, role_name, privileges, opts)

      def drop_role(role_name, opts \\ []),
        do: Demo.Repo.drop_role(@cluster, role_name, opts)

      def set_whitelist(role_name, whitelist, opts \\ []),
        do: Demo.Repo.set_whitelist(@cluster, role_name, whitelist, opts)

      def set_quotas(role_name, read_quota, write_quota, opts \\ []),
        do: Demo.Repo.set_quotas(@cluster, role_name, read_quota, write_quota, opts)

      def grant_privileges(role_name, privileges, opts \\ []),
        do: Demo.Repo.grant_privileges(@cluster, role_name, privileges, opts)

      def revoke_privileges(role_name, privileges, opts \\ []),
        do: Demo.Repo.revoke_privileges(@cluster, role_name, privileges, opts)

      def query_role(role_name, opts \\ []),
        do: Demo.Repo.query_role(@cluster, role_name, opts)

      def query_roles(opts \\ []), do: Demo.Repo.query_roles(@cluster, opts)

      def transaction(fun), do: Demo.Repo.transaction(@cluster, fun)
      def transaction(txn_or_opts, fun), do: Demo.Repo.transaction(@cluster, txn_or_opts, fun)
    end
  end

  def child_spec(module, cluster) do
    opts =
      :demo
      |> Application.fetch_env!(module)
      |> Keyword.put_new(:name, cluster)
      |> Keyword.put_new(:transport, Tcp)
      |> Keyword.put_new(:namespaces, ["test"])

    Aerospike.child_spec(opts)
  end

  def put(cluster, key, bins, opts),
    do: ready_call(cluster, fn -> Aerospike.put(cluster, key, bins, opts) end)

  def put!(cluster, key, bins, opts) do
    case put(cluster, key, bins, opts) do
      {:ok, _metadata} -> :ok
      {:error, %Error{} = err} -> raise err
      {:error, reason} -> raise "put failed: #{inspect(reason)}"
    end
  end

  def add!(cluster, key, bins, opts),
    do:
      bang_metadata(ready_call(cluster, fn -> Aerospike.add(cluster, key, bins, opts) end), "add")

  def append!(cluster, key, bins, opts),
    do:
      bang_metadata(
        ready_call(cluster, fn -> Aerospike.append(cluster, key, bins, opts) end),
        "append"
      )

  def prepend!(cluster, key, bins, opts),
    do:
      bang_metadata(
        ready_call(cluster, fn -> Aerospike.prepend(cluster, key, bins, opts) end),
        "prepend"
      )

  def touch!(cluster, key, opts),
    do: bang_metadata(ready_call(cluster, fn -> Aerospike.touch(cluster, key, opts) end), "touch")

  def get(cluster, key, opts) do
    if Keyword.get(opts, :header_only, false) do
      opts = Keyword.delete(opts, :header_only)
      ready_call(cluster, fn -> Aerospike.get_header(cluster, key, opts) end)
    else
      ready_call(cluster, fn -> Aerospike.get(cluster, key, :all, opts) end)
    end
  end

  def exists(cluster, key, opts),
    do: ready_call(cluster, fn -> Aerospike.exists(cluster, key, opts) end)

  def delete(cluster, key, opts),
    do: ready_call(cluster, fn -> Aerospike.delete(cluster, key, opts) end)

  def operate(cluster, key, operations, opts),
    do: ready_call(cluster, fn -> Aerospike.operate(cluster, key, operations, opts) end)

  def operate!(cluster, key, operations, opts) do
    case operate(cluster, key, operations, opts) do
      {:ok, %Record{} = record} -> record
      {:error, %Error{} = err} -> raise err
      {:error, reason} -> raise "operate failed: #{inspect(reason)}"
    end
  end

  def batch_exists(cluster, keys, opts) do
    with {:ok, results} <-
           ready_call(cluster, fn -> Aerospike.batch_exists(cluster, keys, opts) end) do
      {:ok, Enum.map(results, &unwrap_batch_exists/1)}
    end
  end

  def batch_get(cluster, keys, opts) do
    bins = Keyword.get(opts, :bins, :all)

    result =
      if Keyword.get(opts, :header_only, false) do
        opts = Keyword.drop(opts, [:bins, :header_only])
        ready_call(cluster, fn -> Aerospike.batch_get_header(cluster, keys, opts) end)
      else
        opts = Keyword.drop(opts, [:bins, :header_only])
        ready_call(cluster, fn -> Aerospike.batch_get(cluster, keys, bins, opts) end)
      end

    with {:ok, results} <- result do
      {:ok, Enum.map(results, &unwrap_batch_record/1)}
    end
  end

  def batch_operate(cluster, entries, opts),
    do: ready_call(cluster, fn -> Aerospike.batch_operate(cluster, entries, opts) end)

  def batch_get_operate(cluster, keys, operations, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.batch_get_operate(cluster, keys, operations, opts)
      end)

  def batch_delete(cluster, keys, opts),
    do: ready_call(cluster, fn -> Aerospike.batch_delete(cluster, keys, opts) end)

  def batch_udf(cluster, keys, package, function, args, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.batch_udf(cluster, keys, package, function, args, opts)
      end)

  def all(cluster, %Scan{} = scan, opts),
    do: ready_call(cluster, fn -> Aerospike.scan_all(cluster, scan, opts) end)

  def all(cluster, %Query{} = query, opts),
    do: ready_call(cluster, fn -> Aerospike.query_all(cluster, query, opts) end)

  def count(cluster, %Scan{} = scan, opts),
    do: ready_call(cluster, fn -> Aerospike.scan_count(cluster, scan, opts) end)

  def stream!(cluster, %Scan{} = scan, opts),
    do: ready_call(cluster, fn -> Aerospike.scan_stream!(cluster, scan, opts) end)

  def stream!(cluster, %Query{} = query, opts),
    do: ready_call(cluster, fn -> Aerospike.query_stream!(cluster, query, opts) end)

  def page(cluster, %Scan{} = scan, opts),
    do: ready_call(cluster, fn -> Aerospike.scan_page(cluster, scan, opts) end)

  def page(cluster, %Query{} = query, opts),
    do: ready_call(cluster, fn -> Aerospike.query_page(cluster, query, opts) end)

  def query_aggregate_result(cluster, %Query{} = query, package, function, args, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.query_aggregate_result(cluster, query, package, function, args, opts)
      end)

  def query_execute(cluster, %Query{} = query, ops, opts),
    do: ready_call(cluster, fn -> Aerospike.query_execute(cluster, query, ops, opts) end)

  def query_udf(cluster, %Query{} = query, package, function, args, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.query_udf(cluster, query, package, function, args, opts)
      end)

  def nodes(cluster) do
    with {:ok, nodes} <- ready_call(cluster, fn -> Aerospike.nodes(cluster) end) do
      {:ok, nodes}
    end
  end

  def node_names(cluster), do: ready_call(cluster, fn -> Aerospike.node_names(cluster) end)

  def metrics_enabled?(cluster),
    do: ready_call(cluster, fn -> Aerospike.metrics_enabled?(cluster) end)

  def enable_metrics(cluster, opts),
    do: ready_call(cluster, fn -> Aerospike.enable_metrics(cluster, opts) end)

  def disable_metrics(cluster),
    do: ready_call(cluster, fn -> Aerospike.disable_metrics(cluster) end)

  def stats(cluster),
    do: ready_call(cluster, fn -> Aerospike.stats(cluster) end)

  def warm_up(cluster, opts),
    do: ready_call(cluster, fn -> Aerospike.warm_up(cluster, opts) end)

  def info(cluster, command, opts),
    do: ready_call(cluster, fn -> Aerospike.info(cluster, command, opts) end)

  def info_node(cluster, node_name, command, opts),
    do: ready_call(cluster, fn -> Aerospike.info_node(cluster, node_name, command, opts) end)

  def create_index(cluster, namespace, set, opts),
    do: ready_call(cluster, fn -> Aerospike.create_index(cluster, namespace, set, opts) end)

  def drop_index(cluster, namespace, index_name, opts),
    do: ready_call(cluster, fn -> Aerospike.drop_index(cluster, namespace, index_name, opts) end)

  def register_udf(cluster, source, server_name, opts),
    do: ready_call(cluster, fn -> Aerospike.register_udf(cluster, source, server_name, opts) end)

  def remove_udf(cluster, server_name, opts),
    do: ready_call(cluster, fn -> Aerospike.remove_udf(cluster, server_name, opts) end)

  def apply_udf(cluster, key, package, function, args, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.apply_udf(cluster, key, package, function, args, opts)
      end)

  def truncate(cluster, namespace, set, opts),
    do: ready_call(cluster, fn -> Aerospike.truncate(cluster, namespace, set, opts) end)

  def set_xdr_filter(cluster, datacenter, namespace, filter),
    do:
      ready_call(cluster, fn ->
        Aerospike.set_xdr_filter(cluster, datacenter, namespace, filter)
      end)

  def create_user(cluster, user_name, password, roles, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.create_user(cluster, user_name, password, roles, opts)
      end)

  def create_pki_user(cluster, user_name, roles, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.create_pki_user(cluster, user_name, roles, opts)
      end)

  def drop_user(cluster, user_name, opts),
    do: ready_call(cluster, fn -> Aerospike.drop_user(cluster, user_name, opts) end)

  def change_password(cluster, user_name, password, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.change_password(cluster, user_name, password, opts)
      end)

  def grant_roles(cluster, user_name, roles, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.grant_roles(cluster, user_name, roles, opts)
      end)

  def revoke_roles(cluster, user_name, roles, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.revoke_roles(cluster, user_name, roles, opts)
      end)

  def query_user(cluster, user_name, opts),
    do: ready_call(cluster, fn -> Aerospike.query_user(cluster, user_name, opts) end)

  def query_users(cluster, opts),
    do: ready_call(cluster, fn -> Aerospike.query_users(cluster, opts) end)

  def create_role(cluster, role_name, privileges, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.create_role(cluster, role_name, privileges, opts)
      end)

  def drop_role(cluster, role_name, opts),
    do: ready_call(cluster, fn -> Aerospike.drop_role(cluster, role_name, opts) end)

  def set_whitelist(cluster, role_name, whitelist, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.set_whitelist(cluster, role_name, whitelist, opts)
      end)

  def set_quotas(cluster, role_name, read_quota, write_quota, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.set_quotas(cluster, role_name, read_quota, write_quota, opts)
      end)

  def grant_privileges(cluster, role_name, privileges, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.grant_privileges(cluster, role_name, privileges, opts)
      end)

  def revoke_privileges(cluster, role_name, privileges, opts),
    do:
      ready_call(cluster, fn ->
        Aerospike.revoke_privileges(cluster, role_name, privileges, opts)
      end)

  def query_role(cluster, role_name, opts),
    do: ready_call(cluster, fn -> Aerospike.query_role(cluster, role_name, opts) end)

  def query_roles(cluster, opts),
    do: ready_call(cluster, fn -> Aerospike.query_roles(cluster, opts) end)

  def transaction(cluster, fun),
    do: ready_call(cluster, fn -> Aerospike.transaction(cluster, fun) end)

  def transaction(cluster, txn_or_opts, fun),
    do: ready_call(cluster, fn -> Aerospike.transaction(cluster, txn_or_opts, fun) end)

  def wait_until_ready(cluster, timeout_ms \\ @ready_timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_until_ready(cluster, deadline, @ready_interval_ms)
  end

  defp wait_until_ready(cluster, deadline, interval_ms) do
    _ = Tender.tend_now(cluster)

    cond do
      Tender.ready?(cluster) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :cluster_not_ready}

      true ->
        Process.sleep(interval_ms)
        wait_until_ready(cluster, deadline, interval_ms)
    end
  end

  defp bang_metadata(result, operation) do
    case result do
      {:ok, _metadata} -> :ok
      {:error, %Error{} = err} -> raise err
      {:error, reason} -> raise "#{operation} failed: #{inspect(reason)}"
    end
  end

  defp ready_call(cluster, fun) when is_atom(cluster) and is_function(fun, 0) do
    case wait_until_ready(cluster) do
      :ok -> fun.()
      {:error, reason} -> {:error, reason}
    end
  end

  defp unwrap_batch_exists({:ok, value}), do: value
  defp unwrap_batch_exists({:error, %Error{code: :key_not_found}}), do: false
  defp unwrap_batch_exists({:error, reason}), do: raise("batch exists failed: #{inspect(reason)}")

  defp unwrap_batch_record({:ok, %Record{} = record}), do: record
  defp unwrap_batch_record({:error, %Error{code: :key_not_found}}), do: nil
  defp unwrap_batch_record({:error, reason}), do: raise("batch get failed: #{inspect(reason)}")
end

defmodule Demo.PrimaryClusterRepo do
  @moduledoc """
  Repo-shaped wrapper bound to the primary community cluster.

  Default endpoint: `localhost:3000`.
  """

  use Demo.Repo, name: :aero
end

defmodule Demo.EnterpriseRepo do
  @moduledoc """
  Repo-shaped wrapper bound to the enterprise cluster used for transaction examples.

  Default endpoint: `localhost:3100`.
  """

  use Demo.Repo, name: :aero_ee
end

defmodule Demo.SecurityRepo do
  @moduledoc """
  Repo-shaped wrapper bound to the security-enabled enterprise cluster.

  Default endpoint: `localhost:3200`.
  """

  use Demo.Repo, name: :aero_security
end

defmodule Demo.TlsClusterRepo do
  @moduledoc """
  Repo-shaped wrapper bound to the TLS endpoint.

  Default endpoint: `localhost:4333`.
  """

  use Demo.Repo, name: :aero_tls
end

defmodule Demo.MtlsClusterRepo do
  @moduledoc """
  Repo-shaped wrapper bound to the mTLS endpoint.

  Default endpoint: `localhost:4334`.
  """

  use Demo.Repo, name: :aero_mtls
end

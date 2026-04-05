defmodule Aerospike.Policy do
  @moduledoc false
  # Validates user-supplied options and translates them into wire-level AsmMsg
  # flags and fields. Each command type (write, read, delete, exists, touch) has
  # its own NimbleOptions schema and an `apply_*_policy/2` function that sets
  # the corresponding info2/info3 flag bits, generation, TTL, and timeout on
  # the outgoing message.

  import Bitwise

  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation

  # -- NimbleOptions schemas for per-command policies -------------------------

  @write_keys [
    ttl: [type: :non_neg_integer],
    timeout: [type: :non_neg_integer],
    generation: [type: :non_neg_integer],
    gen_policy: [type: {:in, [:none, :expect_gen_equal, :expect_gen_gt]}],
    exists: [
      type: {:in, [:create_only, :update_only, :replace_only, :create_or_replace]}
    ],
    send_key: [type: :boolean],
    durable_delete: [type: :boolean],
    pool_checkout_timeout: [type: :non_neg_integer],
    replica: [type: :non_neg_integer]
  ]

  @read_keys [
    timeout: [type: :non_neg_integer],
    # Specific bin names to fetch; omit to read all bins.
    bins: [type: {:list, {:or, [:string, :atom]}}],
    # When true, returns generation/expiration metadata without bin data.
    header_only: [type: :boolean],
    pool_checkout_timeout: [type: :non_neg_integer],
    replica: [type: :non_neg_integer]
  ]

  @delete_keys [
    timeout: [type: :non_neg_integer],
    durable_delete: [type: :boolean],
    pool_checkout_timeout: [type: :non_neg_integer],
    replica: [type: :non_neg_integer]
  ]

  @exists_keys [
    timeout: [type: :non_neg_integer],
    pool_checkout_timeout: [type: :non_neg_integer],
    replica: [type: :non_neg_integer]
  ]

  @touch_keys [
    ttl: [type: :non_neg_integer],
    timeout: [type: :non_neg_integer],
    pool_checkout_timeout: [type: :non_neg_integer],
    replica: [type: :non_neg_integer]
  ]

  # Per-command defaults that can be set at `Aerospike.start_link/1` time.
  @defaults_keys [
    write: [type: :keyword_list, keys: @write_keys],
    read: [type: :keyword_list, keys: @read_keys],
    delete: [type: :keyword_list, keys: @delete_keys],
    exists: [type: :keyword_list, keys: @exists_keys],
    touch: [type: :keyword_list, keys: @touch_keys]
  ]

  # Schema for the top-level `Aerospike.start_link/1` options.
  @start_schema NimbleOptions.new!(
                  name: [type: :atom, required: true],
                  hosts: [type: {:list, :string}, required: true],
                  pool_size: [type: :pos_integer, default: 10],
                  pool_checkout_timeout: [type: :non_neg_integer, default: 5_000],
                  connect_timeout: [type: :non_neg_integer, default: 5_000],
                  tend_interval: [type: :non_neg_integer, default: 1_000],
                  recv_timeout: [type: :non_neg_integer, default: 5_000],
                  auth_opts: [type: :keyword_list, default: []],
                  tls: [
                    type: :boolean,
                    default: false,
                    doc:
                      "When true, upgrades the TCP connection with TLS via `:ssl.connect/3`. Options are passed in `:tls_opts`."
                  ],
                  tls_opts: [
                    type: :keyword_list,
                    default: [],
                    doc:
                      "Keyword list passed to `:ssl.connect/3` after TCP connect (certificates, verify modes, etc.)."
                  ],
                  defaults: [
                    type: :keyword_list,
                    keys: @defaults_keys,
                    default: []
                  ]
                )

  @write_schema NimbleOptions.new!(@write_keys)
  @read_schema NimbleOptions.new!(@read_keys)
  @delete_schema NimbleOptions.new!(@delete_keys)
  @exists_schema NimbleOptions.new!(@exists_keys)
  @touch_schema NimbleOptions.new!(@touch_keys)

  @doc false
  def start_schema, do: @start_schema

  @doc false
  def validate_start(opts) when is_list(opts), do: NimbleOptions.validate(opts, @start_schema)

  @doc false
  def validate_start!(opts) when is_list(opts) do
    case validate_start(opts) do
      {:ok, validated} ->
        validated

      {:error, %NimbleOptions.ValidationError{} = e} ->
        raise ArgumentError, Exception.message(e)
    end
  end

  @doc false
  def validate_write(opts), do: NimbleOptions.validate(opts, @write_schema)

  @doc false
  def validate_read(opts), do: NimbleOptions.validate(opts, @read_schema)

  @doc false
  def validate_delete(opts), do: NimbleOptions.validate(opts, @delete_schema)

  @doc false
  def validate_exists(opts), do: NimbleOptions.validate(opts, @exists_schema)

  @doc false
  def validate_touch(opts), do: NimbleOptions.validate(opts, @touch_schema)

  @doc false
  def validation_error_message(%NimbleOptions.ValidationError{} = e), do: Exception.message(e)

  # -- Wire-level policy application -------------------------------------------
  # These functions set info2/info3 flag bits and header fields on an AsmMsg
  # based on validated user options.

  @doc false
  @spec apply_write_policy(AsmMsg.t(), keyword()) :: AsmMsg.t()
  def apply_write_policy(%AsmMsg{} = msg, opts) when is_list(opts) do
    msg
    |> put_ttl(Keyword.get(opts, :ttl))
    |> put_timeout(Keyword.get(opts, :timeout))
    |> apply_generation_flags(opts)
    |> apply_exists_flags(opts)
    |> maybe_durable_delete_write(Keyword.get(opts, :durable_delete))
  end

  defp put_ttl(msg, nil), do: msg
  defp put_ttl(msg, ttl), do: %{msg | expiration: ttl}

  defp put_timeout(msg, nil), do: msg
  defp put_timeout(msg, t), do: %{msg | timeout: t}

  # Sets generation check flags. If `:generation` is given without an explicit
  # `:gen_policy`, defaults to `:expect_gen_equal` (optimistic locking).
  defp apply_generation_flags(msg, opts) do
    gen = Keyword.get(opts, :generation)

    pol =
      case Keyword.get(opts, :gen_policy) do
        nil when is_integer(gen) -> :expect_gen_equal
        nil -> :none
        other -> other
      end

    case {pol, gen} do
      {:expect_gen_equal, g} when is_integer(g) ->
        %{msg | info2: msg.info2 ||| AsmMsg.info2_generation(), generation: g}

      {:expect_gen_gt, g} when is_integer(g) ->
        %{msg | info2: msg.info2 ||| AsmMsg.info2_generation_gt(), generation: g}

      _ ->
        msg
    end
  end

  # Maps the `:exists` option to the correct info2/info3 flag bit.
  # Note: `:create_only` uses info2 while the others use info3.
  defp apply_exists_flags(msg, opts) do
    case Keyword.get(opts, :exists) do
      :create_only ->
        %{msg | info2: msg.info2 ||| AsmMsg.info2_create_only()}

      :update_only ->
        %{msg | info3: msg.info3 ||| AsmMsg.info3_update_only()}

      :replace_only ->
        %{msg | info3: msg.info3 ||| AsmMsg.info3_replace_only()}

      :create_or_replace ->
        %{msg | info3: msg.info3 ||| AsmMsg.info3_create_or_replace()}

      _ ->
        msg
    end
  end

  # Durable delete tells the server to leave a tombstone for the record.
  defp maybe_durable_delete_write(msg, true) do
    %{msg | info2: msg.info2 ||| AsmMsg.info2_durable_delete()}
  end

  defp maybe_durable_delete_write(msg, _), do: msg

  @doc false
  @spec apply_read_policy(AsmMsg.t(), keyword()) :: AsmMsg.t()
  def apply_read_policy(%AsmMsg{} = msg, opts) when is_list(opts) do
    put_timeout(msg, Keyword.get(opts, :timeout))
  end

  @doc false
  @spec apply_delete_policy(AsmMsg.t(), keyword()) :: AsmMsg.t()
  def apply_delete_policy(%AsmMsg{} = msg, opts) when is_list(opts) do
    msg
    |> put_timeout(Keyword.get(opts, :timeout))
    |> maybe_durable_delete_write(Keyword.get(opts, :durable_delete))
  end

  @doc false
  @spec apply_touch_policy(AsmMsg.t(), keyword()) :: AsmMsg.t()
  def apply_touch_policy(%AsmMsg{} = msg, opts) when is_list(opts) do
    msg
    |> put_ttl(Keyword.get(opts, :ttl))
    |> put_timeout(Keyword.get(opts, :timeout))
  end

  # Appends a KEY field to the message so the server stores the original user key
  # alongside the digest. Without this, only the 20-byte digest is stored.
  @doc false
  @spec apply_send_key(AsmMsg.t(), Key.t(), keyword()) :: AsmMsg.t()
  def apply_send_key(%AsmMsg{} = msg, %Key{} = key, opts) when is_list(opts) do
    if Keyword.get(opts, :send_key) == true do
      case key_field(key) do
        nil -> msg
        field -> %{msg | fields: msg.fields ++ [field]}
      end
    else
      msg
    end
  end

  # Encodes the user key as a typed KEY field.
  # Type 1 = integer (8 bytes big-endian), type 3 = string.
  defp key_field(%Key{user_key: n}) when is_integer(n) do
    Field.key(1, <<n::64-signed-big>>)
  end

  defp key_field(%Key{user_key: s}) when is_binary(s) do
    Field.key(3, s)
  end

  defp key_field(%Key{user_key: nil}), do: nil

  # Builds the read AsmMsg, varying the wire shape based on options:
  # - `header_only: true` → READ + NOBINDATA flags (metadata only, no bin payload)
  # - `bins: [...]` → READ with per-bin READ operations (selective bin fetch)
  # - otherwise → full record read (all bins)
  @doc false
  @spec read_message_for_opts(Key.t(), keyword()) :: AsmMsg.t()
  def read_message_for_opts(%Key{} = key, opts) when is_list(opts) do
    ns = key.namespace
    set = key.set
    digest = key.digest
    bins = Keyword.get(opts, :bins)

    cond do
      Keyword.get(opts, :header_only) == true ->
        %AsmMsg{
          info1: AsmMsg.info1_read() ||| AsmMsg.info1_nobindata(),
          fields: [Field.namespace(ns), Field.set(set), Field.digest(digest)]
        }

      is_list(bins) and bins != [] ->
        ops =
          bins
          |> Enum.map(&bin_name_to_string/1)
          |> Enum.sort()
          |> Enum.map(&Operation.read/1)

        %AsmMsg{
          info1: AsmMsg.info1_read(),
          fields: [Field.namespace(ns), Field.set(set), Field.digest(digest)],
          operations: ops
        }

      true ->
        AsmMsg.read_command(ns, set, digest)
    end
  end

  defp bin_name_to_string(b) when is_binary(b), do: b
  defp bin_name_to_string(a) when is_atom(a), do: Atom.to_string(a)
end

defmodule Aerospike.Key do
  @moduledoc """
  Record key: namespace, set, optional user key, and server digest.

  The digest is **RIPEMD-160** over `set_name || particle_type_byte || encoded_user_key`.
  The namespace is **not** part of the digest (standard Aerospike digest rules).

  Supported user keys for `new/3`:

  * `integer()` — encoded as particle type `INTEGER` (1) and 8-byte big-endian signed int64
  * `String.t()` (binary) — encoded as particle type `STRING` (3) and UTF-8 bytes

  Binaries are treated as **string** keys, not blob keys.

  ## Examples

      iex> k = Aerospike.Key.new("test", "users", "user:42")
      iex> byte_size(k.digest)
      20
      iex> is_binary(k.namespace)
      true

  """

  import Bitwise

  @particle_integer 1
  @particle_string 3

  @partitions 4096

  # Signed 64-bit integer range bounds
  @min_int64 -9_223_372_036_854_775_808
  @max_int64 9_223_372_036_854_775_807

  @enforce_keys [:namespace, :digest]
  defstruct namespace: nil, set: "", user_key: nil, digest: nil

  @typedoc "Record key with namespace, set, optional user key, and digest."
  @type t :: %__MODULE__{
          namespace: String.t(),
          set: String.t(),
          user_key: String.t() | integer() | nil,
          digest: <<_::160>>
        }

  @typedoc "Tuple key form: `{namespace, set, user_key}`."
  @type key_tuple :: {String.t(), String.t(), String.t() | integer()}

  @typedoc """
  Accepted key input at public API boundaries.

  Use `%Aerospike.Key{}` directly, or pass `{namespace, set, user_key}` for
  convenience when you have user-key components.
  """
  @type key_input :: t() | key_tuple()

  @doc """
  Validates that `value` fits in Aerospike's signed 64-bit integer range.

  This helper is used by key and secondary-index filter builders before values
  are encoded for the server. It returns the integer unchanged or raises
  `ArgumentError` with `label` in the message.
  """
  @spec validate_int64!(integer(), String.t()) :: integer()
  def validate_int64!(value, _label)
      when is_integer(value) and value >= @min_int64 and value <= @max_int64 do
    value
  end

  def validate_int64!(value, label) when is_integer(value) do
    raise ArgumentError,
          "#{label} must fit in signed int64, got: #{value}"
  end

  def validate_int64!(value, label) do
    raise ArgumentError,
          "#{label} must be an integer that fits in signed int64, got: #{inspect(value)}"
  end

  @doc """
  Returns the partition id (0..4095) derived from the digest.

  Uses the first four bytes of the digest as a little-endian unsigned 32-bit
  integer, then masks to 12 bits (standard partition mapping).

  ## Examples

      iex> k = Aerospike.Key.new("namespace", "set", 0)
      iex> Aerospike.Key.partition_id(k)
      2451

  """
  @spec partition_id(t()) :: 0..4095
  def partition_id(%__MODULE__{digest: digest}) when byte_size(digest) == 20 do
    <<u::unsigned-little-32, _::binary>> = digest
    band(u, @partitions - 1)
  end

  @doc """
  Builds a key and computes the RIPEMD-160 digest.

  `namespace` must be a non-empty string. `set` may be an empty string.
  `user_key` must be an integer in the int64 range or a binary (string key).

  Raises `ArgumentError` if the arguments are invalid.

  ## Examples

      iex> k = Aerospike.Key.new("ns", "s", 1)
      iex> byte_size(k.digest)
      20

  """
  @spec new(String.t(), String.t(), String.t() | integer()) :: t()
  def new(namespace, set, user_key)
      when is_binary(namespace) and namespace != "" and is_binary(set) and is_integer(user_key) and
             user_key >= @min_int64 and user_key <= @max_int64 do
    digest = compute_digest_integer(set, user_key)

    %__MODULE__{
      namespace: namespace,
      set: set,
      user_key: user_key,
      digest: digest
    }
  end

  def new(namespace, set, user_key)
      when is_binary(namespace) and namespace != "" and is_binary(set) and is_binary(user_key) do
    digest = compute_digest_string(set, user_key)

    %__MODULE__{
      namespace: namespace,
      set: set,
      user_key: user_key,
      digest: digest
    }
  end

  def new(_namespace, _set, _user_key) do
    raise ArgumentError,
          "namespace must be a non-empty string, set must be a string, user_key must be string or int64 integer"
  end

  @doc """
  Builds a key from an existing 20-byte digest.

  The returned key keeps `user_key` unset because only the server digest is
  known at this boundary.

  ## Examples

      iex> digest = :crypto.hash(:ripemd160, "digest-only")
      iex> key = Aerospike.Key.from_digest("ns", "users", digest)
      iex> {key.user_key, key.digest == digest}
      {nil, true}

  """
  @spec from_digest(String.t(), String.t(), <<_::160>>) :: t()
  def from_digest(namespace, set, digest)
      when is_binary(namespace) and namespace != "" and is_binary(set) and is_binary(digest) and
             byte_size(digest) == 20 do
    %__MODULE__{
      namespace: namespace,
      set: set,
      user_key: nil,
      digest: digest
    }
  end

  def from_digest(_namespace, _set, _digest) do
    raise ArgumentError,
          "namespace must be a non-empty string, set must be a string, digest must be a 20-byte binary"
  end

  @doc """
  Coerces a public key input into `%Aerospike.Key{}`.

  Passes `%Aerospike.Key{}` through unchanged. For tuple keys, delegates to
  `new/3`, so tuple validation and int64 checks follow the same rules.

  Raises `ArgumentError` for non-key inputs.
  """
  @spec coerce!(key_input()) :: t()
  def coerce!(%__MODULE__{} = key), do: key

  def coerce!({namespace, set, user_key}), do: new(namespace, set, user_key)

  def coerce!(_key) do
    raise ArgumentError,
          "expected %Aerospike.Key{} or {namespace, set, user_key} tuple where user_key is a string or int64 integer"
  end

  defp compute_digest_integer(set, user_key) when is_integer(user_key) do
    data = <<set::binary, @particle_integer::8, user_key::64-signed-big>>
    :crypto.hash(:ripemd160, data)
  end

  defp compute_digest_string(set, user_key) when is_binary(user_key) do
    data = <<set::binary, @particle_string::8, user_key::binary>>
    :crypto.hash(:ripemd160, data)
  end
end

defmodule Aerospike.ExecuteTask do
  @moduledoc """
  Tracks background query execution progress.

  Phase 2 introduces server-side query execution entry points that mutate or
  apply UDFs across matching records and return immediately. `ExecuteTask`
  defines the public task shape for those APIs.

  Runtime status polling is added in a later implementation task.
  """

  alias Aerospike.Error

  @type kind :: :query_execute | :query_udf

  @enforce_keys [:conn, :namespace, :set, :task_id, :kind]
  defstruct [:conn, :namespace, :set, :task_id, :kind, node_name: nil]

  @type t :: %__MODULE__{
          conn: atom(),
          namespace: String.t(),
          set: String.t(),
          task_id: non_neg_integer(),
          kind: kind(),
          node_name: String.t() | nil
        }

  use Aerospike.AsyncTask

  @impl Aerospike.AsyncTask
  def status(%__MODULE__{}) do
    {:error,
     Error.from_result_code(:unsupported_feature,
       message: "query execution status is not implemented yet"
     )}
  end
end

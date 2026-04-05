defmodule CrudExample do
  @moduledoc """
  Basic CRUD examples for the Aerospike Elixir client.

  ## `example_1/0`

  Uses an ad-hoc connection started directly — no application supervisor.

  ## `example_2/0`

  Uses the supervised connection registered as `:aero` by the application.

  ## `example_3/0`

  Demonstrates write policies, TTL, exists checks, and optimistic concurrency.
  """

  @namespace "test"
  @set "people"

  defp create_contact do
    %{
      "firstname" => "Ada",
      "lastname" => "Lovelace",
      "email" => "ada@example.com",
      "language" => "Elixir",
      "score" => 42
    }
  end

  @doc """
  Ad-hoc connection — starts a client directly and runs CRUD operations.
  """
  def example_1 do
    IO.puts("=== Example 1: Ad-hoc connection ===\n")

    {:ok, _pid} =
      Aerospike.start_link(
        name: :example1,
        hosts: ["localhost:3000"]
      )

    # Cluster discovery (tend) runs asynchronously after start_link returns.
    # In a real application the supervisor starts the client early, so the
    # cluster is ready long before any request arrives. For this one-off
    # demo we wait briefly.
    Process.sleep(1_000)

    key = Aerospike.key(@namespace, @set, "contact:ada")

    # Create
    IO.puts("--- PUT ---")
    :ok = Aerospike.put!(:example1, key, create_contact())
    IO.puts("Wrote record for contact:ada\n")

    # Read
    IO.puts("--- GET ---")
    {:ok, record} = Aerospike.get(:example1, key)
    IO.puts("Read back: #{inspect(record.bins)}")
    IO.puts("Generation: #{record.generation}, TTL: #{record.ttl}\n")

    # Update (put with new/modified bins — merges into existing record)
    IO.puts("--- UPDATE (put with changed bins) ---")
    :ok = Aerospike.put!(:example1, key, %{"score" => 100, "title" => "Countess"})
    {:ok, updated} = Aerospike.get(:example1, key)
    IO.puts("Updated bins: #{inspect(updated.bins)}\n")

    # Exists
    IO.puts("--- EXISTS ---")
    {:ok, true} = Aerospike.exists(:example1, key)
    IO.puts("Record exists: true\n")

    # Delete
    IO.puts("--- DELETE ---")
    {:ok, true} = Aerospike.delete(:example1, key)
    IO.puts("Deleted: true")
    {:ok, false} = Aerospike.exists(:example1, key)
    IO.puts("Exists after delete: false\n")

    Aerospike.close(:example1)
    IO.puts("Connection closed.\n")
  end

  @doc """
  Supervised connection — uses the `:aero` connection started by `CrudExample.Application`.
  """
  def example_2 do
    IO.puts("=== Example 2: Supervised connection ===\n")

    key = Aerospike.key(@namespace, @set, "contact:grace")

    bins = %{
      "firstname" => "Grace",
      "lastname" => "Hopper",
      "email" => "grace@example.com",
      "language" => "COBOL",
      "score" => 99
    }

    # Create
    IO.puts("--- PUT ---")
    :ok = Aerospike.put!(:aero, key, bins)
    IO.puts("Wrote record for contact:grace\n")

    # Read all bins
    IO.puts("--- GET (all bins) ---")
    {:ok, record} = Aerospike.get(:aero, key)
    IO.puts("All bins: #{inspect(record.bins)}\n")

    # Read specific bins only
    IO.puts("--- GET (selected bins) ---")
    {:ok, partial} = Aerospike.get(:aero, key, bins: ["firstname", "score"])
    IO.puts("Selected bins: #{inspect(partial.bins)}\n")

    # Read header only (generation + TTL, no bin data)
    IO.puts("--- GET (header only) ---")
    {:ok, header} = Aerospike.get(:aero, key, header_only: true)

    IO.puts(
      "Header only — generation: #{header.generation}, TTL: #{header.ttl}, bins: #{inspect(header.bins)}\n"
    )

    # Update
    IO.puts("--- UPDATE ---")
    :ok = Aerospike.put!(:aero, key, %{"score" => 150})
    {:ok, updated} = Aerospike.get(:aero, key, bins: ["firstname", "score"])
    IO.puts("After update: #{inspect(updated.bins)}\n")

    # Delete
    IO.puts("--- DELETE ---")
    true = Aerospike.delete!(:aero, key)
    IO.puts("Deleted: true\n")
  end

  @doc """
  Demonstrates write policies, TTL, touch, and optimistic concurrency (CAS).
  """
  def example_3 do
    IO.puts("=== Example 3: Policies, TTL & CAS ===\n")

    key = Aerospike.key(@namespace, @set, "contact:alan")

    bins = %{
      "firstname" => "Alan",
      "lastname" => "Turing",
      "score" => 50
    }

    # Write with a TTL of 300 seconds (5 minutes)
    IO.puts("--- PUT with TTL ---")
    :ok = Aerospike.put!(:aero, key, bins, ttl: 300)
    {:ok, record} = Aerospike.get(:aero, key)
    IO.puts("Written with TTL. Server-reported TTL: #{record.ttl}s\n")

    # Touch — refresh the TTL without changing data
    IO.puts("--- TOUCH (refresh TTL) ---")
    :ok = Aerospike.touch!(:aero, key, ttl: 600)
    {:ok, touched} = Aerospike.get(:aero, key, header_only: true)
    IO.puts("After touch — TTL: #{touched.ttl}s, generation: #{touched.generation}\n")

    # Create-only — fails if the record already exists
    IO.puts("--- CREATE ONLY (expect failure) ---")

    case Aerospike.put(:aero, key, bins, exists: :create_only) do
      :ok ->
        IO.puts("Unexpected success\n")

      {:error, %Aerospike.Error{code: code}} ->
        IO.puts("Expected error: #{code} (record already exists)\n")
    end

    # Optimistic concurrency (compare-and-swap)
    IO.puts("--- CAS (optimistic concurrency) ---")
    {:ok, current} = Aerospike.get(:aero, key)
    gen = current.generation

    case Aerospike.put(:aero, key, %{"score" => 75},
           generation: gen,
           gen_policy: :expect_gen_equal
         ) do
      :ok ->
        IO.puts("CAS write succeeded at generation #{gen}\n")

      {:error, %Aerospike.Error{code: :generation_error}} ->
        IO.puts("CAS conflict — someone else modified the record\n")
    end

    # Integer keys
    IO.puts("--- INTEGER KEY ---")
    int_key = Aerospike.key(@namespace, @set, 42)
    :ok = Aerospike.put!(:aero, int_key, %{"meaning" => "life"})
    {:ok, int_record} = Aerospike.get(:aero, int_key)
    IO.puts("Integer key record: #{inspect(int_record.bins)}\n")

    # Clean up
    Aerospike.delete!(:aero, key)
    Aerospike.delete!(:aero, int_key)
    IO.puts("Cleaned up.\n")
  end
end

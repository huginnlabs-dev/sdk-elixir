defmodule Dataflow.EctoTest do
  use ExUnit.Case, async: false

  defmodule FakeRepo do
    def __adapter__, do: Ecto.Adapters.Postgres
  end

  # Stands in for Dataflow.Pipeline under its registered name and forwards
  # every enqueued (JSON) event to the test process.
  defmodule EventRecorder do
    @moduledoc false
    use GenServer

    def start_link(parent) do
      GenServer.start_link(__MODULE__, parent, name: Dataflow.Pipeline)
    end

    def init(parent), do: {:ok, %{parent: parent}}

    def handle_cast({:enqueue, json}, state) do
      send(state.parent, {:dataflow_event, json})
      {:noreply, state}
    end

    def handle_info(_msg, state), do: {:noreply, state}
  end

  setup do
    Application.put_env(:dataflow, :settings, %{
      endpoint: "http://127.0.0.1:1",
      api_key: "test-key",
      service_name: "ecto-test",
      sample_ratio: 1.0,
      buffer_size: 10_000,
      disabled: false
    })

    on_exit(fn -> Application.delete_env(:dataflow, :settings) end)
    :ok
  end

  describe "attach_ecto/detach_ecto" do
    test "attaches on the standard Ecto telemetry names and detaches cleanly" do
      assert Dataflow.attach_ecto([:ecto_test]) == :ok
      assert Dataflow.attach_ecto([:ecto_test]) == {:error, :already_exists}
      assert Dataflow.detach_ecto([:ecto_test]) == :ok
      assert Dataflow.detach_ecto([:ecto_test]) == {:error, :not_found}
    end
  end

  describe "query events" do
    test "become DB_QUERY spans with verb+table name, system callee and clipped statement" do
      with_recorder(fn ->
        assert Dataflow.attach_ecto([:ecto_query]) == :ok

        :telemetry.execute([:ecto_query, :repo, :query], %{total_time: to_native(15)}, %{
          repo: FakeRepo,
          query: "SELECT *\n  FROM orders\n WHERE id = $1",
          params: [42]
        })

        assert_receive {:dataflow_event, json}, 1_000
        event = JSON.decode!(json)

        assert event["type"] == "DB_QUERY"
        assert event["name"] == "SELECT orders"
        assert event["callee_package"] == "postgres"
        assert event["status_code"] == 200
        assert event["duration_ms"] == 15
        assert event["metadata"]["db.system"] == "postgres"
        assert event["metadata"]["db.statement"] == "SELECT * FROM orders WHERE id = $1"
        # Parameter values never travel.
        refute Map.has_key?(event["metadata"], "params")
        refute event["metadata"]["db.statement"] =~ "42"

        assert Dataflow.detach_ecto([:ecto_query]) == :ok
      end)
    end

    test "failed queries record status 500 and the error message" do
      with_recorder(fn ->
        assert Dataflow.attach_ecto([:ecto_error]) == :ok

        :telemetry.execute([:ecto_error, :repo, :query], %{total_time: to_native(3)}, %{
          repo: FakeRepo,
          query: "INSERT INTO users (name) VALUES ($1)",
          error: %RuntimeError{message: "connection lost"}
        })

        assert_receive {:dataflow_event, json}, 1_000
        event = JSON.decode!(json)

        assert event["type"] == "DB_QUERY"
        assert event["name"] == "INSERT users"
        assert event["status_code"] == 500
        assert event["error_message"] =~ "connection lost"

        assert Dataflow.detach_ecto([:ecto_error]) == :ok
      end)
    end

    test "join the current trace when one is active" do
      with_recorder(fn ->
        assert Dataflow.attach_ecto([:ecto_join]) == :ok

        Dataflow.trace("outer", fn span ->
          :telemetry.execute([:ecto_join, :repo, :query], %{total_time: to_native(2)}, %{
            repo: FakeRepo,
            query: "DELETE FROM sessions WHERE id = $1"
          })

          assert_receive {:dataflow_event, json}, 1_000
          event = JSON.decode!(json)

          assert event["trace_id"] == span.trace_id
          assert event["parent_span_id"] == span.span_id
        end)

        assert Dataflow.detach_ecto([:ecto_join]) == :ok
      end)
    end

    test "open their own trace when no span is active" do
      with_recorder(fn ->
        assert Dataflow.attach_ecto([:ecto_orphan]) == :ok

        :telemetry.execute([:ecto_orphan, :repo, :query], %{total_time: to_native(1)}, %{
          repo: FakeRepo,
          query: "UPDATE users SET last_seen = now()"
        })

        assert_receive {:dataflow_event, json}, 1_000
        event = JSON.decode!(json)

        assert event["name"] == "UPDATE users"
        assert event["parent_span_id"] == ""
        assert is_binary(event["trace_id"]) and event["trace_id"] != ""

        assert Dataflow.detach_ecto([:ecto_orphan]) == :ok
      end)
    end

    test "handler survives missing metadata" do
      with_recorder(fn ->
        assert Dataflow.attach_ecto([:ecto_garbage]) == :ok

        :telemetry.execute([:ecto_garbage, :repo, :query], %{}, %{})

        assert_receive {:dataflow_event, json}, 1_000
        event = JSON.decode!(json)

        assert event["name"] == "QUERY"
        assert event["callee_package"] == "ecto"
        assert event["status_code"] == 200
        refute Map.has_key?(event["metadata"], "db.statement")

        assert Dataflow.detach_ecto([:ecto_garbage]) == :ok
      end)
    end

    test "emit nothing when tracing is disabled" do
      Application.put_env(:dataflow, :settings, %{
        endpoint: "http://127.0.0.1:1",
        api_key: "test-key",
        service_name: "ecto-test",
        sample_ratio: 1.0,
        buffer_size: 10_000,
        disabled: true
      })

      with_recorder(fn ->
        assert Dataflow.attach_ecto([:ecto_off]) == :ok

        :telemetry.execute([:ecto_off, :repo, :query], %{total_time: to_native(1)}, %{
          repo: FakeRepo,
          query: "SELECT 1"
        })

        refute_receive {:dataflow_event, _json}, 200

        assert Dataflow.detach_ecto([:ecto_off]) == :ok
      end)
    end
  end

  # Swaps the real Pipeline for a recorder so the emitted JSON events are
  # observable without a server. The SDK tree is stopped whole — child-pid
  # termination is unreliable across OTP releases — and restarted after.
  defp with_recorder(fun) do
    case Process.whereis(Dataflow.Supervisor) do
      nil -> :ok
      sup -> Supervisor.stop(sup)
    end

    {:ok, recorder} = EventRecorder.start_link(self())

    try do
      fun.()
    after
      if Process.alive?(recorder), do: GenServer.stop(recorder)
      Dataflow.start(:normal, [])
      Dataflow.clear_context()
    end
  end

  defp to_native(ms), do: System.convert_time_unit(ms, :millisecond, :native)
end

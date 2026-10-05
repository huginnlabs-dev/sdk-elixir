defmodule Dataflow.ObanTest do
  use ExUnit.Case, async: false

  # Plain map standing in for an Oban.Job: the handler reads only worker,
  # queue and attempt — the identifiable args must never travel.
  defp job do
    %{
      worker: MyApp.EmailWorker,
      queue: :mailers,
      attempt: 2,
      args: %{"email" => "user@corp.test", "token" => "s3cr3t-token"}
    }
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
      service_name: "oban-test",
      sample_ratio: 1.0,
      buffer_size: 10_000,
      disabled: false
    })

    on_exit(fn -> Application.delete_env(:dataflow, :settings) end)
    :ok
  end

  describe "attach_oban/detach_oban" do
    test "attaches on the standard Oban lifecycle events and detaches cleanly" do
      assert Dataflow.attach_oban([:oban_test]) == :ok
      assert Dataflow.attach_oban([:oban_test]) == {:error, :already_exists}
      assert Dataflow.detach_oban([:oban_test]) == :ok
      assert Dataflow.detach_oban([:oban_test]) == {:error, :not_found}
    end
  end

  describe "job lifecycle" do
    test "start/stop become one FUNCTION_CALL span named after the worker, with queue and attempt fields" do
      with_recorder(fn ->
        assert Dataflow.attach_oban([:oban_span]) == :ok

        :telemetry.execute([:oban_span, :job, :start], %{system_time: System.system_time(:millisecond)}, %{job: job()})

        # The start event only opens the span — nothing is emitted yet.
        refute_receive {:dataflow_event, _json}, 200

        :telemetry.execute([:oban_span, :job, :stop], %{duration: to_native(25)}, %{job: job(), state: :success})

        assert_receive {:dataflow_event, json}, 1_000
        event = JSON.decode!(json)

        assert event["type"] == "FUNCTION_CALL"
        assert event["name"] == "oban.MyApp.EmailWorker"
        assert event["callee_package"] == "oban"
        assert event["status_code"] == 200
        assert event["duration_ms"] == 25
        assert event["metadata"] == %{"oban.queue" => "mailers", "oban.attempt" => "2"}
        # The job's own process carries no parent span: its own trace.
        assert event["parent_span_id"] == ""
        assert is_binary(event["trace_id"]) and event["trace_id"] != ""

        assert Dataflow.detach_oban([:oban_span]) == :ok
      end)
    end

    test "exception events record status 500 with clipped message and stack" do
      with_recorder(fn ->
        assert Dataflow.attach_oban([:oban_fail]) == :ok

        :telemetry.execute([:oban_fail, :job, :start], %{system_time: System.system_time(:millisecond)}, %{job: job()})

        :telemetry.execute([:oban_fail, :job, :exception], %{duration: to_native(8)}, %{
          job: job(),
          state: :discard,
          kind: :error,
          error: %RuntimeError{message: "SMTP relay refused"},
          stacktrace: [{MyApp.EmailWorker, :deliver, 2, [file: ~c"lib/my_app/email_worker.ex", line: 42]}]
        })

        assert_receive {:dataflow_event, json}, 1_000
        event = JSON.decode!(json)

        assert event["type"] == "FUNCTION_CALL"
        assert event["status_code"] == 500
        assert event["error_message"] =~ "SMTP relay refused"
        assert event["metadata"]["error.stack"] =~ "deliver"
        assert event["metadata"]["oban.queue"] == "mailers"
        assert event["metadata"]["oban.attempt"] == "2"

        assert Dataflow.detach_oban([:oban_fail]) == :ok
      end)
    end

    test "never captures job args" do
      with_recorder(fn ->
        assert Dataflow.attach_oban([:oban_args]) == :ok

        :telemetry.execute([:oban_args, :job, :start], %{system_time: System.system_time(:millisecond)}, %{job: job()})
        :telemetry.execute([:oban_args, :job, :stop], %{duration: to_native(5)}, %{job: job(), state: :success})

        assert_receive {:dataflow_event, json}, 1_000
        event = JSON.decode!(json)

        refute json =~ "user@corp.test"
        refute json =~ "s3cr3t-token"
        refute Map.has_key?(event["metadata"], "args")
        assert event["metadata"] == %{"oban.queue" => "mailers", "oban.attempt" => "2"}

        assert Dataflow.detach_oban([:oban_args]) == :ok
      end)
    end

    test "emit nothing when tracing is disabled" do
      Application.put_env(:dataflow, :settings, %{
        endpoint: "http://127.0.0.1:1",
        api_key: "test-key",
        service_name: "oban-test",
        sample_ratio: 1.0,
        buffer_size: 10_000,
        disabled: true
      })

      with_recorder(fn ->
        assert Dataflow.attach_oban([:oban_off]) == :ok

        :telemetry.execute([:oban_off, :job, :start], %{system_time: System.system_time(:millisecond)}, %{job: job()})
        :telemetry.execute([:oban_off, :job, :stop], %{duration: to_native(1)}, %{job: job(), state: :success})

        refute_receive {:dataflow_event, _json}, 200

        assert Dataflow.detach_oban([:oban_off]) == :ok
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

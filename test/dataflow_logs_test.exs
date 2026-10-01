defmodule Dataflow.LogsTest do
  use ExUnit.Case, async: false

  require Logger

  # Stands in for Dataflow.Logs under its registered name and forwards
  # every enqueued (wire) entry to the test process.
  defmodule LogRecorder do
    @moduledoc false
    use GenServer

    def start_link(parent) do
      GenServer.start_link(__MODULE__, parent, name: Dataflow.Logs)
    end

    def init(parent), do: {:ok, %{parent: parent}}

    def handle_cast({:enqueue, entry}, state) do
      send(state.parent, {:dataflow_log, entry})
      {:noreply, state}
    end

    def handle_call(:flush, _from, state), do: {:reply, :ok, state}

    def handle_info(_msg, state), do: {:noreply, state}
  end

  setup do
    Application.put_env(:dataflow, :settings, %{
      endpoint: "http://127.0.0.1:1",
      api_key: "test-key",
      service_name: "logs-test",
      sample_ratio: 1.0,
      buffer_size: 10_000,
      disabled: false
    })

    Dataflow.clear_context()

    on_exit(fn ->
      Dataflow.detach_logger()
      Logger.reset_metadata()
      Application.delete_env(:dataflow, :settings)
      Dataflow.clear_context()
    end)

    :ok
  end

  describe "level normalization" do
    test "maps logger levels onto the wire vocabulary" do
      assert Dataflow.Logs.normalize_level(:debug) == "debug"
      assert Dataflow.Logs.normalize_level("debug") == "debug"
      assert Dataflow.Logs.normalize_level(:info) == "info"
      assert Dataflow.Logs.normalize_level(:warn) == "warn"
      assert Dataflow.Logs.normalize_level(:warning) == "warn"
      assert Dataflow.Logs.normalize_level("warning") == "warn"
      assert Dataflow.Logs.normalize_level(:error) == "error"
      assert Dataflow.Logs.normalize_level(:critical) == "error"
      assert Dataflow.Logs.normalize_level(:alert) == "error"
      assert Dataflow.Logs.normalize_level(:emergency) == "error"
      # Idempotent: the handler feeds already-normalized levels back in.
      assert Dataflow.Logs.normalize_level("warn") == "warn"
      # Anything unknown reports as info.
      assert Dataflow.Logs.normalize_level(:whatever) == "info"
      assert Dataflow.Logs.normalize_level(42) == "info"
    end

    test "convenience helpers pin their level" do
      with_recorder(fn ->
        assert Dataflow.debug("d") == :ok
        assert Dataflow.info("i") == :ok
        assert Dataflow.warn("w") == :ok
        assert Dataflow.error("e") == :ok

        assert_receive {:dataflow_log, first}, 1_000
        assert first["level"] == "debug" and first["message"] == "d"

        assert_receive {:dataflow_log, second}, 1_000
        assert second["level"] == "info" and second["message"] == "i"

        assert_receive {:dataflow_log, third}, 1_000
        assert third["level"] == "warn" and third["message"] == "w"

        assert_receive {:dataflow_log, fourth}, 1_000
        assert fourth["level"] == "error" and fourth["message"] == "e"
      end)
    end
  end

  describe "log body map shape" do
    test "build_log/6 produces the wire entry" do
      entry =
        Dataflow.Logs.build_log(:warning, "disk 90%", "trace-1", "span-7", "payments", %{
          "pct" => 90,
          "ok" => true,
          "bad" => {:t, 1}
        })

      assert entry["level"] == "warn"
      assert entry["message"] == "disk 90%"
      assert entry["trace_id"] == "trace-1"
      assert entry["span_id"] == "span-7"
      assert entry["service_name"] == "payments"
      assert entry["fields"] == %{"pct" => "90", "ok" => "true", "bad" => "{:t, 1}"}
      assert is_integer(entry["timestamp"])

      assert Dataflow.Logs.build_batch([entry]) == %{"logs" => [entry]}
    end

    test "nil trace/span ids degrade to empty strings" do
      entry = Dataflow.Logs.build_log(:info, "m", nil, nil, nil, %{})

      assert entry["trace_id"] == ""
      assert entry["span_id"] == ""
      assert entry["service_name"] == ""
      assert entry["fields"] == %{}
    end
  end

  describe "field stringification" do
    test "values stringify (to_string with inspect fallback) and the map caps at 50 entries" do
      fields = Map.new(Enum.map(1..100, fn i -> {:"k#{i}", i} end))
      entry = Dataflow.Logs.build_log(:info, "m", "t", "s", "svc", fields)

      assert map_size(entry["fields"]) == 50
      assert Enum.all?(Map.values(entry["fields"]), &is_binary/1)

      weird = Dataflow.Logs.build_log(:info, "m", "t", "s", "svc", %{a: 1, f: 2.5, t: {:tuple, 1}})
      assert weird["fields"] == %{"a" => "1", "f" => "2.5", "t" => "{:tuple, 1}"}
    end

    test "metadata keeps only cleanly stringifiable values, internal keys dropped" do
      meta = %{
        user_id: "u-9",
        depth: 3,
        tuple: {:nope, 1},
        time: 1_700_000_000_000,
        gl: self(),
        crash_reason: {%RuntimeError{}, []},
        domain: [:elixir]
      }

      assert Dataflow.Logs.stringify_metadata(meta) == %{"user_id" => "u-9", "depth" => "3"}
    end
  end

  describe "buffer" do
    test "add_capped/3 drops the oldest entry past the cap" do
      queue = Enum.reduce(1..1_024, :queue.new(), fn i, q -> Dataflow.Logs.add_capped(q, i) end)
      assert :queue.len(queue) == 1_024

      queue = Dataflow.Logs.add_capped(queue, :newest)
      assert :queue.len(queue) == 1_024

      {{:value, front}, _} = :queue.out(queue)
      assert front == 2

      {_, tail} = :queue.split(1_023, queue)
      {{:value, back}, _} = :queue.out(tail)
      assert back == :newest
    end
  end

  describe "trace correlation" do
    test "entries carry the current span's trace and span ids" do
      with_recorder(fn ->
        Dataflow.trace("job.Run", fn span ->
          assert Dataflow.info("working", %{"step" => 1}) == :ok

          assert_receive {:dataflow_log, entry}, 1_000
          assert entry["trace_id"] == span.trace_id
          assert entry["span_id"] == span.span_id
          assert entry["level"] == "info"
          assert entry["message"] == "working"
          assert entry["service_name"] == "logs-test"
          assert entry["fields"] == %{"step" => "1"}
          assert is_integer(entry["timestamp"])
        end)
      end)
    end

    test "without a current span the ids are empty" do
      with_recorder(fn ->
        assert Dataflow.current_span() == nil
        assert Dataflow.warn("orphan") == :ok

        assert_receive {:dataflow_log, entry}, 1_000
        assert entry["trace_id"] == ""
        assert entry["span_id"] == ""
        assert entry["level"] == "warn"
      end)
    end
  end

  describe "Logger handler" do
    test "attach/detach follow the telemetry-style returns" do
      assert Dataflow.attach_logger() == :ok
      assert Dataflow.attach_logger() == {:error, :already_exists}
      assert Dataflow.detach_logger() == :ok
      assert Dataflow.detach_logger() == {:error, :not_found}
    end

    test "forwards Logger messages with mapped level, text and metadata fields" do
      with_recorder(fn ->
        assert Dataflow.attach_logger() == :ok

        Logger.metadata(user_id: "u-9", detail: {:not, "stringifiable"})
        Logger.warning("cache full")

        assert_receive {:dataflow_log, entry}, 1_000
        assert entry["level"] == "warn"
        assert entry["message"] == "cache full"
        assert entry["fields"]["user_id"] == "u-9"
        refute Map.has_key?(entry["fields"], "detail")
        refute Map.has_key?(entry["fields"], "time")
        refute Map.has_key?(entry["fields"], "pid")
        assert entry["trace_id"] == ""

        # Joins the caller's trace when one is open.
        Dataflow.trace("log.Span", fn span ->
          Logger.info("inside span")

          assert_receive {:dataflow_log, joined}, 1_000
          assert joined["trace_id"] == span.trace_id
          assert joined["span_id"] == span.span_id
        end)

        assert Dataflow.detach_logger() == :ok
      end)
    end

    test "handler callback never raises on garbage events" do
      assert Dataflow.Logs.log(%{}, %{}) == :ok
      assert Dataflow.Logs.log(%{level: :warning, msg: :garbage, meta: :garbage}, %{}) == :ok
    end
  end

  describe "flush and disabled" do
    test "flush_logs/0 returns :ok" do
      with_recorder(fn ->
        assert Dataflow.flush_logs() == :ok
      end)
    end

    test "disabled config turns every entry point into a no-op" do
      Application.put_env(:dataflow, :settings, %{
        endpoint: "",
        api_key: "",
        disabled: true
      })

      with_recorder(fn ->
        assert Dataflow.info("nope") == :ok
        assert Dataflow.log("error", "nope", %{"a" => 1}) == :ok
        assert Dataflow.Logs.log(%{level: :error, msg: {:text, "nope"}, meta: %{}}, %{}) == :ok
      end)

      refute_receive {:dataflow_log, _entry}, 200
    end
  end

  # Swaps the real Logs GenServer for a recorder so the wire entries are
  # observable without a server; the supervised child is restored after.
  defp with_recorder(fun) do
    pid = Process.whereis(Dataflow.Logs)
    :ok = Supervisor.terminate_child(Dataflow.Supervisor, pid)
    {:ok, recorder} = LogRecorder.start_link(self())

    try do
      fun.()
    after
      GenServer.stop(recorder)
      {:ok, _pid} = Supervisor.restart_child(Dataflow.Supervisor, Dataflow.Logs)
    end
  end
end

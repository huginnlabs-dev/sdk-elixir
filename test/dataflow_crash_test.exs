defmodule Dataflow.CrashTest do
  use ExUnit.Case, async: false

  # Plug-free router harness: defines plug_builder_call/2 the way
  # Plug.Router does, then applies the `use Dataflow.PlugCrash` wrapper.
  defmodule PlugRouterHarness do
    def plug_builder_call(conn, _opts) do
      if conn[:explode], do: raise("handler boom")
      {:handled, conn}
    end

    use Dataflow.PlugCrash
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
      service_name: "crash-test",
      sample_ratio: 1.0,
      buffer_size: 10_000,
      disabled: false
    })

    Dataflow.clear_context()

    on_exit(fn ->
      Application.delete_env(:dataflow, :settings)
      Dataflow.clear_context()
    end)

    :ok
  end

  describe "format helpers" do
    test "format_error/3 renders the BEAM-style message and truncates to 500 chars" do
      assert Dataflow.Crash.format_error(:error, %RuntimeError{message: "boom"}, []) ==
               "** (RuntimeError) boom"

      long = String.duplicate("x", 2_000)
      truncated = Dataflow.Crash.format_error(:error, %RuntimeError{message: long}, [])

      assert String.length(truncated) == 500
      assert truncated =~ "** (RuntimeError) "
    end

    test "format_error/3 renders throws and exits" do
      assert Dataflow.Crash.format_error(:throw, {:custom, :throw}, []) =~ "** (throw)"
      assert Dataflow.Crash.format_error(:exit, :shutdown, []) =~ "** (exit) shutdown"
    end

    test "clip_stack/1 caps the formatted trace at 8192 bytes, keeping the top" do
      frame = {Foo.Bar, :baz, 1, [file: ~c"lib/foo/bar.ex", line: 7]}

      clipped = Dataflow.Crash.clip_stack([frame])
      assert is_binary(clipped)
      assert clipped =~ "lib/foo/bar.ex"
      assert byte_size(clipped) <= 8_192

      assert byte_size(Dataflow.Crash.clip_stack(List.duplicate(frame, 2_000))) == 8_192
      assert Dataflow.Crash.clip_stack(nil) == ""
    end
  end

  describe "capture/1" do
    test "returns the function's value and records nothing when it succeeds" do
      with_recorder(fn ->
        assert Dataflow.Crash.capture(fn -> {:ok, 42} end) == {:ok, 42}
      end)

      refute_receive {:dataflow_event, _json}, 200
    end

    test "records on the current span — status 500, error message, error.stack — then re-raises" do
      with_recorder(fn ->
        Dataflow.start_span("manual.Op")

        try do
          Dataflow.Crash.capture(fn -> raise RuntimeError, message: "boom" end)
          flunk("capture must re-raise")
        rescue
          e in RuntimeError -> assert e.message == "boom"
        end

        Dataflow.end_current_span()
      end)

      assert_receive {:dataflow_event, json}, 1_000
      event = JSON.decode!(json)

      assert event["name"] == "manual.Op"
      assert event["status_code"] == 500
      assert event["error_message"] =~ "** (RuntimeError) boom"

      stack = event["metadata"]["error.stack"]
      assert is_binary(stack) and stack != ""
      assert byte_size(stack) <= 8_192
    end

    test "records throws and exits, re-raising with the original value" do
      with_recorder(fn ->
        Dataflow.start_span("thrown.Op")

        try do
          Dataflow.Crash.capture(fn -> throw({:custom, :throw}) end)
          flunk("capture must re-throw")
        catch
          :throw, {:custom, :throw} -> :ok
        end

        try do
          Dataflow.Crash.capture(fn -> exit(:shutdown) end)
          flunk("capture must re-exit")
        catch
          :exit, :shutdown -> :ok
        end

        Dataflow.end_current_span()
      end)

      # Both crashes ride the one span, joined into its single event.
      assert_receive {:dataflow_event, json}, 1_000
      event = JSON.decode!(json)

      assert event["name"] == "thrown.Op"
      assert event["status_code"] == 500
      assert event["error_message"] =~ "** (throw)"
      assert event["error_message"] =~ "** (exit)"
    end

    test "records a synthetic exception span when no span is current" do
      with_recorder(fn ->
        assert Dataflow.current_span() == nil

        assert_raise RuntimeError, "boom", fn ->
          Dataflow.Crash.capture(fn -> raise "boom" end)
        end
      end)

      assert_receive {:dataflow_event, json}, 1_000
      event = JSON.decode!(json)

      assert event["type"] == "EXCEPTION"
      assert event["name"] == "exception"
      assert event["parent_span_id"] == ""
      assert is_binary(event["trace_id"]) and event["trace_id"] != ""
      assert event["status_code"] == 500
      assert event["error_message"] =~ "** (RuntimeError) boom"
      assert event["metadata"]["error.stack"] != ""
    end
  end

  describe "record_exception/4" do
    test "attaches error.request metadata from a Plug conn" do
      with_recorder(fn ->
        Dataflow.start_span("request.Span")

        stack = [{MyApp.Handler, :show, 2, [file: ~c"lib/my_app/handler.ex", line: 12]}]
        conn = %{request_method: "get", request_path: "/orders/42"}

        assert Dataflow.Crash.record_exception(:error, %RuntimeError{message: "handler boom"}, stack, conn) == :ok

        Dataflow.end_current_span()
      end)

      assert_receive {:dataflow_event, json}, 1_000
      event = JSON.decode!(json)

      assert event["status_code"] == 500
      assert event["error_message"] =~ "** (RuntimeError) handler boom"
      assert event["metadata"]["error.request"] == "GET /orders/42"
      assert event["metadata"]["error.stack"] =~ "lib/my_app/handler.ex"
    end

    test "never raises into the caller, even for garbage input" do
      with_recorder(fn ->
        assert Dataflow.Crash.record_exception(:error, %RuntimeError{message: "x"}, :garbage, %{weird: :conn}) ==
                 :ok
      end)
    end
  end

  describe "PlugCrash wrapper" do
    test "wraps dispatch: records on the request's span and re-raises" do
      with_recorder(fn ->
        Dataflow.start_span("http.Request")

        assert {:handled, %{}} = PlugRouterHarness.plug_builder_call(%{}, [])

        try do
          PlugRouterHarness.plug_builder_call(%{explode: true}, [])
          flunk("the wrapper must re-raise")
        rescue
          e in RuntimeError -> assert e.message == "handler boom"
        end

        Dataflow.end_current_span()
      end)

      assert_receive {:dataflow_event, json}, 1_000
      event = JSON.decode!(json)

      assert event["name"] == "http.Request"
      assert event["status_code"] == 500
      assert event["error_message"] =~ "handler boom"
      assert event["metadata"]["error.stack"] != ""
    end
  end

  describe "disabled" do
    test "capture is a pure pass-through: fun runs bare, nothing is recorded" do
      Application.put_env(:dataflow, :settings, %{endpoint: "", api_key: "", disabled: true})

      assert Dataflow.Crash.capture(fn -> :passthrough end) == :passthrough
      assert Dataflow.Crash.capture(fn -> send(self(), :ran) end) == :ran
      assert_received :ran

      assert_raise RuntimeError, "bare raise", fn ->
        Dataflow.Crash.capture(fn -> raise "bare raise" end)
      end

      refute_receive {:dataflow_event, _json}, 200
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
end

defmodule Dataflow.PipelineTest do
  use ExUnit.Case, async: false

  # Regression: overflowing the replay buffer used to bind the popped
  # EVENT (a JSON binary) into the buffer slot, so the next :queue.in
  # raised and the Pipeline entered a crash-restart loop, dropping every
  # buffered span. The buffer must survive overflow as a queue.
  test "buffer overflow drops the oldest event and keeps the queue intact" do
    Application.put_env(:dataflow, :settings, %{
      endpoint: "http://127.0.0.1:1",
      api_key: "test-key",
      service_name: "pipeline-test",
      buffer_size: 3
    })

    on_exit(fn -> Application.delete_env(:dataflow, :settings) end)

    pid = self() |> pipeline_pid()
    for i <- 1..10, do: GenServer.cast(Dataflow.Pipeline, {:enqueue, ~s({"seq":#{i}})})
    # casts are async; give the server a moment to chew through them
    wait_until(fn ->
      state = :sys.get_state(Dataflow.Pipeline)
      :queue.len(state.buffer) == 3
    end)

    assert Process.alive?(pid)
    state = :sys.get_state(Dataflow.Pipeline)
    assert :queue.is_queue(state.buffer)
    assert :queue.len(state.buffer) == 3
  end

  defp pipeline_pid(_parent) do
    case GenServer.whereis(Dataflow.Pipeline) do
      nil ->
        {:ok, pid} = Dataflow.Pipeline.start_link([])
        pid

      pid ->
        pid
    end
  end

  defp wait_until(fun, tries \\ 50)

  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, tries) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, tries - 1)
    end
  end
end

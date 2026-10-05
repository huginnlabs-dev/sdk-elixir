defmodule Dataflow.Oban do
  @moduledoc """
  Oban job tracing over :telemetry. `Dataflow.attach_oban/1` subscribes a
  handler to the standard Oban job lifecycle events — `[:<prefix>, :job,
  :start]`, `[:<prefix>, :job, :stop]` and `[:<prefix>, :job, :exception]`;
  every job becomes a FUNCTION_CALL span:

    * name — "oban.<worker>", the worker module from the job map
    * metadata — "oban.queue" and "oban.attempt" only; job args may carry
      end-user data and are never captured
    * duration — from the stop/exception measurements
    * failures — status 500 with the `Exception.format/3` rendering
      truncated to 500 characters as the error message, and the formatted
      stacktrace capped at 8192 bytes as the "error.stack" metadata entry

  Spans join the caller's current trace when one exists, else open a new
  one. Best-effort: a failing handler never raises into the job process.
  """

  # The in-flight job span rides the process dictionary: Oban fires the
  # start and the stop/exception events in the job's own process.
  @span_key :dataflow_oban_span

  # --- attach / detach ---------------------------------------------------------

  @doc """
  Attaches the job handler for `prefix ++ [:job, :start | :stop | :exception]`
  events. Returns `:ok`, `{:error, :already_exists}` when the same prefix is
  attached twice, or `{:error, :telemetry_not_available}` when :telemetry
  is not loaded.
  """
  def attach(prefix) when is_list(prefix) do
    if Code.ensure_loaded?(:telemetry) do
      events = for suffix <- [[:job, :start], [:job, :stop], [:job, :exception]], do: prefix ++ suffix

      :telemetry.attach_many(prefix, events, &__MODULE__.handle_event/4, nil)
    else
      {:error, :telemetry_not_available}
    end
  end

  def attach(_prefix), do: {:error, :bad_prefix}

  @doc "Detaches the handler installed by `attach/1`. Inverse return values."
  def detach(prefix) when is_list(prefix) do
    if Code.ensure_loaded?(:telemetry) do
      :telemetry.detach(prefix)
    else
      {:error, :telemetry_not_available}
    end
  end

  def detach(_prefix), do: {:error, :bad_prefix}

  # --- :telemetry handler --------------------------------------------------------

  # Oban fires the lifecycle events synchronously in the job's own process:
  # start opens the span (held under @span_key), stop/exception closes it.
  # Like the Ecto handler the span is built off the process-local stack (no
  # push/pop), so the handler leaves the job's span context exactly as it
  # found it.
  def handle_event(event, measurements, metadata, _config) do
    if Dataflow.enabled?() do
      case Enum.take(event, -2) do
        [:job, :start] -> open_job_span(metadata)
        [:job, :stop] -> close_job_span(measurements, &Dataflow.Span.status(&1, 200))
        [:job, :exception] -> close_job_span(measurements, &record_failure(&1, metadata))
        _suffix -> :ok
      end
    end

    :ok
  rescue
    _ -> :ok
  catch
    _kind, _value -> :ok
  end

  defp open_job_span(metadata) do
    job = metadata[:job]

    span =
      Dataflow.Span.new("oban." <> worker_name(job), "FUNCTION_CALL", Dataflow.current_span())
      |> Dataflow.Span.attr("oban.queue", queue_name(job))
      |> Dataflow.Span.attr("oban.attempt", attempt_count(job))

    Process.put(@span_key, span)
    :ok
  end

  defp close_job_span(measurements, decorate) do
    case Process.get(@span_key) do
      nil ->
        :ok

      span ->
        Process.delete(@span_key)
        span = backdate(span, duration_ms(measurements))
        Dataflow.Span.end_span(decorate.(span))
        :ok
    end
  end

  defp record_failure(span, metadata) do
    message =
      Dataflow.Crash.format_error(metadata[:kind] || :error, metadata[:error], metadata[:stacktrace] || [])

    span
    |> Dataflow.Span.attr("error.stack", Dataflow.Crash.clip_stack(metadata[:stacktrace]))
    |> Dataflow.Span.record_error(message)
    |> Dataflow.Span.status(500)
  end

  # The span was created at the start event but measures a job that already
  # ran: backdate start/mono so end_span's duration math yields the measured
  # value.
  defp backdate(span, duration_ms) do
    %{
      span
      | start_ms: System.system_time(:millisecond) - duration_ms,
        mono_start: System.monotonic_time(:millisecond) - duration_ms
    }
  end

  # Oban reports job durations in native time units under :duration.
  defp duration_ms(measurements) do
    duration = measurements[:duration] || 0

    if is_integer(duration) do
      System.convert_time_unit(duration, :native, :millisecond)
    else
      0
    end
  end

  # Only the worker name, queue and attempt travel as fields; the args map
  # may carry end-user data and is never read.
  # Atom.to_string/1 renders "Elixir.MyApp.Worker" on recent OTP — keep
  # the bare module path the dashboard expects.
  defp worker_name(%{worker: worker}) when is_atom(worker) and worker != nil do
    case Atom.to_string(worker) do
      "Elixir." <> rest -> rest
      name -> name
    end
  end
  defp worker_name(%{worker: worker}) when is_binary(worker), do: worker
  defp worker_name(_job), do: "unknown"

  defp queue_name(%{queue: queue}) when is_atom(queue) and queue != nil, do: Atom.to_string(queue)
  defp queue_name(%{queue: queue}) when is_binary(queue), do: queue
  defp queue_name(_job), do: "unknown"

  defp attempt_count(%{attempt: attempt}) when is_integer(attempt), do: Integer.to_string(attempt)
  defp attempt_count(_job), do: "0"
end

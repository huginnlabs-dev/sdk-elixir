defmodule Dataflow.Ecto do
  @moduledoc """
  Ecto query tracing over :telemetry. `Dataflow.attach_ecto/1` subscribes a
  handler to the standard `[:<prefix>, :repo, :query]` events; every fired
  event becomes a DB_QUERY span:

    * name — verb + first table reference ("SELECT orders", "INSERT users"),
      derived by `Dataflow.SQL`
    * callee_package — the database system derived from the repo's adapter
      ("postgres", "mysql", "sqlite", ...)
    * metadata — "db.system" and the single-spaced, 200-char-truncated
      "db.statement"; parameter values are never captured (Ecto sends them
      separately from the statement text and they are ignored here)
    * duration — from the telemetry measurements
    * failures — status 500 with the error message

  Spans join the caller's current trace when one exists, else open a new
  one. Best-effort: a failing handler never raises into the caller.
  """

  # --- attach / detach ---------------------------------------------------------

  @doc """
  Attaches the query handler for `prefix ++ [:repo, :query]` events.
  Returns `:ok`, `{:error, :already_exists}` when the same prefix is
  attached twice, or `{:error, :telemetry_not_available}` when :telemetry
  is not loaded.
  """
  def attach(prefix) when is_list(prefix) do
    if Code.ensure_loaded?(:telemetry) do
      :telemetry.attach(prefix, prefix ++ [:repo, :query], &__MODULE__.handle_event/4, nil)
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

  # Ecto fires [:prefix, :repo, :query] synchronously in the calling
  # process at query completion, with the durations in `measurements`
  # (native time units) and the repo/query/error in `metadata`. The span
  # is built off the process-local stack (no push/pop): the handler must
  # leave the caller's span context exactly as it found it.
  def handle_event(_event, measurements, metadata, _config) do
    if Dataflow.enabled?(), do: emit_query_span(measurements, metadata)

    :ok
  rescue
    _ -> :ok
  catch
    _kind, _value -> :ok
  end

  defp emit_query_span(measurements, metadata) do
    query = metadata[:query]
    system = system_for(metadata[:repo])
    duration_ms = duration_ms(measurements)

    span =
      Dataflow.Span.new(Dataflow.SQL.summary(query), "DB_QUERY", Dataflow.current_span())
      |> backdate(duration_ms)

    span = %{span | callee: system}
    span = Dataflow.Span.attr(span, "db.system", system)

    span =
      case Dataflow.SQL.clip_statement(query) do
        "" -> span
        clipped -> Dataflow.Span.attr(span, "db.statement", clipped)
      end

    span =
      case error_from(metadata) do
        nil ->
          Dataflow.Span.status(span, 200)

        error ->
          span
          |> Dataflow.Span.record_error(error_text(error))
          |> Dataflow.Span.status(500)
      end

    Dataflow.Span.end_span(span)
    :ok
  end

  # The span was created "now" but measures a query that already ran:
  # backdate start/mono so end_span's duration math yields the measured
  # value.
  defp backdate(span, duration_ms) do
    %{
      span
      | start_ms: System.system_time(:millisecond) - duration_ms,
        mono_start: System.monotonic_time(:millisecond) - duration_ms
    }
  end

  defp duration_ms(measurements) do
    total = measurements[:total_time] || measurements[:query_time] || 0

    if is_integer(total) do
      System.convert_time_unit(total, :native, :millisecond)
    else
      0
    end
  end

  defp error_from(metadata), do: metadata[:error] || metadata[:exception]

  defp error_text(%module{} = error) do
    if function_exported?(module, :message, 1), do: Exception.message(error), else: inspect(error)
  end

  defp error_text(error) when is_binary(error), do: error
  defp error_text({_kind, reason, _stacktrace}), do: error_text(reason)
  defp error_text(error), do: inspect(error)

  # Repo adapter → db system. Anything unrecognized (or a hand-rolled repo
  # without an adapter) degrades to the adapter module's last segment.
  defp system_for(repo) when is_atom(repo) and repo != nil do
    if Code.ensure_loaded?(repo) and function_exported?(repo, :__adapter__, 0) do
      adapter_system(repo.__adapter__())
    else
      "ecto"
    end
  end

  defp system_for(_repo), do: "ecto"

  defp adapter_system(Ecto.Adapters.Postgres), do: "postgres"
  defp adapter_system(Ecto.Adapters.MySQL), do: "mysql"
  defp adapter_system(Ecto.Adapters.MyXQL), do: "mysql"
  defp adapter_system(Ecto.Adapters.SQLite3), do: "sqlite"
  defp adapter_system(Ecto.Adapters.Tds), do: "sqlserver"
  defp adapter_system(adapter), do: adapter |> Module.split() |> List.last() |> String.downcase()
end

defmodule Dataflow.Crash do
  @moduledoc """
  Crash capture with stack traces: `capture/1` wraps a zero-arity function
  and, when it raises, throws or exits, records the crash on the current
  span — status 500, the `Exception.format/3` rendering truncated to 500
  characters as the error message, and the formatted stacktrace capped at
  8192 bytes (from the top) as the "error.stack" metadata entry. The crash
  is never swallowed: raises propagate through `reraise/3` (preserving the
  stacktrace), throws and exits through `:erlang.raise/3` with the original
  stack.

  Without a current span (nothing opened by `Dataflow.trace/2` or
  `Dataflow.start_server_span/2`) the crash is recorded on a synthetic
  "exception" span instead, so crashes stay visible outside traced work.
  Recording is best-effort — a failure to record never masks the original
  crash — and with tracing disabled `capture/1` is a pure pass-through:
  the function runs bare, with no wrapper at all.

  `Dataflow.PlugCrash` builds on this module to capture Plug router
  crashes the same way.
  """

  @max_error 500
  @max_stack_bytes 8_192

  # --- capture -----------------------------------------------------------------

  @doc """
  Runs `fun` and records any crash on the current span (or a synthetic
  "exception" span when none is open) before propagating it unchanged.
  With tracing disabled the function runs bare — no wrapper at all.
  """
  def capture(fun) when is_function(fun, 0) do
    if Dataflow.enabled?() do
      try do
        fun.()
      rescue
        e ->
          Dataflow.Crash.record_exception(:error, e, __STACKTRACE__)
          reraise e, __STACKTRACE__
      catch
        kind, value ->
          Dataflow.Crash.record_exception(kind, value, __STACKTRACE__)
          :erlang.raise(kind, value, __STACKTRACE__)
      end
    else
      fun.()
    end
  end

  # --- recording -----------------------------------------------------------------

  @doc """
  Records a caught `kind`/`reason` crash — and, when `conn` is given, the
  request line as "error.request" metadata — on the current span, or on a
  synthetic "exception" span when none is open. The recording rides the
  process-local span stack and never ends a span the caller owns; returns
  `:ok` in every case, so it can never mask the crash it records.
  """
  def record_exception(kind, reason, stacktrace, conn \\ nil) when is_atom(kind) do
    if Dataflow.enabled?(), do: do_record(kind, reason, stacktrace, conn)

    :ok
  rescue
    _ -> :ok
  catch
    _kind, _value -> :ok
  end

  # Like the Ecto handler: the synthetic span is built off the process-local
  # stack (no push/pop) and ended immediately, since nobody else will.
  defp do_record(kind, reason, stacktrace, conn) do
    message = format_error(kind, reason, stacktrace)
    meta = [{"error.stack", clip_stack(stacktrace)}] ++ request_attrs(conn)

    case Dataflow.current_span() do
      nil ->
        Dataflow.Span.new("exception", "EXCEPTION", nil)
        |> Dataflow.Span.record_error(message)
        |> Dataflow.Span.status(500)
        |> put_meta(meta)
        |> Dataflow.Span.end_span()

        :ok

      span ->
        span
        |> Dataflow.Span.record_error(message)
        |> Dataflow.Span.status(500)
        |> put_meta(meta)

        :ok
    end
  end

  # The current-span branch discards the returned copies on purpose: the
  # stack copies are the truth, and the caller keeps ending spans as usual.
  defp put_meta(span, meta),
    do: Enum.reduce(meta, span, fn {k, v}, acc -> Dataflow.Span.attr(acc, k, v) end)

  defp request_attrs(nil), do: []

  defp request_attrs(%{request_method: method, request_path: path})
       when is_binary(method) and is_binary(path),
       do: [{"error.request", String.upcase(method) <> " " <> path}]

  defp request_attrs(_conn), do: []

  # --- pure helpers (unit-tested) -------------------------------------------------

  @doc """
  Renders a caught crash the way the BEAM does (`Exception.format/3`),
  truncated to 500 characters — the span's error message. Broken
  exceptions degrade to `inspect/1` instead of raising.
  """
  def format_error(kind, reason, stacktrace) do
    formatted =
      try do
        Exception.format(kind, reason, stacktrace)
      rescue
        _ -> inspect({kind, reason})
      catch
        _k, _v -> inspect({kind, reason})
      end

    String.slice(formatted, 0, @max_error)
  end

  @doc """
  Formats a stacktrace (`Exception.format_stacktrace/1`) capped at 8192
  bytes, keeping the top of the trace — the frames closest to the crash.
  Non-list input yields "".
  """
  def clip_stack(stacktrace) when is_list(stacktrace) do
    Exception.format_stacktrace(stacktrace)
    |> clip_bytes(@max_stack_bytes)
  end

  def clip_stack(_stacktrace), do: ""

  defp clip_bytes(binary, max) when byte_size(binary) <= max, do: binary

  defp clip_bytes(binary, max) do
    <<clipped::binary-size(max), _::binary>> = binary
    clipped
  end
end

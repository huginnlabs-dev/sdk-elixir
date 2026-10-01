defmodule Dataflow.Logs do
  @moduledoc """
  Application log shipping with trace correlation: `Dataflow.log/3` and the
  `debug/info/warn/error` helpers record a wire log entry — timestamped,
  level-normalized, carrying the current span's trace/span ids and
  stringified fields — into a bounded GenServer buffer. A background
  flusher posts batches (≤1000 entries) to the REST logs endpoint every
  500 ms, or as soon as 50 lines are buffered; the buffer holds at most
  1024 entries and drops the oldest on overflow.

  `attach_logger/0` installs an Erlang `:logger` handler (OTP 21+ handler
  API) that forwards `Logger` messages into the same pipeline — `warning`
  maps to `warn`, metadata becomes fields where stringifiable.

  Best-effort by design: logging never raises into the caller, a failed
  batch is retried once and then dropped, and with the SDK disabled every
  entry point is a no-op.
  """

  use GenServer
  require Logger

  @flush_interval_ms 500
  @threshold 50
  @max_batch 1_000
  @buffer_size 1_024
  @max_fields 50
  @timeout_ms 5_000

  # :logger/Logger internal metadata that never becomes a field.
  @internal_meta ~w(time gl pid ancestors callers crash_reason report_cb
                    logger_formatter domain erl_level mfa file line)a

  @handler_id :dataflow_log_handler

  # --- client API ------------------------------------------------------------

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @doc "Convenience for `log(\"debug\", message, fields)`."
  def debug(message, fields \\ %{}), do: log("debug", message, fields)

  @doc "Convenience for `log(\"info\", message, fields)`."
  def info(message, fields \\ %{}), do: log("info", message, fields)

  @doc "Convenience for `log(\"warn\", message, fields)`."
  def warn(message, fields \\ %{}), do: log("warn", message, fields)

  @doc "Convenience for `log(\"error\", message, fields)`."
  def error(message, fields \\ %{}), do: log("error", message, fields)

  @doc """
  Records a log line at `level`, joined to the current span's trace/span
  ids (empty when no span is open). Fields are stringified and capped at
  50 entries. Best-effort: never raises; no-op with the SDK disabled.
  """
  def log(level, message, fields) do
    if Dataflow.enabled?() do
      record(normalize_level(level), message, stringify_fields(fields))
    end

    :ok
  rescue
    _ -> :ok
  catch
    _kind, _value -> :ok
  end

  @doc "Flushes buffered entries now. Best-effort; returns :ok in every case."
  def flush do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      _pid -> GenServer.call(__MODULE__, :flush, 30_000)
    end
  rescue
    _ -> :ok
  catch
    _kind, _value -> :ok
  end

  # --- :logger handler -----------------------------------------------------------

  @doc """
  Installs the Erlang `:logger` handler that forwards every `Logger`
  message into the log buffer (`warning` → `warn`; metadata becomes fields
  where stringifiable). Returns `:ok`, or `{:error, :already_exists}`
  when the handler is already attached.
  """
  def attach_logger do
    case :logger.add_handler(@handler_id, __MODULE__, %{}) do
      :ok -> :ok
      {:error, {:already_exist, _id}} -> {:error, :already_exists}
      {:error, reason} -> {:error, reason}
    end
  rescue
    _ -> {:error, :attach_failed}
  end

  @doc "Removes the handler installed by `attach_logger/0`. Inverse return values."
  def detach_logger do
    case :logger.remove_handler(@handler_id) do
      :ok -> :ok
      {:error, {:not_found, _id}} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  rescue
    _ -> {:error, :detach_failed}
  end

  # Runs synchronously in the logging process (default :logger sync mode):
  # the same path as log/3, with Logger metadata instead of the caller's
  # fields. Every failure is swallowed — a handler must never crash the
  # process that logs.
  def log(event, _config) when is_map(event) do
    if Dataflow.enabled?() do
      record(normalize_level(event.level), event_text(event.msg), stringify_metadata(event.meta))
    end

    :ok
  rescue
    _ -> :ok
  catch
    _kind, _value -> :ok
  end

  # --- server ----------------------------------------------------------------

  @impl true
  def init(_state) do
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:ok, %{buffer: :queue.new()}}
  end

  @impl true
  def handle_cast({:enqueue, entry}, state) do
    buffer = add_capped(state.buffer, entry)

    if :queue.len(buffer) >= @threshold do
      # One-shot message — handle_info(:flush) owns the periodic timer and
      # must not be re-armed from here.
      Process.send_after(self(), :flush_soon, 0)
    end

    {:noreply, %{state | buffer: buffer}}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    {:reply, :ok, flush(state)}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush(state)
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:noreply, state}
  end

  def handle_info(:flush_soon, state), do: {:noreply, flush(state)}

  def handle_info(_msg, state), do: {:noreply, state}

  # Takes at most @max_batch entries and ships them; the batch is consumed
  # either way — one retry, then drop (log shipping must never back up).
  defp flush(%{buffer: q} = state) do
    batch = q |> :queue.to_list() |> Enum.take(@max_batch)

    cond do
      batch == [] ->
        state

      true ->
        ship(batch)
        %{state | buffer: drop_batch(q, length(batch))}
    end
  end

  defp drop_batch(q, n) do
    {_taken, rest} = :queue.split(n, q)
    rest
  end

  defp ship(batch) do
    base = Dataflow.Manifest.http_base_url(settings(:endpoint, ""))
    api_key = settings(:api_key, "")

    if is_binary(base) and api_key != "" do
      body = JSON.encode!(build_batch(batch))

      case post_logs(base, api_key, body) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.debug("dataflow: log send failed, retrying: #{inspect(reason)}")

          case post_logs(base, api_key, body) do
            :ok -> :ok
            {:error, reason} -> Logger.debug("dataflow: log batch dropped: #{inspect(reason)}")
          end
      end
    end

    :ok
  rescue
    _ -> :ok
  catch
    _kind, _value -> :ok
  end

  defp post_logs(base, api_key, body) do
    headers = [{~c"content-type", ~c"application/json"}, {~c"x-api-key", String.to_charlist(api_key)}]
    request = {String.to_charlist(base <> "/api/v1/logs"), headers, ~c"application/json", body}

    case :httpc.request(:post, request, [timeout: @timeout_ms, connect_timeout: @timeout_ms], [body_format: :binary]) do
      {:ok, {{_http_version, 200, _reason}, _headers, _resp_body}} ->
        :ok

      {:ok, {{_http_version, status, reason}, _headers, _resp_body}} ->
        {:error, "logs status #{status} #{reason}"}

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  defp record(level, message, fields) when is_map(fields) do
    span = Dataflow.current_span()

    entry =
      build_log(level, message, (span && span.trace_id) || "", (span && span.span_id) || "", Dataflow.service_name(), fields)

    GenServer.cast(__MODULE__, {:enqueue, entry})
    :ok
  end

  defp record(_level, _message, _fields), do: :ok

  defp settings(key, fallback) do
    Application.get_env(:dataflow, :settings, %{}) |> Map.get(key, fallback)
  end

  # --- pure helpers (unit-tested) -------------------------------------------------

  @doc false
  # Wire level vocabulary; anything unknown reports as "info".
  def normalize_level(level) when level in [:debug, "debug"], do: "debug"
  def normalize_level(level) when level in [:info, "info"], do: "info"
  def normalize_level(level) when level in [:warn, :warning, "warn", "warning"], do: "warn"
  def normalize_level(level) when level in [:error, "error"], do: "error"
  def normalize_level(level) when level in [:critical, :alert, :emergency, "critical", "alert", "emergency"], do: "error"
  def normalize_level(_level), do: "info"

  @doc false
  # One wire log entry (POST /api/v1/logs body): unix-ms timestamp,
  # normalized level, stringified message, trace/span ids ("" when no span
  # is open) and stringified fields.
  def build_log(level, message, trace_id, span_id, service_name, fields) do
    %{
      "timestamp" => System.system_time(:millisecond),
      "level" => normalize_level(level),
      "message" => message_string(message),
      "trace_id" => trace_id || "",
      "span_id" => span_id || "",
      "service_name" => service_name || "",
      "fields" => stringify_fields(fields)
    }
  end

  @doc false
  # Bounded-buffer insert: drops the oldest entry once the cap is exceeded.
  def add_capped(queue, entry, cap \\ @buffer_size) do
    q = :queue.in(entry, queue)

    if :queue.len(q) > cap do
      {{:value, _dropped}, q} = :queue.out(q)
      q
    else
      q
    end
  end

  @doc false
  # The batch body: {"logs": [entry, ...]}.
  def build_batch(entries) when is_list(entries), do: %{"logs" => entries}

  @doc false
  # Direct caller fields: stringified values (Kernel.to_string with an
  # inspect fallback), capped at 50 entries.
  def stringify_fields(fields) when is_map(fields) do
    fields
    |> Enum.take(@max_fields)
    |> Map.new(fn {k, v} -> {key_string(k), field_string(v)} end)
    |> Map.reject(fn {k, v} -> is_nil(k) or is_nil(v) end)
  end

  def stringify_fields(_fields), do: %{}

  @doc false
  # Logger metadata: keeps only the cleanly stringifiable keys, internal
  # :logger metadata excluded, capped at 50 entries.
  def stringify_metadata(meta) when is_map(meta) do
    meta
    |> Map.drop(@internal_meta)
    |> Enum.take(@max_fields)
    |> Map.new(fn {k, v} -> {key_string(k), meta_string(v)} end)
    |> Map.reject(fn {k, v} -> is_nil(k) or is_nil(v) end)
  end

  def stringify_metadata(_meta), do: %{}

  # --- stringification helpers -----------------------------------------------------

  defp message_string(message) when is_binary(message), do: message

  defp message_string(message) do
    try do
      Kernel.to_string(message)
    rescue
      _ -> inspect(message)
    catch
      _kind, _value -> inspect(message)
    end
  end

  defp key_string(key) when is_binary(key), do: key
  defp key_string(key) when is_atom(key), do: Atom.to_string(key)
  defp key_string(key) when is_integer(key), do: Integer.to_string(key)
  defp key_string(_key), do: nil

  defp field_string(value) when is_binary(value), do: value

  defp field_string(value) do
    try do
      Kernel.to_string(value)
    rescue
      _ -> inspect(value)
    catch
      _kind, _value -> inspect(value)
    end
  end

  defp meta_string(value) when is_binary(value), do: value

  defp meta_string(value) do
    try do
      Kernel.to_string(value)
    rescue
      _ -> nil
    catch
      _kind, _value -> nil
    end
  end

  # :logger msg forms: {:text, chardata} / {:string, chardata} (what Elixir's
  # compiled Logger macros deliver) and {:report, map} (structured reports).
  defp event_text({kind, chardata}) when kind in [:text, :string], do: chardata_string(chardata)
  defp event_text({:report, report}), do: report_string(report)
  defp event_text(other), do: inspect(other)

  defp chardata_string(chardata) when is_binary(chardata), do: chardata

  defp chardata_string(chardata) do
    case :unicode.characters_to_binary(chardata) do
      text when is_binary(text) -> text
      _ -> inspect(chardata)
    end
  end

  defp report_string(report) when is_map(report) do
    case Map.get(report, :text) do
      text when is_binary(text) -> text
      _ -> inspect(report)
    end
  end

  defp report_string(other), do: inspect(other)
end

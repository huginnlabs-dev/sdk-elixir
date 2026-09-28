defmodule Dataflow.Span do
  @moduledoc """
  One measured unit of work. A plain map holding span state; ends via
  `end_span/1` (also called by `Dataflow.trace/2`'s after-block).
  """

  alias Dataflow.Pii

  defstruct ~w(trace_id span_id parent_span_id name kind start_ms mono_start
               sampled attrs payload error status callee caller)a

  def new(name, kind, parent, incoming_trace_id \\ nil) do
    ratio = settings(:sample_ratio, 1.0)
    sampled = ratio >= 1.0 or :rand.uniform() < ratio

    callee =
      if kind == "FUNCTION_CALL" do
        name |> String.split(".") |> hd()
      else
        ""
      end

    caller = if parent, do: parent.callee, else: ""

    %__MODULE__{
      trace_id: incoming_trace_id && incoming_trace_id != "" && incoming_trace_id ||
                  (parent && parent.trace_id) || uuid(),
      span_id: uuid(),
      parent_span_id: (parent && parent.span_id) || "",
      name: name,
      kind: kind,
      start_ms: System.system_time(:millisecond),
      mono_start: System.monotonic_time(:millisecond),
      sampled: sampled,
      attrs: %{},
      payload: %{},
      error: "",
      status: 0,
      callee: callee,
      caller: caller
    }
  end

  # Mutators persist through the process-local span stack: the map argument
  # may be stale by the time end_span runs, the stack copy is the truth.

  @doc "Plaintext attribute (metadata entry)."
  def attr(span, key, value) do
    Dataflow.update_span(span.span_id, fn sp -> %{sp | attrs: Map.put(sp.attrs, key, to_string(value))} end)
    %{span | attrs: Map.put(span.attrs, key, to_string(value))}
  end

  @doc "Payload field (encrypted at end when a key is configured)."
  def data(span, key, value) do
    Dataflow.update_span(span.span_id, fn sp -> %{sp | payload: Map.put(sp.payload, key, value)} end)
    %{span | payload: Map.put(span.payload, key, value)}
  end

  @doc "Payload field holding a pre-serialized JSON document."
  def data_json(span, key, raw), do: data(span, key, {:raw, raw})

  @doc "Marks this span's package (or host) for the data-flow graph."
  def callee(span, pkg) do
    Dataflow.update_span(span.span_id, fn sp -> %{sp | callee: pkg} end)
    %{span | callee: pkg}
  end

  def record_error(span, message) do
    Dataflow.update_span(span.span_id, fn sp ->
      error = if sp.error == "", do: message, else: "#{sp.error}; #{message}"
      status = (sp.status == 0 && 500) || sp.status
      %{sp | error: error, status: status}
    end)

    error = if span.error == "", do: message, else: "#{span.error}; #{message}"
    %{span | error: error, status: (span.status == 0 && 500) || span.status}
  end

  def status(span, code) do
    Dataflow.update_span(span.span_id, fn sp -> %{sp | status: code} end)
    %{span | status: code}
  end

  @doc "Ends the span and enqueues it for delivery. Idempotent per span."
  def end_span(%__MODULE__{sampled: false}), do: :ok

  def end_span(%__MODULE__{} = span) do
    stack = Process.get(:dataflow_stack) || []

    {fresh, rest} =
      case List.keytake(stack, span.span_id, 0) do
        {{_id, sp}, rest} -> {sp, rest}
        nil -> {span, stack}
      end

    Process.put(:dataflow_stack, rest)
    end_span_impl(fresh)
  end

  defp end_span_impl(%__MODULE__{} = span) do
    duration_ms = System.monotonic_time(:millisecond) - span.mono_start

    meta =
      if map_size(span.payload) > 0 do
        fields = span.payload |> Map.keys() |> Enum.sort()
        meta = Map.put(span.attrs, "data.fields", Enum.join(fields, ","))

        case Pii.classify(fields) do
          "" -> meta
          pii -> Map.put(meta, "data.pii", pii)
        end
      else
        span.attrs
      end

    event = %{
      "event_id" => uuid(),
      "seq" => Dataflow.Pipeline.next_seq(),
      "trace_id" => span.trace_id,
      "span_id" => span.span_id,
      "parent_span_id" => span.parent_span_id,
      "type" => span.kind,
      "service_name" => Dataflow.service_name(),
      "name" => span.name,
      "caller_package" => span.caller,
      "callee_package" => span.callee,
      "function_name" => span.name,
      "timestamp" => span.start_ms,
      "duration_ms" => duration_ms,
      "status_code" => span.status,
      "error_message" => span.error,
      "payload" => payload_json(span.payload),
      "metadata" => meta
    }

    Dataflow.Pipeline.enqueue(JSON.encode!(event))
    :ok
  end

  defp payload_json(payload) when map_size(payload) == 0, do: nil

  defp payload_json(payload) do
    case Application.get_env(:dataflow, :envelope) do
      nil ->
        JSON.encode!(plain(payload))

      %{key: key, salt_hex: salt_hex} ->
        plain = JSON.encode!(plain(payload))
        iv = :crypto.strong_rand_bytes(12)

        {ct, tag} =
          :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, plain, <<>>, true)

        %{
          "encrypted" => true,
          # base64 payload rides as a JSON string inside data
          "data_b64" => Base.encode64(ct <> tag),
          "iv_b64" => Base.encode64(iv),
          "key_salt" => salt_hex
        }
    end
  end

  defp plain(payload) do
    Map.new(payload, fn
      {k, {:raw, json}} -> {k, {:raw_term, json}}
      {k, v} -> {k, v}
    end)
    |> then(fn m ->
      # {:raw_term, json} entries embed pre-serialized JSON documents
      Map.new(m, fn
        {k, {:raw_term, json}} -> {k, JSON.decode!(json)}
        {k, v} -> {k, v}
      end)
    end)
  end

  defp settings(key, fallback) do
    Application.get_env(:dataflow, :settings, %{}) |> Map.get(key, fallback)
  end

  defp uuid do
    # 128 bits: 32+16+4+12+2+14+48 — a proper UUIDv4 shape.
    <<a::32, b::16, _::4, c::12, _::2, d::14, e::48>> = :crypto.strong_rand_bytes(16)

    :io_lib.format("~8.16.0b-~4.16.0b-4~3.16.0b-8~3.16.0b-~12.16.0b", [a, b, c, d, e])
    |> to_string()
    |> String.downcase()
  end
end

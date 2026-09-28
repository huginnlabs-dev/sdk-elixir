defmodule Dataflow.Pipeline do
  @moduledoc """
  Delivery path: a GenServer holding the replay buffer. Every 300 ms (or on
  demand) it posts pending events to the REST ingest endpoint and trims the
  buffer up to the acked `last_seq`. Failed batches stay buffered.
  """

  use GenServer
  require Logger

  @flush_interval_ms 300
  @max_batch 500
  @ack_timeout_ms 15_000

  # --- client API ------------------------------------------------------------

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def next_seq, do: System.unique_integer([:positive, :monotonic])

  def enqueue(event_json) do
    if Dataflow.enabled?() do
      GenServer.cast(__MODULE__, {:enqueue, event_json})
    end
  end

  # --- server ----------------------------------------------------------------

  @impl true
  def init(_state) do
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:ok, %{buffer: :queue.new(), base: 1}}
  end

  @impl true
  def handle_cast({:enqueue, json}, state) do
    state = add_with_cap(state, json)
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush, state) do
    state = flush(state)
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:noreply, state}
  end

  defp add_with_cap(%{buffer: q, base: base} = state, json) do
    q = :queue.in(json, q)

    cap = settings(:buffer_size, 10_000)

    if :queue.len(q) > cap do
      {{_, q}, _} = :queue.out(q)
      %{state | buffer: q, base: base + 1}
    else
      %{state | buffer: q}
    end
  end

  defp flush(%{buffer: q} = state) do
    batch = q |> :queue.to_list() |> Enum.take(@max_batch)

    cond do
      batch == [] ->
        state

      true ->
        body = JSON.encode!(%{"events" => Enum.map(batch, &JSON.decode!/1)})

        case post_ingest(body) do
          {:ok, %{"last_seq" => acked}} when is_integer(acked) and acked > 0 ->
            trim(state, acked)

          {:ok, _} ->
            state

          {:error, reason} ->
            Logger.debug("dataflow: send failed, retrying: #{inspect(reason)}")
            state
        end
    end
  end

  defp trim(%{buffer: q, base: base} = state, acked) do
    drop = max(acked + 1 - base, 0) |> min(:queue.len(q))

    if drop <= 0 do
      state
    else
      {_, q} = :queue.split(drop, q)
      %{state | buffer: q, base: base + drop}
    end
  end

  defp post_ingest(body) do
    url = endpoint_url() <> "/api/v1/ingest"
    headers = [{~c"content-type", ~c"application/json"}, {~c"x-api-key", String.to_charlist(settings(:api_key, ""))}]

    request = {String.to_charlist(url), headers, ~c"application/json", body}

    :httpc.request(:post, request, [timeout: @ack_timeout_ms, connect_timeout: 5_000], [body_format: :binary])
    |> case do
      {:ok, {{_version, 200, _reason}, _headers, body}} ->
        {:ok, JSON.decode!(body)}

      {:ok, {{_version, status, reason}, _headers, _body}} ->
        {:error, "ingest status #{status} #{reason}"}

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  defp endpoint_url do
    ep = settings(:endpoint, "")
    # :httpc speaks http/1.1 over TCP; front the endpoint with a
    # TLS-terminating proxy for WAN deployments.
    ep |> String.trim_trailing("/") |> String.replace_prefix("https://", "http://")
  end

  defp settings(key, fallback) do
    Application.get_env(:dataflow, :settings, %{}) |> Map.get(key, fallback)
  end
end

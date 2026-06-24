defmodule Valvula.Server do
  @moduledoc """
  Per-limiter GenServer that owns one ETS table of buckets.

  One `Valvula.Server` per `name:` registered via `Valvula.start_link/1`.
  Buckets are stored in an ETS table for O(1) read access; the GenServer
  serialises writes (consume + cleanup) to keep counters consistent.

  ## Concurrency model

  - **Reads (status/2)**: Direct ETS lookup, no GenServer involvement.
  - **Writes (consume/2, reset/2)**: `GenServer.call/2` — atomic with
    respect to other consumes.
  - **Cleanup**: Periodic timer (every 60s) removes buckets idle for
    more than `2 * window_ms`.

  The ETS table is `:public` so callers can read buckets directly via
  `Valvula.Server.lookup/2` if they need raw access.
  """

  use GenServer
  require Logger

  @table_prefix :valvula_buckets_
  @cleanup_interval_ms 60_000
  @idle_multiplier 2

  @type config :: %{
          rate: pos_integer(),
          window: pos_integer(),
          burst: non_neg_integer()
        }

  @type stats :: %{
          rate: pos_integer(),
          window_ms: pos_integer(),
          burst: non_neg_integer(),
          bucket_count: non_neg_integer(),
          consumed_total: non_neg_integer(),
          rejected_total: non_neg_integer()
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Direct ETS lookup. Returns `{:ok, bucket}` or `:error`.

  Prefer `Valvula.status/2` over this in application code.
  """
  @spec lookup(GenServer.server(), term()) :: {:ok, Valvula.Bucket.t()} | :error
  def lookup(server, key) do
    table = table_name(server)

    case :ets.info(table, :name) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(table, key) do
          [{^key, bucket}] -> {:ok, bucket}
          [] -> :error
        end
    end
  end

  @doc """
  Server-side stats. Useful for monitoring dashboards.
  """
  @spec stats(GenServer.server()) :: stats()
  def stats(server) do
    GenServer.call(server, :stats)
  end

  @doc false
  # Eagerly validate opts so start_link raises rather than crashing the
  # supervisor from inside init/1. Called by the Valvula facade.
  @spec validate_config!(keyword()) :: :ok
  def validate_config!(opts) do
    _ = parse_config(opts)
    :ok
  end

  # ---------------------------------------------------------------------------
  # GenServer implementation
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    config = parse_config(opts)
    table = new_table(config)
    Process.send_after(self(), :cleanup, @cleanup_interval_ms)

    state = %{
      config: config,
      table: table,
      consumed_total: 0,
      rejected_total: 0
    }

    Logger.info(
      "Valvula.Server up: rate=#{config.rate} per #{config.window_ms}ms " <>
        "(max=#{config.max_tokens}, burst=#{config.burst})"
    )

    {:ok, state}
  end

  @impl true
  def handle_call({:consume, key, requested}, _from, state) do
    now_ms = System.monotonic_time(:millisecond)
    bucket = fetch_or_create_bucket(key, state)
    refilled = Valvula.Bucket.refill(bucket, now_ms)

    result =
      if refilled.tokens >= requested do
        new_bucket = %{
          refilled
          | tokens: refilled.tokens - requested,
            consumed_total: refilled.consumed_total + requested
        }

        :ets.insert(state.table, {key, new_bucket})

        {:ok, %{state | consumed_total: state.consumed_total + requested}}
      else
        retry_after = Valvula.Bucket.retry_after_ms(refilled, requested, now_ms)

        new_bucket = %{refilled | rejected_total: refilled.rejected_total + 1}
        :ets.insert(state.table, {key, new_bucket})

        {{:error, :rate_limited, retry_after},
         %{state | rejected_total: state.rejected_total + 1}}
      end

    case result do
      {payload, new_state} -> {:reply, payload, new_state}
    end
  end

  def handle_call({:reset, key}, _from, state) do
    fresh = fresh_bucket_for(state, key)
    :ets.insert(state.table, {key, fresh})
    {:reply, :ok, state}
  end

  def handle_call({:status, key}, _from, state) do
    case :ets.lookup(state.table, key) do
      [{^key, bucket}] ->
        refilled = Valvula.Bucket.refill(bucket, System.monotonic_time(:millisecond))
        {:reply, status_payload(refilled, state), state}

      [] ->
        # No bucket yet → equivalent to a full bucket for the user
        default = fresh_bucket_for(state, key)
        {:reply, status_payload(default, state), state}
    end
  end

  def handle_call(:stats, _from, state) do
    payload = %{
      rate: state.config.rate,
      window_ms: state.config.window_ms,
      burst: state.config.burst,
      bucket_count: :ets.info(state.table, :size) || 0,
      consumed_total: state.consumed_total,
      rejected_total: state.rejected_total
    }

    {:reply, payload, state}
  end

  def handle_call(:table_name, _from, state), do: {:reply, state.table, state}

  @impl true
  def handle_info(:cleanup, state) do
    now_ms = System.monotonic_time(:millisecond)
    idle_cutoff = state.config.window_ms * @idle_multiplier
    max_last_refill = now_ms - idle_cutoff

    # Use match spec form for delete (dialyzer-friendly).
    # Bucket schema is {key, %Valvula.Bucket{last_refill: lr, ...}}.
    :ets.select_delete(state.table, match_spec(max_last_refill))

    Process.send_after(self(), :cleanup, @cleanup_interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if :ets.info(state.table, :name) != :undefined, do: :ets.delete(state.table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp fetch_or_create_bucket(key, state) do
    case :ets.lookup(state.table, key) do
      [{^key, bucket}] -> bucket
      [] -> fresh_bucket_for(state, key)
    end
  end

  defp fresh_bucket_for(state, key) do
    Valvula.Bucket.new(key, %{
      max_tokens: state.config.max_tokens,
      refill_rate: state.config.refill_rate,
      window_ms: state.config.window_ms
    })
  end

  defp status_payload(bucket, state) do
    %{
      tokens: bucket.tokens,
      max: bucket.max_tokens,
      limited: bucket.tokens == 0,
      window_ms: state.config.window_ms,
      rate: state.config.rate,
      burst: state.config.burst,
      consumed_total: bucket.consumed_total,
      rejected_total: bucket.rejected_total
    }
  end

  # Parse and validate the configuration.
  defp parse_config(opts) do
    rate = Keyword.fetch!(opts, :rate)
    window = Keyword.fetch!(opts, :window)
    burst = Keyword.get(opts, :burst, 0)
    name = Keyword.get(opts, :name)

    # Validate before window resolution so callers get a clear message
    # about a bad :rate rather than the generic window error.
    if rate <= 0, do: raise(ArgumentError, ":rate must be > 0")
    if burst < 0, do: raise(ArgumentError, ":burst must be >= 0")
    if is_nil(name), do: raise(ArgumentError, ":name is required")

    window_ms = resolve_window(window)
    if window_ms <= 0, do: raise(ArgumentError, ":window must be > 0")

    %{
      name: name,
      rate: rate,
      refill_rate: rate,
      window_ms: window_ms,
      burst: burst,
      max_tokens: rate + burst
    }
  end

  # Resolve :window into milliseconds. Accepts either an integer
  # (already in ms) or a `:timer.seconds(N)`, `:timer.minutes(N)`, etc.
  # tuple. Anything else raises.
  defp resolve_window(window) when is_integer(window), do: window

  defp resolve_window({mod, n}) when is_atom(mod) and is_integer(n) do
    # Dialyzer can't prove the apply returns an integer for any atom,
    # but we already constrained mod to be :timer.* modules in the docs.
    mod.apply(n)
  end

  defp resolve_window(_),
    do: raise(ArgumentError, ":window must be ms (integer) or :timer.seconds(N)")

  defp new_table(%{name: name}) do
    table = :"#{@table_prefix}#{name}"
    :ets.new(table, [:set, :named_table, :public, read_concurrency: true])
    table
  end

  # Match spec for cleanup: delete buckets whose last_refill is older
  # than max_last_refill. Bucket schema: {key, %Valvula.Bucket{last_refill: lr, ...}}.
  # We bind the whole struct as $2 and reach last_refill via map_get.
  defp match_spec(max_last_refill) do
    [
      {{:"$1", :"$2"}, [{:<, {:map_get, :last_refill, :"$2"}, max_last_refill}], [true]}
    ]
  end

  # Public helper used by the Valvula facade to find the table for a
  # registered server by name.
  @doc false
  def table_name(server) do
    case GenServer.call(server, :table_name) do
      table when is_atom(table) -> table
      _ -> :"#{@table_prefix}#{server}"
    end
  catch
    :exit, _ -> :"#{@table_prefix}#{server}"
  end
end

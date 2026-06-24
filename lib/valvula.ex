defmodule Valvula do
  @moduledoc """
  Token-bucket rate limiter for Elixir with ETS backend and OTP-native
  GenServer.

  ## Algorithm

  **Token bucket with lazy refill.** Each `key` (user, IP, API token)
  gets a bucket that holds up to `max_tokens = rate + burst` tokens.
  Every `window_ms`, `rate` new tokens are added — but not by a timer.
  Instead, every `consume/2` call computes how many tokens would have
  been generated since `last_refill` and tops up accordingly. This
  gives O(1) memory per bucket and O(1) CPU per consume, no matter
  how long the bucket has been idle.

  ## Quick start

      # 1. Start a rate limiter
      {:ok, _pid} = Valvula.start_link(
        name: :api_limiter,
        rate: 100,                          # 100 tokens per window
        window: :timer.seconds(1),          # window = 1 second
        burst: 20                           # +20 burst → max 120 tokens
      )

      # 2. Consume one token (sync, recommended)
      case Valvula.consume(:api_limiter, key: "user_123") do
        :ok                                  # → proceed
        {:error, :rate_limited, retry_ms}   # → reject / retry
      end

      # 3. Consume N tokens at once
      Valvula.consume(:api_limiter, key: "user_123", tokens: 5)

      # 4. Introspection
      Valvula.status(:api_limiter, key: "user_123")
      # => %{tokens: 87, max: 120, limited: false, ...}

      Valvula.reset(:api_limiter, key: "user_123")
      Valvula.stats(:api_limiter)
      # => %{rate: 100, window_ms: 1000, ..., consumed_total: 137, rejected_total: 4}

  ## Concurrency

  - **Reads** (`status/2`, `lookup/2`) go directly to ETS — no GenServer
    round-trip, fully concurrent.
  - **Writes** (`consume/2`, `reset/2`) go through `GenServer.call/2`
    so concurrent consumes can't race on the same bucket.
  - **Cleanup**: every 60s, buckets idle for `> 2 * window_ms` are
    deleted from ETS to bound memory.

  ## Zero dependencies

  No Redis. No Postgres. No Mnesia. Just `:ets` + `:timer` from stdlib.
  """

  alias Valvula.Server

  @type server :: GenServer.server()
  @type key :: term()

  @typedoc "Result of a successful `consume/2`."
  @type consume_ok :: :ok

  @typedoc "Result of a rejected `consume/2`."
  @type consume_err :: {:error, :rate_limited, non_neg_integer()}

  @typedoc "Result of `consume/2`."
  @type consume_result :: consume_ok() | consume_err()

  @typedoc "Status payload from `status/2`."
  @type status :: %{
          required(:tokens) => non_neg_integer(),
          required(:max) => pos_integer(),
          required(:limited) => boolean(),
          required(:window_ms) => pos_integer(),
          required(:rate) => pos_integer(),
          required(:burst) => non_neg_integer(),
          required(:consumed_total) => non_neg_integer(),
          required(:rejected_total) => non_neg_integer()
        }

  @typedoc "Aggregate stats from `stats/1`."
  @type stats :: %{
          required(:rate) => pos_integer(),
          required(:window_ms) => pos_integer(),
          required(:burst) => non_neg_integer(),
          required(:bucket_count) => non_neg_integer(),
          required(:consumed_total) => non_neg_integer(),
          required(:rejected_total) => non_neg_integer()
        }

  # ---------------------------------------------------------------------------
  # Lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Starts a new rate limiter GenServer.

  ## Required options

    * `:name` — registered name (atom) for the GenServer and the ETS table
    * `:rate` — tokens regenerated per `:window` (must be > 0)
    * `:window` — `:timer.seconds(N)` or a positive integer (ms)

  ## Optional

    * `:burst` — extra tokens above `:rate` (default `0`)

  ## Examples

      Valvula.start_link(name: :api, rate: 100, window: :timer.seconds(1))
      Valvula.start_link(name: :login, rate: 5, window: :timer.minutes(1), burst: 2)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    server = Keyword.fetch!(opts, :name)

    # Eagerly validate config so the caller gets a clean raise instead of
    # a supervisor crash from inside init/1.
    Server.validate_config!(opts)
    GenServer.start_link(Server, opts, name: server)
  end

  # child_spec/1 is required to use Valvula as a supervised child.
  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @doc """
  Stops a rate limiter and deletes its ETS table.
  """
  @spec stop(server()) :: :ok
  def stop(server) do
    GenServer.stop(server)
  catch
    :exit, _ -> :ok
  end

  # ---------------------------------------------------------------------------
  # Consume / reset
  # ---------------------------------------------------------------------------

  @doc """
  Attempts to consume one token for `key` on the named limiter.

  Returns `:ok` on success, or `{:error, :rate_limited, retry_after_ms}`
  on rejection. Pass `tokens: N` to consume more than one at a time.

  ## Examples

      iex> Valvula.start_link(name: :test, rate: 2, window: 60_000)
      iex> :ok = Valvula.consume(:test, key: "u1")
      iex> :ok = Valvula.consume(:test, key: "u1")
      iex> {:error, :rate_limited, _} = Valvula.consume(:test, key: "u1")
  """
  @spec consume(server(), keyword()) :: consume_result()
  def consume(server, opts) when is_list(opts) do
    key = Keyword.fetch!(opts, :key)
    requested = Keyword.get(opts, :tokens, 1)

    if requested < 1 do
      raise ArgumentError, ":tokens must be >= 1, got #{requested}"
    end

    GenServer.call(server, {:consume, key, requested})
  end

  @doc """
  Resets the bucket for `key` to full. Useful for admin actions
  ("unblock this user", "clear after a sync that should be free").
  """
  @spec reset(server(), keyword()) :: :ok
  def reset(server, opts) when is_list(opts) do
    key = Keyword.fetch!(opts, :key)
    GenServer.call(server, {:reset, key})
  end

  # ---------------------------------------------------------------------------
  # Introspection
  # ---------------------------------------------------------------------------

  @doc """
  Returns the current state of `key`'s bucket.

  Status shape:

      %{
        tokens: 87,
        max: 100,
        limited: false,
        window_ms: 1000,
        rate: 100,
        burst: 0,
        consumed_total: 13,
        rejected_total: 0
      }

  If `key` has never been seen, returns a synthetic "full bucket"
  payload (the same you'd get right after `reset/2`).
  """
  @spec status(server(), keyword()) :: status()
  def status(server, opts) when is_list(opts) do
    key = Keyword.fetch!(opts, :key)
    GenServer.call(server, {:status, key})
  end

  @doc """
  Returns aggregate statistics for the limiter (across all keys).
  """
  @spec stats(server()) :: stats()
  def stats(server), do: Server.stats(server)

  @doc """
  Direct ETS lookup — returns `{:ok, bucket}` or `:error`. Bypasses the
  GenServer for the cheapest possible read.
  """
  @spec lookup(server(), key()) :: {:ok, Valvula.Bucket.t()} | :error
  def lookup(server, key), do: Server.lookup(server, key)
end

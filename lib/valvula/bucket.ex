defmodule Valvula.Bucket do
  @moduledoc """
  Per-key state for the token-bucket algorithm.

  Each `key` (any term — typically a user_id, IP, or API token) gets its
  own bucket. A bucket tracks:

    * `tokens` — current number of available tokens
    * `max_tokens` — capacity (set at startup)
    * `refill_rate` — tokens regenerated per `window_ms`
    * `window_ms` — refill window in milliseconds
    * `last_refill` — monotonic time (ms) of the last refill calculation
    * `consumed_total` — lifetime counter of consumed tokens
    * `rejected_total` — lifetime counter of rejections

  The bucket is "lazy-refilled": tokens are not added by a per-bucket
  timer. Instead, every `consume/2` call computes how many tokens would
  have been added since `last_refill` and tops up accordingly. This
  means **O(1) memory per bucket, O(1) CPU per consume**, regardless
  of how many buckets are alive.
  """

  @type key :: term()
  @type t :: %__MODULE__{
          key: key(),
          tokens: non_neg_integer(),
          max_tokens: non_neg_integer(),
          refill_rate: non_neg_integer(),
          window_ms: non_neg_integer(),
          last_refill: integer(),
          consumed_total: non_neg_integer(),
          rejected_total: non_neg_integer()
        }

  @enforce_keys [:key, :max_tokens, :refill_rate, :window_ms]
  defstruct [
    :key,
    :last_refill,
    tokens: 0,
    max_tokens: 0,
    refill_rate: 0,
    window_ms: 0,
    consumed_total: 0,
    rejected_total: 0
  ]

  @doc """
  Creates a fresh bucket for `key` with the given config.

  The bucket starts FULL (`tokens == max_tokens`) so the first request
  after a cold start is never rejected by the limiter.

  ## Examples

      iex> Valvula.Bucket.new("user_1", %{max_tokens: 10, refill_rate: 5, window_ms: 1_000})
      %Valvula.Bucket{key: "user_1", tokens: 10, max_tokens: 10, ...}
  """
  @spec new(key(), map()) :: t()
  def new(key, %{max_tokens: max_tokens, refill_rate: refill_rate, window_ms: window_ms}) do
    %__MODULE__{
      key: key,
      tokens: max_tokens,
      max_tokens: max_tokens,
      refill_rate: refill_rate,
      window_ms: window_ms,
      last_refill: System.monotonic_time(:millisecond)
    }
  end

  @doc """
  Computes the lazy-refilled bucket. Does NOT modify state — returns a new
  bucket with `tokens` topped up to the current `now_ms` and `last_refill`
  advanced accordingly.
  """
  @spec refill(t(), integer()) :: t()
  def refill(%__MODULE__{} = bucket, now_ms) do
    elapsed = max(now_ms - bucket.last_refill, 0)
    windows_passed = div(elapsed, max(bucket.window_ms, 1))
    added = windows_passed * bucket.refill_rate
    new_tokens = min(bucket.max_tokens, bucket.tokens + added)
    new_last_refill = bucket.last_refill + windows_passed * bucket.window_ms
    %{bucket | tokens: new_tokens, last_refill: new_last_refill}
  end

  @doc """
  Returns the milliseconds until at least `requested` tokens will be
  available, assuming no other consumers. Used to populate the
  `retry_after_ms` field on rejections.

  Always returns a non-negative integer.
  """
  @spec retry_after_ms(t(), pos_integer(), integer()) :: non_neg_integer()
  def retry_after_ms(%__MODULE__{} = bucket, requested, now_ms) do
    needed = max(requested - bucket.tokens, 0)

    if needed == 0 or bucket.refill_rate == 0 do
      0
    else
      # ms until the next (needed / refill_rate) windows
      windows_needed = ceil(needed / bucket.refill_rate)
      next_window_at = bucket.last_refill + bucket.window_ms * windows_needed
      max(next_window_at - now_ms, 0)
    end
  end
end

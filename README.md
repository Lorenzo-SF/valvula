# Valvula

> Token-bucket rate limiter for Elixir with ETS backend and OTP-native GenServer.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.md)
[![Elixir](https://img.shields.io/badge/elixir-~%201.14-purple.svg)](mix.exs)

Valvula gives you a per-key rate limiter with a token-bucket algorithm
and **zero external dependencies** — no Redis, no Postgres, no Mnesia.
Just `:ets` and `:timer` from stdlib.

## Quick start

```elixir
# 1. Start a rate limiter
{:ok, _} = Valvula.start_link(
  name: :api_limiter,
  rate: 100,                       # 100 tokens per window
  window: :timer.seconds(1),       # window = 1 second
  burst: 20                        # +20 burst → max 120 tokens
)

# 2. Consume one token (sync, recommended)
case Valvula.consume(:api_limiter, key: "user_123") do
  :ok                              # → proceed
  {:error, :rate_limited, retry}   # → reject / wait `retry` ms
end

# 3. Consume N tokens at once
Valvula.consume(:api_limiter, key: "user_123", tokens: 5)

# 4. Introspection
Valvula.status(:api_limiter, key: "user_123")
# => %{tokens: 87, max: 120, limited: false, ...}

Valvula.reset(:api_limiter, key: "user_123")
Valvula.stats(:api_limiter)
# => %{rate: 100, window_ms: 1000, ..., consumed_total: 137, rejected_total: 4}
```

## Algorithm

**Token bucket with lazy refill.** Each `key` (user, IP, API token)
gets a bucket that holds up to `max_tokens = rate + burst` tokens.
Every `window_ms`, `rate` new tokens are added — but not by a timer.

Instead, every `consume/2` call computes how many tokens would have been
generated since `last_refill` and tops up accordingly. This gives:

- **O(1) memory per bucket** (a fixed-size struct)
- **O(1) CPU per consume** (a couple of arithmetic ops + an ETS lookup)
- **No timer sprawl** — no per-bucket scheduler entries

```
   tokens
   ^
120|.................##########.........###  ← burst after idle
100|----------##########---------##########  ← steady-state = rate
 80|##########         ##########
 60|##########         ##########
 40|##########         ##########
 20|##########         ##########
  0+--------------------------------------> t
   ^                                     ^
   bucket starts full              next consume
   (after init/reset)              tops up lazily
```

## Use cases

- **API rate limiting** — `key: api_token` or `key: {tenant, route}`
- **Login throttling** — `key: email_or_ip` with `rate: 5, window: 1min`
- **External service quota** — `key: :global` for shared per-service caps
- **Webhook delivery backoff** — `key: webhook_url` with burst

### Combine with Arrea (Lorenzo-SF's parallel executor)

```elixir
{:ok, _} = Valvula.start_link(name: :external_api, rate: 10, window: :timer.seconds(1))

Arrea.run(tasks, fn task ->
  case Valvula.consume(:external_api, key: :global) do
    :ok ->
      process(task)

    {:error, :rate_limited, wait_ms} ->
      Process.sleep(wait_ms)
      process(task)
  end
end)
```

## Comparison with Hammer

| Feature | Valvula | Hammer |
|---------|---------|--------|
| Backend | ETS (built-in) | ETS / Redis |
| Algorithm | Token Bucket | Sliding Window |
| Burst support | ✓ (`:burst` option) | ✗ |
| Per-key stats | ✓ | ✗ |
| OTP-native supervisor child | ✓ (`child_spec/1`) | partial |
| External dependencies | **0** | 1+ |

## Installation

```elixir
def deps do
  [
    {:valvula, "~> 0.1"}
  ]
end
```

## Documentation

- `README.md` — this file (English)
- `docs/README.es.md` — Spanish version

## License

MIT — see [LICENSE.md](LICENSE.md).

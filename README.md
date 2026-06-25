# Valvula

> Token-bucket rate limiter for Elixir with ETS backend and OTP-native GenServer.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.md)
[![Elixir](https://img.shields.io/badge/elixir-~%201.14-purple.svg)](mix.exs)

Valvula is a **generic rate-limiting library**, not a Plug or a Phoenix
component. You wire it into whatever calls your service — an HTTP
controller, a Plug, a background job, a GenServer, or a raw `Req.post/1`
to a third-party API.

It gives you a per-key token bucket with **zero external dependencies**
— no Redis, no Postgres, no Mnesia. Just `:ets` and `:timer` from stdlib.

---

## When to reach for Valvula

Valvula fits whenever you have a **producer-consumer** relationship where
the producer can outrun the consumer and you need to slow it down:

| Scenario | Why rate-limit? | Key dimension |
|----------|-----------------|---------------|
| Your **Phoenix API** is being called by users | protect your DB / CPU | `user_id` or IP |
| Your **API is being scraped** by one client | prevent one user starving others | `api_key`, IP |
| You call **OpenAI / Stripe / external APIs** | they enforce quotas and will 429 you | `:global`, or per tenant |
| A **Oban / Queue worker** is sending emails | SMTP provider throttles | recipient domain |
| A **Plug** protects `/login` | brute-force protection | `email` or IP |
| A **LiveView** receives many events | prevent UI flooding | `user_id` or socket id |
| A **GenServer pipeline** processes batches | backpressure | source stream |
| **Webhooks fire** from external service | deliver at controlled rate | webhook URL |

If your producer runs **inside your BEAM** and the consumer is anything
(microseconds or minutes away), Valvula fits.

If your producer is in another node/cluster, you need a distributed
rate limiter (Redis, or ETS replicated) — Valvula is **single-node only**.

---

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

# 3. Consume N tokens at once (e.g. OpenAI TPM quotas)
Valvula.consume(:api_limiter, key: "user_123", tokens: 5)

# 4. Introspection
Valvula.status(:api_limiter, key: "user_123")
# => %{tokens: 87, max: 120, limited: false, ...}

Valvula.reset(:api_limiter, key: "user_123")    # admin "unblock this user"
Valvula.stats(:api_limiter)
# => %{rate: 100, window_ms: 1000, ..., consumed_total: 137, rejected_total: 4}
```

---

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

---

## Use cases in depth

### 1. HTTP rate-limit (Plug in Phoenix)

The most common case. **Valvula is not a Plug** — you write the Plug
yourself (it's 10 lines) and call Valvula inside `call/2`.

```elixir
defmodule MyAppWeb.Plugs.RateLimit do
  import Plug.Conn
  require Logger

  def init(opts), do: opts

  def call(conn, _opts) do
    key = rate_limit_key(conn)

    case Valvula.consume(:http_limiter, key: key) do
      :ok ->
        conn

      {:error, :rate_limited, retry_ms} ->
        Logger.warning("Rate limit hit for #{inspect(key)}")

        conn
        |> put_resp_header("retry-after", Integer.to_string(div(retry_ms, 1000)))
        |> put_resp_content_type("application/json")
        |> send_resp(429, Jason.encode!(%{
          error: "rate_limited",
          retry_after_ms: retry_ms
        }))
        |> halt()
    end
  end

  defp rate_limit_key(conn) do
    case conn.assigns[:current_user] do
      %{id: id, tier: :premium} -> {:premium, id}
      %{id: id} -> id
      _ -> conn.remote_ip |> :inet.ntoa() |> to_string()
    end
  end
end
```

Mount in your router:

```elixir
pipeline :api do
  plug :accepts, ["json"]
  plug MyAppWeb.Plugs.Auth
  plug MyAppWeb.Plugs.RateLimit     # ← after auth so we have current_user
end
```

### 2. Protect external API calls (OpenAI, Stripe, etc.)

External services enforce quotas. If your code talks to OpenAI directly,
you'll get 429 errors. Wrap the call:

```elixir
defmodule MyApp.OpenAIClient do
  def chat(messages, opts \\ []) do
    tenant = opts[:tenant] || :default

    case Valvula.consume(:openai_rpm, key: tenant) do
      :ok ->
        Req.post!("https://api.openai.com/v1/chat/completions",
                  json: %{messages: messages, model: "gpt-4o"})

      {:error, :rate_limited, retry_ms} ->
        {:error, :openai_throttled, retry_ms}
    end
  end

  # For TPM (tokens-per-minute) quotas, consume N tokens at once:
  def chat_with_estimate(messages, opts) do
    estimate = estimate_tokens(messages)
    tenant = opts[:tenant] || :default

    case Valvula.consume(:openai_tpm, key: tenant, tokens: estimate) do
      :ok -> Req.post!(...)
      {:error, :rate_limited, _} -> {:error, :quota_exceeded}
    end
  end
end
```

### 3. Background jobs (Oban / Queue)

```elixir
defmodule MyApp.Workers.EmailSender do
  use Oban.Worker

  def perform(%Job{args: %{"to" => to}}) do
    domain = to |> String.split("@") |> List.last()

    case Valvula.consume(:smtp, key: domain) do
      :ok ->
        MyApp.Mailer.send(to)
        :ok

      {:error, :rate_limited, retry_ms} ->
        # Re-enqueue with backoff
        __MODULE__.new(%{"to" => to}, schedule_in: retry_ms)
        :ok
    end
  end
end
```

### 4. LiveView events (rate-limit user clicks)

```elixir
defmodule MyAppWeb.CounterLive do
  use MyAppWeb, :live_view

  def handle_event("increment", _params, socket) do
    user_id = socket.assigns.current_user.id

    case Valvula.consume(:counter_clicks, key: user_id) do
      :ok ->
        # process click
        {:noreply, assign(socket, :count, socket.assigns.count + 1)}

      {:error, :rate_limited, _} ->
        # silently drop — too fast for human anyway
        {:noreply, socket}
    end
  end
end
```

### 5. GenServer / pipeline backpressure

```elixir
defmodule MyApp.ETL.Worker do
  use GenServer

  def handle_call({:process, batch}, _from, state) do
    case Valvula.consume(:downstream_api, key: batch.source) do
      :ok ->
        MyApp.DownstreamAPI.send(batch)
        {:reply, :ok, state}

      {:error, :rate_limited, _} ->
        # backpressure: tell caller to slow down
        {:reply, :throttled, state}
    end
  end
end
```

### 6. Login brute-force protection

```elixir
# 5 attempts per email or IP per minute
defmodule MyAppWeb.Plugs.LoginThrottle do
  def call(conn, _params) do
    email = conn.params["email"]
    ip = conn.remote_ip |> :inet.ntoa() |> to_string()

    case Valvula.consume(:login, key: {:email, email}) do
      :ok ->
        case Valvula.consume(:login, key: {:ip, ip}) do
          :ok -> conn
          {:error, :rate_limited, _} -> too_many(conn)
        end

      {:error, :rate_limited, _} ->
        too_many(conn)
    end
  end

  defp too_many(conn) do
    conn
    |> put_flash(:error, "Too many login attempts. Try again in a minute.")
    |> redirect(to: "/login")
    |> halt()
  end
end
```

---

## Combining with Arrea

Valvula + Arrea is the canonical pair for fan-out with throttling.
Arrea runs tasks in parallel workers; Valvula gates each worker.

```elixir
{:ok, _} = Valvula.start_link(name: :external_api, rate: 10, window: :timer.seconds(1))

Arrea.run(tasks, fn task ->
  case Valvula.consume(:external_api, key: task.tenant) do
    :ok ->
      process(task)

    {:error, :rate_limited, wait_ms} ->
      Process.sleep(wait_ms)
      process(task)
  end
end)
```

---

## Supervision tree integration

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      # Each Valvula is a separate GenServer with its own ETS table
      Valvula.child_spec(name: :http_limiter,    rate: 100,  window: :timer.seconds(1)),
      Valvula.child_spec(name: :user_limiter,    rate: 60,   window: :timer.minutes(1), burst: 10),
      Valvula.child_spec(name: :openai_rpm,      rate: 60,   window: :timer.minutes(1)),
      Valvula.child_spec(name: :openai_tpm,      rate: 60_000, window: :timer.minutes(1)),
      Valvula.child_spec(name: :login,           rate: 5,    window: :timer.minutes(1)),
      Valvula.child_spec(name: :smtp,            rate: 100,  window: :timer.minutes(1)),

      # Other workers, endpoint, etc.
      MyAppWeb.Endpoint
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
  end
end
```

Each Valvula is `restart: :permanent`, so the supervisor restarts it on
crash (the ETS table is recreated with the same configuration).

---

## Choosing keys — what to use as the bucket dimension

The key is what makes your rate-limit "per-something". Pick the right one:

| Goal | Key |
|------|-----|
| Fair across all users | `:global` |
| Per-user quota | `user_id` |
| Per-user with tier | `{user_id, tier}` or `{:premium, user_id}` |
| Anti-abuse on a public endpoint | `remote_ip` |
| Per-IP AND per-user | call `consume` twice (composes) |
| Per-API-token | `api_key_hash` (hash it, don't store raw!) |
| Per-tenant (multi-tenant app) | `tenant_id` |
| Per-route | `{user_id, route}` |
| Per-external-service | `{:openai, tenant_id}` |

**Composability**: call `consume` multiple times for layered limits:

```elixir
def call(conn, _opts) do
  ip = get_ip(conn)
  user_id = conn.assigns.current_user.id

  with :ok <- Valvula.consume(:per_ip, key: ip),
       :ok <- Valvula.consume(:per_user, key: user_id) do
    conn
  else
    {:error, :rate_limited, _} = e -> throttle(conn, e)
  end
end
```

This gives you both "max 100/min per IP" and "max 1000/min per user".

---

## Choosing rate, window, burst

| Use case | rate | window | burst | Rationale |
|----------|------|--------|-------|-----------|
| Public API | 60 | 1min | 10 | Generous, anti-bot |
| Login attempts | 5 | 1min | 0 | Brute-force protection, no burst |
| OpenAI tier 1 | 60 | 1min | 0 | Match their RPM limit |
| OpenAI tier 4 | 10000 | 1min | 0 | Match their RPM limit |
| Internal cron / backpressure | 1000 | 1s | 500 | High throughput, smoothing |
| WebSocket message rate | 10 | 1s | 2 | Allow bursts of typing, not floods |

`burst` lets short spikes pass (the bucket fills after idle periods). Use
`burst: 0` for strict limits (e.g. login attempts, API quotas).

---

## Comparison with Hammer

| Feature | Valvula | Hammer |
|---------|---------|--------|
| Backend | ETS (built-in) | ETS / Redis |
| Algorithm | Token Bucket | Sliding Window |
| Burst support | ✓ (`:burst` option) | ✗ |
| Per-key stats | ✓ | ✗ |
| OTP-native supervisor child | ✓ (`child_spec/1`) | partial |
| External dependencies | **0** | 1+ |
| Distributed (multi-node) | ✗ | ✓ (with Redis) |

Pick Valvula when: single-node, want zero deps, want burst semantics.
Pick Hammer when: distributed setup, or already using Redis.

---

## When NOT to use Valvula

- **Multi-node rate limiting** — ETS is local. Use Redis-backed Hammer or
  a distributed token bucket (e.g. with `Phoenix.PubSub` + a CRDT).
- **Sliding-window accuracy matters** — Valvula uses token bucket (slight
  over-allow at window boundaries). Hammer or a custom sliding window if
  precise.
- **You only need a global rate limit** — for a single shared bucket,
  `:gen_event` with `:timer.send_interval` is fine; you don't need
  per-key buckets.

---

## Installation

```elixir
def deps do
  [
    {:valvula, "~> 0.2"}
  ]
end
```

## Documentation

- `README.md` — this file (English)
- `docs/README.es.md` — Spanish version

## License

MIT — see [LICENSE.md](LICENSE.md).
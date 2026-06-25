# Valvula

> Rate limiter token-bucket para Elixir con backend ETS y GenServer OTP nativo.

[![Licencia](https://img.shields.io/badge/licencia-MIT-blue.svg)](LICENSE.md)
[![Elixir](https://img.shields.io/badge/elixir-~%201.14-purple.svg)](mix.exs)

Valvula es una **librería genérica de rate-limiting**, no un Plug ni un
componente de Phoenix. La enchufás donde sea que produzcas trabajo que
puede saturar al consumidor — un controller HTTP, un Plug, un job en
background, un GenServer, o una llamada directa a una API externa.

Te da un token bucket por clave con **cero dependencias externas** —
sin Redis, sin Postgres, sin Mnesia. Solo `:ets` y `:timer` de la stdlib.

---

## Cuándo usar Valvula

Valvula encaja cuando tenés una relación **productor-consumidor** donde
el productor puede correr más rápido que el consumidor y necesitás frenarlo:

| Escenario | Por qué rate-limitar | Dimensión de key |
|-----------|----------------------|------------------|
| Tu **API Phoenix** recibe muchos requests | proteger DB / CPU | `user_id` o IP |
| Tu API está siendo **scrappeada** por un cliente | evitar que un usuario monopolic | `api_key`, IP |
| Llamás a **OpenAI / Stripe / APIs externas** | ellos imponen cuotas y te van a 429 | `:global`, o por tenant |
| Un **Oban / Queue worker** manda emails | SMTP provider tiene rate limit | dominio del destinatario |
| Un **Plug** protege `/login` | anti brute-force | `email` o IP |
| Un **LiveView** recibe muchos eventos | evitar flood de UI | `user_id` o socket id |
| Un **GenServer pipeline** procesa batches | backpressure | stream de origen |
| **Webhooks entrantes** desde servicio externo | entregar a velocidad controlada | URL del webhook |

Si tu productor corre **dentro de tu BEAM** y el consumidor es cualquier
cosa (microsegundos o minutos), Valvula encaja.

Si tu productor está en otro nodo/cluster, necesitás un rate limiter
distribuido (Redis, o ETS replicada) — Valvula es **single-node**.

---

## Inicio rápido

```elixir
# 1. Arrancar un rate limiter
{:ok, _} = Valvula.start_link(
  name: :api_limiter,
  rate: 100,                       # 100 tokens por ventana
  window: :timer.seconds(1),       # ventana = 1 segundo
  burst: 20                        # +20 de burst → máximo 120 tokens
)

# 2. Consumir un token (sync, recomendado)
case Valvula.consume(:api_limiter, key: "user_123") do
  :ok                              # → proceder
  {:error, :rate_limited, retry}   # → rechazar / esperar `retry` ms
end

# 3. Consumir N tokens de una
Valvula.consume(:api_limiter, key: "user_123", tokens: 5)

# 4. Introspección
Valvula.status(:api_limiter, key: "user_123")
# => %{tokens: 87, max: 120, limited: false, ...}

Valvula.reset(:api_limiter, key: "user_123")
Valvula.stats(:api_limiter)
# => %{rate: 100, window_ms: 1000, ..., consumed_total: 137, rejected_total: 4}
```

---

## Algoritmo

**Token bucket con refill lazy.** Cada `key` (usuario, IP, API token) tiene
un bucket que admite hasta `max_tokens = rate + burst` tokens. Cada
`window_ms`, se agregan `rate` tokens nuevos — pero no por un timer.

En su lugar, cada llamada a `consume/2` calcula cuántos tokens se habrían
generado desde `last_refill` y repone. Esto da:

- **O(1) memoria por bucket** (struct de tamaño fijo)
- **O(1) CPU por consume** (un par de operaciones + lookup ETS)
- **Sin proliferación de timers** — sin entradas de scheduler por bucket

```
   tokens
   ^
120|.................##########.........###  ← burst tras idle
100|----------##########---------##########  ← estado estable = rate
 80|##########         ##########
 60|##########         ##########
 40|##########         ##########
 20|##########         ##########
  0+--------------------------------------> t
   ^                                     ^
   bucket arranca lleno           próximo consume
   (tras init/reset)              repone lazy
```

---

## Casos de uso en detalle

### 1. Rate-limit HTTP (Plug en Phoenix)

El caso más común. **Valvula no es un Plug** — vos escribís el Plug
vos mismo (son ~10 líneas) y llamás a Valvula dentro de `call/2`.

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

Montalo en tu router:

```elixir
pipeline :api do
  plug :accepts, ["json"]
  plug MyAppWeb.Plugs.Auth
  plug MyAppWeb.Plugs.RateLimit     # ← después de auth para tener current_user
end
```

### 2. Proteger llamadas a APIs externas (OpenAI, Stripe, etc.)

Los servicios externos imponen cuotas. Si tu código habla directo con
OpenAI, vas a recibir 429. Envolvé la llamada:

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

  # Para cuotas TPM (tokens-por-minuto), consumí N tokens de una:
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
        # Re-encolar con backoff
        __MODULE__.new(%{"to" => to}, schedule_in: retry_ms)
        :ok
    end
  end
end
```

### 4. Eventos de LiveView (rate-limit clicks)

```elixir
defmodule MyAppWeb.CounterLive do
  use MyAppWeb, :live_view

  def handle_event("increment", _params, socket) do
    user_id = socket.assigns.current_user.id

    case Valvula.consume(:counter_clicks, key: user_id) do
      :ok ->
        {:noreply, assign(socket, :count, socket.assigns.count + 1)}

      {:error, :rate_limited, _} ->
        # descartar silenciosamente — demasiado rápido para humano igual
        {:noreply, socket}
    end
  end
end
```

### 5. Backpressure en GenServer / pipeline

```elixir
defmodule MyApp.ETL.Worker do
  use GenServer

  def handle_call({:process, batch}, _from, state) do
    case Valvula.consume(:downstream_api, key: batch.source) do
      :ok ->
        MyApp.DownstreamAPI.send(batch)
        {:reply, :ok, state}

      {:error, :rate_limited, _} ->
        {:reply, :throttled, state}
    end
  end
end
```

### 6. Anti brute-force en login

```elixir
# 5 intentos por email o IP por minuto
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
    |> put_flash(:error, "Demasiados intentos. Probá de nuevo en un minuto.")
    |> redirect(to: "/login")
    |> halt()
  end
end
```

---

## Combinar con Arrea

Valvula + Arrea es la dupla canónica para fan-out con throttling.
Arrea corre tasks en workers paralelos; Valvula los va soltando.

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

## Integración en el supervision tree

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      # Cada Valvula es un GenServer con su propia tabla ETS
      Valvula.child_spec(name: :http_limiter,    rate: 100,  window: :timer.seconds(1)),
      Valvula.child_spec(name: :user_limiter,    rate: 60,   window: :timer.minutes(1), burst: 10),
      Valvula.child_spec(name: :openai_rpm,      rate: 60,   window: :timer.minutes(1)),
      Valvula.child_spec(name: :openai_tpm,      rate: 60_000, window: :timer.minutes(1)),
      Valvula.child_spec(name: :login,           rate: 5,    window: :timer.minutes(1)),
      Valvula.child_spec(name: :smtp,            rate: 100,  window: :timer.minutes(1)),

      # Otros workers, endpoint, etc.
      MyAppWeb.Endpoint
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
  end
end
```

Cada Valvula es `restart: :permanent`, así que el supervisor la reinicia
si crashea (la tabla ETS se recrea con la misma configuración).

---

## Cómo elegir la key

La key define **"rate-limit por qué"**. Elegí bien:

| Objetivo | Key |
|----------|-----|
| Fairness entre todos los usuarios | `:global` |
| Cuota por usuario | `user_id` |
| Cuota por usuario con tier | `{user_id, tier}` o `{:premium, user_id}` |
| Anti-abuso en endpoint público | `remote_ip` |
| Por IP y por usuario a la vez | llamá `consume` dos veces (se compone) |
| Por API token | `api_key_hash` (hasheala, no guardes el raw) |
| Multi-tenant | `tenant_id` |
| Por ruta | `{user_id, route}` |
| Por servicio externo | `{:openai, tenant_id}` |

**Componibilidad**: llamá `consume` varias veces para límites en capas:

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

Esto te da "máx 100/min por IP" **y** "máx 1000/min por user" en simultáneo.

---

## Cómo elegir rate, window, burst

| Caso | rate | window | burst | Razón |
|------|------|--------|-------|-------|
| API pública | 60 | 1min | 10 | Generoso, anti-bot |
| Intentos de login | 5 | 1min | 0 | Brute-force, sin burst |
| OpenAI tier 1 | 60 | 1min | 0 | Match su RPM |
| OpenAI tier 4 | 10000 | 1min | 0 | Match su RPM |
| Cron interno / backpressure | 1000 | 1s | 500 | Throughput alto, smoothing |
| Mensajes WebSocket | 10 | 1s | 2 | Permitir ráfagas de tipeo, no floods |

`burst` permite picos cortos (el bucket se llena tras idle). Usá
`burst: 0` para límites estrictos (login attempts, cuotas de API).

---

## Comparación con Hammer

| Característica | Valvula | Hammer |
|----------|---------|--------|
| Backend | ETS (built-in) | ETS / Redis |
| Algoritmo | Token Bucket | Sliding Window |
| Soporte de burst | ✓ (opción `:burst`) | ✗ |
| Stats por clave | ✓ | ✗ |
| Hijo de supervisor OTP nativo | ✓ (`child_spec/1`) | parcial |
| Dependencias externas | **0** | 1+ |
| Distribuido (multi-nodo) | ✗ | ✓ (con Redis) |

Elegí Valvula cuando: single-node, querés cero deps, querés burst.
Elegí Hammer cuando: setup distribuido, o ya usás Redis.

---

## Cuándo NO usar Valvula

- **Rate limiting multi-nodo** — ETS es local. Usá Hammer con Redis, o
  un token bucket distribuido.
- **Sliding-window exacto** — Valvula usa token bucket (pequeño
  over-allow en bordes de ventana). Si necesitás precisión, sliding window
  custom.
- **Solo necesitás un rate limit global** — para un único bucket
  compartido, `:gen_event` con `:timer.send_interval` alcanza; no necesitás
  per-key buckets.

---

## Instalación

```elixir
def deps do
  [
    {:valvula, "~> 0.2"}
  ]
end
```

## Licencia

MIT — ver [LICENSE.md](LICENSE.md).
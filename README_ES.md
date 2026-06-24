# Valvula

> Rate limiter token-bucket para Elixir con backend ETS y GenServer OTP nativo.

[![Licencia](https://img.shields.io/badge/licencia-MIT-blue.svg)](LICENSE.md)
[![Elixir](https://img.shields.io/badge/elixir-~%201.14-purple.svg)](mix.exs)

Valvula te da un rate limiter por clave con algoritmo token-bucket y
**cero dependencias externas** — sin Redis, sin Postgres, sin Mnesia. Solo
`:ets` y `:timer` de la stdlib.

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

## Casos de uso

- **Rate limiting de API** — `key: api_token` o `key: {tenant, route}`
- **Throttling de login** — `key: email_or_ip` con `rate: 5, window: 1min`
- **Cuota de servicio externo** — `key: :global` para límites compartidos
- **Backoff de webhooks** — `key: webhook_url` con burst

### Combinar con Arrea (ejecutor paralelo de Lorenzo-SF)

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

## Comparación con Hammer

| Característica | Valvula | Hammer |
|----------|---------|--------|
| Backend | ETS (built-in) | ETS / Redis |
| Algoritmo | Token Bucket | Sliding Window |
| Soporte de burst | ✓ (opción `:burst`) | ✗ |
| Stats por clave | ✓ | ✗ |
| Hijo de supervisor OTP nativo | ✓ (`child_spec/1`) | parcial |
| Dependencias externas | **0** | 1+ |

## Instalación

```elixir
def deps do
  [
    {:valvula, "~> 0.1"}
  ]
end
```

## Licencia

MIT — ver [LICENSE.md](LICENSE.md).

# Changelog

All notable changes to Valvula are documented in this file.

## [0.1.0] — 2026-06-24

### Added

- `Valvula.start_link/1` — start a new rate limiter GenServer.
- `Valvula.consume/2` — atomic token consume with `tokens: N` option.
  Returns `:ok` or `{:error, :rate_limited, retry_after_ms}`.
- `Valvula.reset/2` — restore a bucket to full.
- `Valvula.status/2` — inspect a bucket's current state.
- `Valvula.stats/1` — aggregate counters across all keys.
- `Valvula.lookup/2` — direct ETS read (bypass GenServer).
- `Valvula.Bucket` — struct + lazy-refill math.
- `Valvula.Server` — per-limiter GenServer + ETS owner.
- Lazy refill algorithm — no per-bucket timer, O(1) memory and CPU.
- 22 tests covering consume / reset / status / stats / lookup / config
  validation / Bucket math.
- README + README_ES + CHANGELOG + MIT LICENSE.
- CI workflow (lint + test + dialyzer).

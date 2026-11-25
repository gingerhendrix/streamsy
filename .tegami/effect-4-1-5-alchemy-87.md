---
packages:
  "group:streamsy": minor
---

## Require Effect 4.1.5 and Alchemy beta.87

**Breaking change:** Streamsy now requires `effect` and any `@effect/*` peers at `^4.1.5`. Upgrade the Effect family together.

The optional Alchemy integration requires exactly `alchemy@2.0.0-beta.87` and `@alchemy.run/cloudflare@2.0.0-beta.87`. Install both when using `@streamsy/serve/alchemy`.

On Node, `Node.listener` leaves the answer before the routes serve and after they stop to Effect's `NodeHttpServer`. It stays 503 with the body `service unavailable` and `Retry-After: 1`. It now has `Cache-Control: no-store` and keeps the connection open. With Effect 4.1.5 and Streamsy 0.14.0, such a request got no answer. Bun and Deno answer 503 instead of 404 in the same gap. See [the Node listener](https://streamsy.dev/docs/reference/serve/node).

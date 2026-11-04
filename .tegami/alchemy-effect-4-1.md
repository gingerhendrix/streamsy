---
packages:
  "group:streamsy": minor
---

## Require Effect 4.1.2 and Alchemy beta.85

**Breaking change:** Streamsy now requires `effect` and any `@effect/*` peers at `^4.1.2`. Upgrade the Effect family together. Effect 4.0.x is no longer supported.

The optional Alchemy integration requires exactly `alchemy@2.0.0-beta.85` and `@alchemy.run/cloudflare@2.0.0-beta.85`. Install both when using `@streamsy/serve/alchemy`, and replace imports from `alchemy/Cloudflare` with `@alchemy.run/cloudflare`.

Examples and tests use Effect's `SecureRandom` and `Digest` services with explicit crypto layers. Content hashes remain SHA-256 with the same bytes and lowercase hex encoding.

`expiresAt` and `Stream-Expires-At` follow Effect 4.1 `DateTime.make`, which also accepts a trailing lowercase `z` or `gmt`, such as `2028-01-01T00:00:00z`.

---
packages:
  "group:streamsy": minor
---

## Streamsy 0.4: a new library, written in Effect

Streamsy 0.4 is a complete rewrite in [Effect](https://effect.website) 4. It shares only the name and the Durable Streams protocol with the 0.2 releases. No code or API carries over, so treat it as a new library and not as an upgrade.

Four packages ship together and share one version:

- `@streamsy/core` has the protocol, typed stream refs, stream services, a memory Layer, the HTTP app and a remote client.
- `@streamsy/storage` is new. It stores streams in SQLite on Bun and in Durable Objects.
- `@streamsy/projection` is new. It runs checkpointed projections with named stream, rows and value outputs.
- `@streamsy/serve` is new. It has HttpRouter routes, the Bun and Cloudflare hosts and action delivery.

`effect` is now a peer dependency (`^4.0.5`), so install it beside Streamsy:

```sh
bun add @streamsy/core effect
```

The guides and API reference are at [streamsy.dev](https://streamsy.dev).

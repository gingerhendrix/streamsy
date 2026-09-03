# @streamsy/storage

Storage adapters for [Streamsy](https://github.com/gingerhendrix/streamsy) durable streams. Each backend is a separate subpath; there is no root export.

| Subpath                            | Adapter                                                           | Runtime                     |
| ---------------------------------- | ----------------------------------------------------------------- | --------------------------- |
| `@streamsy/storage/fs`             | `createFsStorageAdapter`                                          | Node/Bun filesystem (JSONL) |
| `@streamsy/storage/sqlite`         | `createSqliteStorageAdapter`                                      | Bun `bun:sqlite`            |
| `@streamsy/storage/durable-object` | `createDurableObjectStorageAdapter`, `DurableObjectStreamStorage` | Cloudflare Workers          |

```ts
import { createStreamProtocol } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage/sqlite";

const protocol = createStreamProtocol({
  storage: { adapter: createSqliteStorageAdapter({ path: "streams.db" }) },
});
```

## Module rules

Each backend owns one folder — `src/fs`, `src/sqlite`, `src/durable-object` — and the folder's `adapter.ts` is its public module. The Durable Object backend has a second one, `storage.ts`: it holds the `DurableObjectStreamStorage` class, which imports `cloudflare:workers` at runtime, so it is kept out of the adapter entry that Node-hosted callers and tests import. Everything else in the folder (codecs, locks, notifiers, schedulers, row stores, the schema migrator, the in-memory Durable Object namespace used by tests) is internal and reached by relative import. A backend folder never imports another backend folder; shared behaviour belongs in `@streamsy/core`.

Adapters are verified against the shared conformance kit from `@streamsy/core/testing`.

## License

MIT

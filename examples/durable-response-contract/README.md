# Durable response contract

Private shared contract for `durable-response`, `durable-response-atom`, and `durable-response-foldkit`.

- `entry-schema.ts`: native Effect AI parts, stop finish metadata, and JSON encoding.
- `api.ts`: input/reply schemas, route and response-page helpers.
- `fold.ts`, `read.ts`: response fold, batch validation, and scoped Durable Streams reader.
- `resume.ts`: atomic session checkpoint and per-tab identity.
- `client.ts`: Effect HTTP operations used by both browser stacks.

Run `bun run test`, `bun run typecheck`, and `bun run build` here. The package exports TypeScript source; its build verifies the types. See [the host README](../durable-response/README.md) for runnable commands, proof flows, and limitations.

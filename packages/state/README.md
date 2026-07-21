# @streamsy/state

Durable State protocol and stream wrappers for [Streamsy](https://github.com/gingerhendrix/streamsy) durable streams.

`DurableStateProtocol<S>`/`DurableStateStream<S>` wrap a `@streamsy/core` `StreamProtocolFactory` with a typed Durable State layer: schema-validated collections with standards-shaped change messages (`insert`/`update`/`delete`) and control messages (`snapshot-start`/`snapshot-end`/`reset`). The control methods append the wire vocabulary only; the package does not implement snapshot construction, retention/compaction, snapshot bootstrap, or a browser database adapter.

## Usage

```ts
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import { createDurableStateProtocol } from "@streamsy/state";
import type { JsonCodec } from "@streamsy/state";

type User = { id: string; name: string };

const userCodec: JsonCodec<User> = {
  encode: (value) => value,
  decode: (value) => value as User,
};

const protocol = createStreamProtocol({
  storage: { adapter: createMemoryStorageAdapter() },
});
const durable = createDurableStateProtocol(protocol, {
  users: { type: "user", schema: userCodec, primaryKey: "id" },
});

const created = await durable.create("state");
if (created.status === "created") {
  await created.stream.state.insert("users", { id: "u1", name: "Alice" });
  await created.stream.state.update("users", { id: "u1", name: "Alicia" });
  await created.stream.state.delete("users", "u1");
}
```

Schemas accept a `JsonCodec` or a [Standard Schema](https://standardschema.dev/) validator (via `@streamsy/json`); `primaryKey` may be a field name or a function.

## Exports

- `createDurableStateProtocol`, `DurableStateProtocol`, `DurableStateStream`
- types: `DurableStateCollectionDef`, `DurableStateMessage`, `ChangeMessage`, `ControlMessage`, and the typed create/get/read result and option types
- re-exports `JsonCodec` and `JsonSchema` from `@streamsy/json`

## License

MIT

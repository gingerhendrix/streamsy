# @streamsy/json

Typed JSON protocol and stream wrappers for [Streamsy](https://github.com/gingerhendrix/streamsy) durable streams.

`JsonProtocol<T>`/`JsonStream<T>` wrap a `@streamsy/core` `StreamProtocolFactory` so that stream messages are encoded and decoded as `application/json` values through a `JsonCodec<T>` or a [Standard Schema](https://standardschema.dev/) validator.

## Usage

```ts
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";

type User = { id: string; name: string };

const userCodec: JsonCodec<User> = {
  encode: (value) => value,
  decode: (value) => value as User,
};

const protocol = createStreamProtocol({
  storage: { adapter: createMemoryStorageAdapter() },
});
const json = createJsonProtocol(protocol, userCodec);

const created = await json.create("users", {
  initialMessage: { id: "u1", name: "Alice" },
});
if (created.status === "created") {
  await created.stream.append({ id: "u2", name: "Bob" });
  const read = await created.stream.read();
  if (read.status === "ok") {
    read.messages.map((message) => message.value.name); // ["Alice", "Bob"]
  }
}
```

Values that fail codec or schema validation reject at append with `JsonValidationError`; stored messages that fail to decode or validate on read surface as an `invalid-json` read status.

## Exports

- `createJsonProtocol`, `JsonProtocol`, `JsonStream`
- `JsonValidationError`, `normalizeJsonCodec`, `JSON_CONTENT_TYPE`
- types: `JsonCodec`, `JsonSchema`, `JsonStoredMessage`, and the typed create/get/read/readLive result and option types

## License

MIT

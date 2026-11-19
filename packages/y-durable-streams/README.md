# @streamsy/y-durable-streams

A Streamsy fork of the `YjsProvider` client from
[`@durable-streams/y-durable-streams`](https://github.com/durable-streams/durable-streams/tree/main/packages/y-durable-streams)
0.2.9. Upstream development is paused, so Streamsy carries the fixes it needs here.

The provider speaks the upstream
[Yjs Durable Streams protocol](https://github.com/durable-streams/durable-streams/blob/main/packages/y-durable-streams/PROTOCOL.md)
unchanged. Any server that works with the upstream provider works with this one.

```ts
import { YjsProvider } from "@streamsy/y-durable-streams";
import * as Y from "yjs";

const doc = new Y.Doc();
const provider = new YjsProvider({
  doc,
  baseUrl: "https://example.com/v1/yjs/demo",
  docId: "room-1",
});
```

The options, events and methods are the upstream ones.

## Scope

The fork contains only the client entry point (`src/index.ts` and `src/yjs-provider.ts`).
The upstream Node `YjsServer` (`@durable-streams/y-durable-streams/server`) is not forked.

## Changes from upstream 0.2.9

The first commit imports the published 0.2.9 source unchanged. Later changes:

- The source is formatted with the repository's `oxfmt` settings.
- `destroy()` has an `override` modifier, required by the repository's `noImplicitOverride`.
- Fire-and-forget calls carry `void`, as the repository's `no-floating-promises` rule requires. Behaviour is unchanged.

## Known upstream limits

- Raw `fetch` calls for `PUT`, snapshot discovery and snapshot reads cast `headers` to
  `Record<string, string>`. A function-valued `HeadersRecord` entry is not resolved on
  those calls, although the `DurableStream` client resolves it on reads and appends.
- Upstream issue #413: edits can be lost across reconnects. Not addressed here.

## License

MIT. The upstream repository is MIT licensed and `LICENSE` is the file shipped in the
upstream package. The upstream `package.json` declares `Apache-2.0`, which disagrees with
its own `LICENSE` file and repository license.

## @streamsy/serve@0.5.0

### Durable State control messages

**Breaking change:** the item type of a State address now includes control messages. Code that reads `type`, `key` or `headers.operation` from a State item must narrow the item first.

A State stream can now hold the control messages of the Durable State protocol: `snapshot-start`, `snapshot-end` and `reset`. Before this release, `StreamAddress.state` failed to decode a stored control. `Streams.decode` then failed the whole batch with a `DecodeFault`, and a follow or projection on that address stopped.

- `StreamAddress.state` decodes and encodes controls. The items of a `StateAddress`, a `Streamsy.state` route and `Serve.state` have the new type `StreamAddress.CollectionsEvent<C>`. That type is `CollectionsChange<C> | StateControl`. It was `CollectionsChange<C>`.
- `StreamAddress.isControl` and `StreamAddress.isChange` narrow an item. `StreamAddress.reset()`, `StreamAddress.snapshotStart()` and `StreamAddress.snapshotEnd()` make a control. Each one takes an optional `{ offset }`.
- `StreamAddress.stateControl` is the codec for one control. `StreamAddress.stateChange` stays the codec for the changes of one collection.
- `DurableState.changes` and projection outputs accept `DurableState.Entry<C>`, which is a row change or a control. A control is written unchanged. With an `offset`, a control that has no offset gets that offset.
- `Serve.state` serves stored controls as they are. It still refuses the `x-streamsy-state-reset: snapshot` request header.

To migrate a reader that folds State items, skip controls or handle them:

```ts
if (StreamAddress.isControl(item)) {
  if (item.headers.control === "reset") rows.clear();
} else if ("value" in item) rows.set(item.key, item.value);
else rows.delete(item.key);
```

## @streamsy/serve@0.4.0

### Streamsy 0.4: a new library, written in Effect

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

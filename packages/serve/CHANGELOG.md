## @streamsy/serve@0.6.0

### Projection.follow returns a Stream

**Breaking change:** `Projection.follow` now returns a `Stream` of committed cycles. Before this release, it returned a scoped `Fiber` and needed `Scope`. `Progress` now has a state type parameter, and `Projection.run` returns the same `Progress`.

A follower in the same process, such as a page, a TUI or a test, had no signal for each commit. It could only join the fiber for the end, or poll the checkpoint store. Now each element of the stream tells it what the follower committed.

- `Projection.follow(bound, options)` gives `Stream<Progress<StateValue<S>, Inputs>, E | ProjectionFault, R>`. The stream has no `Scope` requirement and forks no fiber. To run the stream is to be the writer, so run one `follow` stream per key.
- `Progress<S, Inputs>` extends `Snapshot<S, Inputs>`. It has the `state` and `offsets` that the last pass of the cycle left, and `status`, `units` and `items`. Neither `run` nor `follow` loads the checkpoint again after the cycle. `status` is `caught-up`, `limit-reached` or `source-closed`.
- The stream emits after the first cycle, also with 0 units. After that, it emits after each cycle that commits a unit or reaches `limit`, and after the last cycle when every input is closed. Then it ends. A wake that finds no input emits nothing.
- The stream is pull based. The next cycle runs only when the consumer pulls, so a slow consumer gets one element for all the input that arrived meanwhile. Use `Stream.buffer({ capacity: 1, strategy: "sliding" })` to decouple a slow consumer.
- An invalid `limit` or `repairIntervalMs` fails the stream at its first pull. Before, the `yield*` of `follow` failed.
- `Projection.run` now gives the `state` that the last pass of its cycle left. `Projection.pass` keeps its own result type, `PassResult`, with the `progress` status and no state.
- One `Projection.follow` span covers the whole stream. Each `Projection.pass` and `Projection.hint` span of the follower is a child of it.
- The wake hints, the retry of `storage-failure` and the repair interval do not change.

To migrate a caller:

```ts
Effect.gen(function* () {
  // Run to the end, as Fiber.join did.
  yield* Stream.runDrain(Projection.follow(bound));

  // Run in the background of a scope, as follow did.
  yield* Projection.follow(bound).pipe(Stream.runDrain, Effect.forkScoped);

  // Wait for an offset without a second writer.
  yield* Projection.follow(bound).pipe(
    Stream.takeUntil(({ offsets }) => offsets.input === offset),
    Stream.runLast,
  );
});
```

### Wake streams, attached projections and object links

- `Streamsy.wake<Self>()(name, template, { params })` is a State preset with `wakeCollections` and the normal State service. `WakeRow` holds `{ stream, offset, closed? }`, keyed by source id. `DurableState.materialized` and `materialize` fold State events into collection maps.
- `Projection.wake({ id, params, input, output })` uses `mapAccumBatch` and `Commit.producer` to publish source tails, closes and removals. The producer creates a missing output on its own owner. An output on another owner must exist first: its owner creates it at start with the State service's `create`. HTTP access remains mount policy.
- **Behaviour change from 0.5.0:** a close without new items gives one unit with `slice.closed`. A gone input with a checkpoint gives one final unit with `slice.gone`, then forgets the binding by default. The last `Projection.run` result or `Projection.follow` element holds the state and offsets that this final unit committed, and `Projection.read` then gives the initial state. Custom Commits need `remove` for this outcome. Live history gaps remain faults. A removal between a pin and its settle still fails with `range-unreproducible`, and the pin stays; retain history until pending units settle. `Projection.follow` already ended on close in 0.5.0; use attachment or another run for a later removal.
- **Upgrade from 0.5.0:** the checkpoint now records which inputs are closed. A 0.5.0 checkpoint has no such record, so the first run after the upgrade gives one extra unit with no items and `slice.closed` for each input that was already closed. A handler that expects items in every unit sees this unit once.
- **Breaking change:** `Storage.committed` is a scoped `PubSub` subscription to the ids of each later local commit. The subscription exists when the effect returns. Custom storage adapters must add it, or they do not compile.
- `Projection.attach(projection, { limit, retry })` runs affected bindings on Bun or Node. `ProjectionWakeup.attach(projection, options)` is acquired in a Cloudflare object scope. Both share one runner and accept a retry `Schedule`. The default schedule has no end. A custom schedule with an end stops the attachment for all bindings of that projection when the schedule ends, until the process or object restarts. Pending bindings live in memory; a restart, such as a crash, a Cloudflare eviction or a deploy, can delay work until the next source commit. Remote process commits do not appear.
- `Streamsy.followWake` and `wakeActions` compare rows with a saved cursor map and return `WakeItem` actions: `Dirty`, `Removed`, `Reset` and `Snapshot`.
- `ObjectStreams.layer(group, { objects, placement, prefix })` routes stream services through object stubs. `ObjectFetch` returns an Effect with typed HTTP client errors, preserving caller context and cancellation. Alchemy's `objectFetch` adapts its binding.
- `ObjectLink.make({ group, prefix, placement, access })` declares the internal owner routes and caller client together. Register `link.routes` and host `links`. `link.client(objects)` takes a namespace or an `ObjectFetch` and returns one `LinkClient` shape for both hosts. `StreamsyObject` takes `links: (env) => [...]`; Alchemy `objectHandlers` takes `links: [link.client(objectFetch(binding))]`. Local storage cannot also provide a linked declaration.
- **Behaviour change from 0.5.0:** public Cloudflare and Alchemy routers return 404 for paths whose first segment starts with `__`. Internal links require a `/__` prefix. Apps using root stream ids starting with `__` must rename them or use another prefix.

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

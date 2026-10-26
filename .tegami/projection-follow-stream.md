---
packages:
  "group:streamsy": minor
---

## Projection.follow returns a Stream

**Breaking change:** `Projection.follow` now returns a `Stream` of committed cycles. Before this release, it returned a scoped `Fiber` and needed `Scope`. `Progress` now has a state type parameter, and `Projection.run` returns the same `Progress`.

A follower in the same process, such as a page, a TUI or a test, had no signal for each commit. It could only join the fiber for the end, or poll the checkpoint store. Now each element of the stream tells it what the follower committed.

- `Projection.follow(bound, options)` gives `Stream<Progress<StateValue<S>, Inputs>, E | ProjectionFault, R>`. The stream has no `Scope` requirement and forks no fiber. To run the stream is to be the writer, so run one `follow` stream per key.
- `Progress<S, Inputs>` extends `Snapshot<S, Inputs>`. It has the `state` and `offsets` that the last pass of the cycle left, and `status`, `units` and `items`. Neither `run` nor `follow` loads the checkpoint again after the cycle. `status` is `caught-up`, `limit-reached` or `source-closed`.
- The stream emits after the first cycle, also with 0 units. After that, it emits after each cycle that commits a unit or reaches `limit`, and after the last cycle when every input is closed. Then it ends. A wake that finds no input emits nothing.
- The stream is pull based. The next cycle runs only when the consumer pulls, so a slow consumer gets one element for all the input that arrived meanwhile. Use `Stream.buffer({ capacity: 1, strategy: "sliding" })` to decouple a slow consumer.
- An invalid `limit` or `repairIntervalMs` fails the stream at its first pull. Before, the `yield*` of `follow` failed.
- `Projection.run` now gives the `state` that the last pass of its cycle left. `Projection.pass` keeps its own result type, `PassResult`, with the `progress` status and no state.
- One `Projection.follow` span covers the whole stream. Each `Projection.pass` and `Projection.wake` span of the follower is a child of it.
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

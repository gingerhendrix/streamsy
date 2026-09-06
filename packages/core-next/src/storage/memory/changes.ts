import { Effect, Schedule, Stream } from "effect";
import { ZERO_OFFSET } from "../../offset/index.ts";
import type { ChangeSnapshot, StreamId } from "../../schema/index.ts";
import type { Notifier } from "./notifier.ts";
import type { State } from "./state.ts";

export function snapshot(state: State, id: StreamId): ChangeSnapshot {
  const record = state.entries.get(id)?.record;
  return {
    present: record !== undefined,
    currentOffset: record?.currentOffset ?? ZERO_OFFSET,
    closed: record?.lifecycle.closed ?? false,
    softDeleted: record?.lifecycle.softDeleted ?? false,
  };
}
export function changes(
  state: State,
  bus: Notifier,
  id: StreamId,
  push: boolean,
  interval: number,
): Stream.Stream<ChangeSnapshot> {
  const read = Effect.sync(() => snapshot(state, id));
  if (!push) return Stream.fromEffect(read).pipe(Stream.repeat(Schedule.spaced(interval)));
  return Stream.unwrap(
    Effect.gen(function* () {
      // Each subscription coalesces its own wakes, independently of slow peers.
      // Payload-free wakes always re-read this id, including after unrelated commits.
      // Subscribe before the first read: a commit in the acquisition window cannot be lost.
      const subscription = yield* bus.subscribe;
      return Stream.concat(
        Stream.fromEffect(read),
        Stream.fromQueue(subscription).pipe(Stream.mapEffect(() => read)),
      );
    }),
  );
}

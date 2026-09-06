import { Effect, PubSub, Schedule, Stream } from "effect";
import { ZERO_OFFSET } from "../../offset/index.ts";
import type { ChangeSnapshot, StreamId } from "../../schema/index.ts";
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
  bus: PubSub.PubSub<void>,
  id: StreamId,
  push: boolean,
  interval: number,
): Stream.Stream<ChangeSnapshot> {
  const read = Effect.sync(() => snapshot(state, id));
  if (!push) return Stream.fromEffect(read).pipe(Stream.repeat(Schedule.spaced(interval)));
  return Stream.unwrap(
    Effect.gen(function* () {
      // A single pending store wake coalesces all commits. Every wake re-reads
      // this id, so an unrelated commit cannot overwrite a relevant notification.
      // Subscribe before the first read: a commit in the acquisition window cannot be lost.
      const subscription = yield* PubSub.subscribe(bus);
      return Stream.concat(
        Stream.fromEffect(read),
        Stream.fromSubscription(subscription).pipe(Stream.mapEffect(() => read)),
      );
    }),
  );
}

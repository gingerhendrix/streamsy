import { compare, ZERO_OFFSET, type Offset } from "@streamsy/core";
import { Array, Effect, Order, PubSub, Record, Stream, type Exit, type Take } from "effect";
import type { CircuitError } from "./circuit.ts";
import type { JsonObject } from "./ir/contracts.ts";
import type { LiveSnapshot, Views } from "./live.ts";
import { deepFreeze } from "./relation.ts";
import { compareRowKeys, encodeRowKey } from "./runtime/key.ts";
import type { OutputChange } from "./sql/engine.ts";

const snapshotKeyOrder = Order.make<string>((left, right) => {
  const order = compareRowKeys(left, right);

  return order < 0 ? -1 : order > 0 ? 1 : 0;
});

type Tails = Readonly<Record<string, { readonly offset: Offset }>>;

class OutputRelation {
  private readonly rows = new Map<string, JsonObject>();
  private cached: ReadonlyArray<JsonObject> | undefined = Object.freeze([]);

  set(key: string, row: JsonObject | undefined) {
    if (row === undefined) {
      if (!this.rows.delete(key)) return;
    } else this.rows.set(key, row);

    this.cached = undefined;
  }

  get array() {
    return (this.cached ??= Object.freeze(
      Array.map(Array.sort([...this.rows.keys()], snapshotKeyOrder), (key) => this.rows.get(key)!),
    ));
  }
}

export const makeLiveOutputs = <V extends Views>(
  names: readonly string[],
  inputs: readonly string[],
) =>
  Effect.gen(function* () {
    // Reserve room for a wake and the terminal exit, including for late subscribers.
    const signals = yield* PubSub.sliding<Take.Take<void, CircuitError>>({
      capacity: 2,
      replay: 2,
    });

    const relations = Record.fromEntries(
      Array.map(names, (name) => [name, new OutputRelation()] as const),
    );

    let offsets: Readonly<Record<string, Offset>> = Object.freeze(
      Record.fromEntries(Array.map(inputs, (key) => [key, ZERO_OFFSET] as const)),
    );

    let tails: Tails | undefined;
    let caughtUp = false;
    let current: LiveSnapshot<V> | undefined;

    const reachTails = () =>
      tails !== undefined &&
      Record.every(tails, (tail, key) => compare(offsets[key] ?? ZERO_OFFSET, tail.offset) >= 0);

    const signal = () => {
      current = undefined;

      // publishUnsafe drops the wake when one subscriber holds a full buffer. This slides
      // instead, and a sliding publish never suspends, so it can run in the commit callback.
      // oxlint-disable-next-line effecttsgo/run-effect-inside-effect -- The commit callback is synchronous, and the publish needs no services.
      Effect.runSync(PubSub.publish(signals, [undefined]));
    };

    const read = () =>
      (current ??= Object.freeze({
        // SAFETY: Each output contains the rows of the relation compiled from its declaration.
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The engine represents rows as JSON.
        rows: Object.freeze(
          Record.map(relations, (relation) => relation.array),
        ) as LiveSnapshot<V>["rows"],
        offsets,
        caughtUp,
      }));

    signal();

    return {
      stage: (changes: readonly OutputChange[], next: Record<string, Offset>) => {
        const staged = Array.map(changes, (change) => ({
          relation: relations[change.view]!,
          key: encodeRowKey(change.key),
          row: change.kind === "delete" ? undefined : deepFreeze(structuredClone(change.value)),
        }));

        return () => {
          for (const { relation, key, row } of staged) relation.set(key, row);

          offsets = Object.freeze(next);
          caughtUp ||= reachTails();
          signal();
        };
      },
      captureTails: (target: Tails) =>
        Effect.sync(() => {
          tails = target;

          if (reachTails()) {
            caughtUp = true;
            signal();
          }
        }),
      end: (exit: Exit.Exit<void, CircuitError>) => PubSub.publish(signals, exit),
      snapshot: Effect.sync(read),
      snapshots: Stream.fromPubSubTake(signals).pipe(
        Stream.map(read),
        Stream.changesWith(Object.is),
      ),
    };
  });

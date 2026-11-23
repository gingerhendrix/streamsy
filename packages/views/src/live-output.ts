import { Array, Cause, Deferred, Effect, Exit, Order, Queue, Stream } from "effect";
import type { CircuitError } from "./circuit.ts";
import type { JsonObject } from "./ir/contracts.ts";
import { deepFreeze } from "./relation.ts";
import { compareRowKeys, encodeRowKey } from "./runtime/key.ts";
import type { OutputChange } from "./sql/engine.ts";

export interface LiveSnapshot<Row> {
  readonly rows: ReadonlyArray<Row>;
  readonly caughtUp: boolean;
}

// SAFETY: The declaration chooses the row type of each compiled output relation.
// oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The engine represents all rows as JSON.
const typed = <Row>(snapshot: LiveSnapshot<JsonObject | Row>) => snapshot as LiveSnapshot<Row>;

const snapshotKeyOrder = Order.make<string>((left, right) => {
  // UTF-8 replacement makes lone surrogates compare equal to each other and U+FFFD.
  const order = compareRowKeys(left, right);

  return order < 0 ? -1 : order > 0 ? 1 : Order.String(left, right);
});

class OutputRelation {
  readonly rows = new Map<string, JsonObject>();
  readonly subscribers = new Set<Queue.Queue<void>>();
  revision = 0;
  private current: LiveSnapshot<JsonObject> | undefined;

  signal() {
    for (const subscriber of this.subscribers) Queue.offerUnsafe(subscriber, undefined);
  }

  changed() {
    this.revision++;
    this.current = undefined;
    this.signal();
  }

  snapshot(caughtUp: boolean): LiveSnapshot<JsonObject> {
    if (this.current?.caughtUp === caughtUp) return this.current;

    const rows =
      this.current?.rows ??
      Object.freeze(
        Array.map(Array.sort([...this.rows.keys()], snapshotKeyOrder), (key) =>
          this.rows.get(key)!,
        ),
      );

    return (this.current = Object.freeze({ rows, caughtUp }));
  }
}

export const makeLiveOutputs = (names: readonly string[]) =>
  Effect.gen(function* () {
    const relations = new Map(Array.map(names, (name) => [name, new OutputRelation()]));
    const ready = yield* Deferred.make<void, CircuitError>();
    let caughtUp = false;
    let terminal: Exit.Exit<void, CircuitError> | undefined;
    let closed = false;

    function signal() {
      for (const relation of relations.values()) relation.signal();
    }

    const stage = (changes: readonly OutputChange[]) => {
      const staged = Array.map(changes, (change) => ({
        relation: relations.get(change.view)!,
        key: encodeRowKey(change.key),
        row: change.kind === "delete" ? undefined : deepFreeze(structuredClone(change.value)),
      }));

      return () => {
        const changed = new Set<OutputRelation>();

        for (const { relation, key, row } of staged) {
          if (row === undefined) {
            if (!relation.rows.delete(key)) continue;
          } else relation.rows.set(key, row);

          changed.add(relation);
        }

        for (const relation of changed) relation.changed();
      };
    };

    const finishCatchUp = (exit: Exit.Exit<void, CircuitError>) =>
      Effect.sync(() => {
        // Deferred completion can resume a waiting caller immediately.
        if (Exit.isSuccess(exit)) {
          caughtUp = true;
          signal();
        }

        Deferred.doneUnsafe(ready, exit);
      });

    const finishFollow = (exit: Exit.Exit<void, CircuitError>) =>
      Effect.sync(() => {
        if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) return;
        terminal = exit;

        if (Exit.isFailure(exit)) Deferred.doneUnsafe(ready, exit);

        signal();
      });

    function close() {
      closed = true;

      for (const relation of relations.values()) {
        relation.rows.clear();
        relation.changed();
        relation.subscribers.clear();
      }

      Deferred.doneUnsafe(ready, Effect.interrupt);
    }

    const rows = <Row>(name: string) =>
      Effect.sync(() => typed<Row>(relations.get(name)!.snapshot(caughtUp)));

    const changes = <Row>(name: string) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const relation = relations.get(name)!;
          const queue = yield* Queue.sliding<void>(1);

          yield* Effect.acquireRelease(
            Effect.sync(() => {
              if (!closed) relation.subscribers.add(queue);
            }),
            () =>
              Effect.sync(() => {
                relation.subscribers.delete(queue);
              }),
          );

          return Stream.unfold<
            { readonly revision: number; readonly caughtUp: boolean } | undefined,
            LiveSnapshot<Row>,
            CircuitError,
            never
          >(undefined, (previous) =>
            Effect.gen(function* () {
              while (true) {
                if (closed) return undefined;

                if (terminal !== undefined && Exit.isFailure(terminal))
                  return yield* Effect.failCause(terminal.cause);

                const { revision } = relation;

                if (previous?.revision !== revision || previous.caughtUp !== caughtUp)
                  return [typed<Row>(relation.snapshot(caughtUp)), { revision, caughtUp }] as const;

                if (terminal !== undefined) return undefined;
                yield* Queue.take(queue);
              }
            }),
          );
        }),
      );

    return {
      stage,
      finishCatchUp,
      finishFollow,
      close,
      rows,
      changes,
      caughtUp: Effect.suspend(() =>
        terminal !== undefined && Exit.isFailure(terminal) ? terminal : Deferred.await(ready),
      ),
    };
  });

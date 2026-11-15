import { Array, Cause, Clock, Effect, Option, Schema, Semaphore } from "effect";
import {
  CommitBoundary,
  Offset,
  Storage,
  StorageFault,
  StreamAddress,
  StreamId,
  Streamsy,
  ZERO_OFFSET,
} from "@streamsy/core";
import { OwnerRecords, type ProjectionMarksApi } from "@streamsy/core/internal";
import { runnerFailure } from "./failure.ts";
import type { RunnerFailure } from "./registry.ts";
import { ProjectionFault } from "./fault.ts";
import { encodeKey } from "./unit.ts";
import { storageFault } from "./storage-fault.ts";

import type { WakeOutput } from "./wake.ts";

const DEFAULT_LIMIT = 1_000;

const decodeWakeMessage = Schema.decodeEffect(Schema.fromJsonString(Streamsy.WakeMessage));

const decoder = new TextDecoder();

/** Persistent ingestion shared by both hosts; no trigger service or remote reader is used. */
export const triggersOf = <B>(
  projection: string,
  declarations: ReadonlyArray<WakeOutput<{}>>,
  match: (id: StreamId) => Option.Option<readonly [string, B]>,
  marks: ProjectionMarksApi,
) =>
  Effect.gen(function* () {
    const storage = yield* Storage;
    const records = yield* Effect.serviceOption(OwnerRecords);
    const boundary = yield* Effect.serviceOption(CommitBoundary);

    if (Option.isNone(records) || Option.isNone(boundary))
      return yield* Effect.die(
        new Error(
          `Projection ${projection} cannot attach triggers: use a local memory or SQL storage layer.`,
        ),
      );

    const owner = records.value;
    const transaction = boundary.value;
    const permit = yield* Semaphore.make(1);
    const cursors = new Map<string, Offset>();
    const stopped = new Map<string, RunnerFailure>();

    const streams = new Map<StreamId, string>(
      Array.map(declarations, (declaration) => {
        const { id } = declaration.address({});

        return [id, encodeKey(["streamsy.projection.trigger.v1", projection, id])] as const;
      }),
    );

    const cursor = (key: string) =>
      Effect.gen(function* () {
        const cached = cursors.get(key);

        if (cached !== undefined) return cached;
        const saved = yield* owner.read(key);

        const from = Option.isNone(saved)
          ? Offset.make(ZERO_OFFSET)
          : yield* Schema.decodeEffect(Offset)(saved.value).pipe(
              Effect.mapError(
                (cause) =>
                  new StorageFault({
                    operation: "projection.trigger.cursor",
                    retryable: false,
                    message: `Invalid trigger cursor for ${projection}`,
                    cause,
                  }),
              ),
            );

        cursors.set(key, from);

        return from;
      });

    const ahead = (id: StreamId, key: string) =>
      permit.withPermit(
        Effect.gen(function* () {
          if (stopped.has(id)) return false;
          const record = yield* storage.record(id);

          return (
            Option.isSome(record) &&
            !record.value.lifecycle.softDeleted &&
            record.value.currentOffset > (yield* cursor(key))
          );
        }),
      );

    const next = Effect.gen(function* () {
      for (const [id, key] of streams) {
        // A cursor loaded by a deadline check must not overwrite a concurrent reset.
        if (yield* ahead(id, key)) return true;
      }

      return false;
    });

    const read = Effect.gen(function* () {
      for (const [id, key] of streams) {
        // Durable marks join this transaction. Private marks can survive a rollback,
        // leaving harmless work for a pass that finds nothing new.
        yield* permit.withPermit(
          Effect.suspend(() =>
            stopped.has(id)
              ? Effect.void
              : transaction
                  .withTransaction(
                    Effect.gen(function* () {
                      const record = yield* storage.record(id);
                      const found = new Set<string>();

                      if (Option.isNone(record) || record.value.lifecycle.softDeleted)
                        return { offset: undefined };
                      const from = yield* cursor(key);

                      const messages = yield* storage.messages(id, {
                        after: from,
                        until: record.value.currentOffset,
                        limit: DEFAULT_LIMIT,
                      });

                      for (const message of messages) {
                        const event = yield* decodeWakeMessage(decoder.decode(message.data)).pipe(
                          Effect.mapError(
                            (cause) =>
                              new ProjectionFault({
                                phase: "read",
                                reason: "invalid-source",
                                message: `Cannot decode trigger ${id} at offset ${message.offset}`,
                                cause,
                              }),
                          ),
                        );

                        // Deletes request cleanup too (Projection.wake emits them for removed sources).
                        if (!StreamAddress.isChange(event)) continue;

                        const binding = match(
                          StreamAddress.isUpsert(event)
                            ? event.value.stream
                            : StreamId.make(event.key),
                        );

                        if (Option.isSome(binding)) found.add(binding.value[0]);
                        else
                          yield* Effect.logWarning(
                            `Projection ${projection} skipped unmatched trigger source ${event.key} from ${id}`,
                          );
                      }

                      const last = messages.at(-1);

                      if (last === undefined) return { offset: from };

                      if (found.size > 0) yield* marks.mark(projection, [...found]);
                      yield* owner.write(key, last.offset);

                      return { offset: last.offset };
                    }),
                  )
                  .pipe(
                    Effect.catchTag("SqlError", (cause) =>
                      Effect.fail(storageFault("projection.triggers")(cause)),
                    ),
                    // Publish the cached position only after commit. On interruption or rollback,
                    // reload storage in case the transaction committed before it was interrupted.
                    Effect.tap(({ offset }) =>
                      Effect.sync(() => {
                        if (offset !== undefined) cursors.set(key, offset);
                      }),
                    ),
                    Effect.onError(() => Effect.sync(() => cursors.delete(key))),
                    Effect.catchTag("ProjectionFault", (fault) =>
                      Effect.gen(function* () {
                        const failure = runnerFailure(
                          Cause.fail(fault),
                          yield* Clock.currentTimeMillis,
                        );

                        stopped.set(id, failure);
                        yield* Effect.logError(
                          `Projection ${projection} trigger ${id} stopped`,
                          fault,
                        );
                      }),
                    ),
                  ),
          ),
        );
      }
    });

    const rescan = (id: string) =>
      permit.withPermit(
        Effect.gen(function* () {
          const key = streams.get(StreamId.make(id));

          if (key === undefined)
            return yield* Effect.die(
              new RangeError(`Projection ${projection} has no trigger ${id}`),
            );
          yield* transaction.withTransaction(owner.write(key, ZERO_OFFSET)).pipe(
            Effect.catchTag("SqlError", (cause) =>
              Effect.fail(storageFault("projection.triggers.rescan")(cause)),
            ),
            Effect.tap(() =>
              Effect.sync(() => {
                cursors.set(key, Offset.make(ZERO_OFFSET));
                stopped.delete(id);
              }),
            ),
            Effect.onError(() => Effect.sync(() => cursors.delete(key))),
          );

          return undefined;
        }),
      );

    return {
      next,
      read,
      rescan,
      retry: () => stopped.clear(),
      status: () =>
        Array.map([...streams.keys()], (stream) => ({
          stream,
          failure: Option.fromUndefinedOr(stopped.get(stream)),
        })),
    };
  });

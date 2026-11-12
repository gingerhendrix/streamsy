import { Array, Clock, Effect, Option, Schema } from "effect";
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
import { ProjectionFault } from "./fault.ts";
import { encodeKey } from "./unit.ts";

/** A wake declaration at one fixed address on the runner's storage owner. */
export type WakeClass = Pick<
  Streamsy.StateClass<unknown, string, string, {}, Streamsy.WakeCollections>,
  "address"
>;

/** Persistent ingestion shared by both hosts; no trigger service or remote reader is used. */
export const triggersOf = <B>(
  projection: string,
  declarations: ReadonlyArray<WakeClass>,
  match: (id: StreamId) => Option.Option<readonly [string, B]>,
  marks: ProjectionMarksApi | undefined,
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

    const streams = new Map(
      Array.map(declarations, (declaration) => {
        const { id } = declaration.address({});

        return [id, encodeKey(["streamsy.projection.trigger.v1", projection, id])] as const;
      }),
    );

    const cursor = (key: string) =>
      Effect.gen(function* () {
        const saved = yield* owner.read(key);

        if (Option.isNone(saved)) return Offset.make(ZERO_OFFSET);

        return yield* Schema.decodeEffect(Offset)(saved.value).pipe(
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
      });

    const next = Effect.gen(function* () {
      for (const [id, key] of streams) {
        const record = yield* storage.record(id);

        if (
          Option.isSome(record) &&
          !record.value.lifecycle.softDeleted &&
          record.value.currentOffset > (yield* cursor(key))
        )
          return true;
      }

      return false;
    });

    const read = Effect.gen(function* () {
      const ready = new Map<string, B>();

      for (const [id, key] of streams) {
        // Return bindings only after the marks and cursor have committed. An interruption
        // before enqueue loses only non-durable work, exactly like a process stop.
        const bindings = yield* transaction
          .withTransaction(
            Effect.gen(function* () {
              const record = yield* storage.record(id);
              const found = new Map<string, B>();

              if (Option.isNone(record) || record.value.lifecycle.softDeleted) return found;
              const from = yield* cursor(key);

              const messages = yield* storage.messages(id, {
                after: from,
                until: record.value.currentOffset,
                limit: 1_000,
              });

              for (const message of messages) {
                const event = yield* Schema.decodeEffect(
                  Schema.fromJsonString(Streamsy.WakeMessage),
                )(new TextDecoder().decode(message.data)).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProjectionFault({
                        phase: "read",
                        reason: "invalid-source",
                        message: `Cannot decode trigger ${id}`,
                        cause,
                      }),
                  ),
                );

                // Deletes request cleanup too (Projection.wake emits them for removed sources).
                if (!StreamAddress.isChange(event)) continue;

                const binding = match(
                  StreamAddress.isUpsert(event) ? event.value.stream : StreamId.make(event.key),
                );

                if (Option.isSome(binding)) found.set(...binding.value);
                else
                  yield* Effect.logWarning(
                    `Projection ${projection} skipped unmatched trigger source ${event.key} from ${id}`,
                  );
              }

              const last = messages.at(-1);

              if (last === undefined) return found;

              if (marks !== undefined && found.size > 0)
                yield* marks.mark(projection, [...found.keys()]);
              yield* owner.write(key, last.offset);

              if (marks !== undefined && found.size > 0) {
                const states = yield* marks.read(projection, [...found.keys()]);
                const now = yield* Clock.currentTimeMillis;

                // A new row keeps the binding's existing backoff, as mark() does.
                for (const [params, state] of states) if (state.dueAt > now) found.delete(params);
              }

              return found;
            }),
          )
          .pipe(
            Effect.catchTag("SqlError", (cause) =>
              Effect.fail(
                new StorageFault({
                  operation: "projection.triggers",
                  message: cause.message,
                  retryable: cause.isRetryable,
                  cause,
                }),
              ),
            ),
          );

        for (const [params, binding] of bindings) ready.set(params, binding);
      }

      return ready;
    });

    return { next, read };
  });

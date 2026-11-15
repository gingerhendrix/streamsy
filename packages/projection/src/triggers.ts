import {
  Array,
  Cause,
  Clock,
  Deferred,
  Effect,
  Option,
  Pull,
  Result,
  Schedule,
  Schema,
  Semaphore,
} from "effect";
import {
  CommitBoundary,
  Offset,
  Storage,
  StorageFault,
  StreamAddress,
  StreamId,
  Streamsy,
  type StreamsFault,
  ZERO_OFFSET,
} from "@streamsy/core";
import type { Reader } from "@streamsy/core/protocol";
import { OwnerRecords, ownerOf, type ProjectionMarksApi } from "@streamsy/core/internal";
import { runnerFailure } from "./failure.ts";
import type { RunnerFailure } from "./registry.ts";
import { ProjectionFault } from "./fault.ts";
import { encodeKey } from "./unit.ts";
import { storageFault } from "./storage-fault.ts";

import type { WakeOutput } from "./wake.ts";

const DEFAULT_LIMIT = 1_000;

const decodeWakeMessage = Schema.decodeEffect(Schema.fromJsonString(Streamsy.WakeMessage));

const decoder = new TextDecoder();

interface RemoteTrigger {
  readonly wire: Reader<StreamsFault>;
  ahead: boolean;
  changed: Deferred.Deferred<void>;
}

const invalid = (id: StreamId, message: string, cause: unknown) =>
  new ProjectionFault({
    phase: "read",
    reason: "invalid-source",
    message: `${message}: ${id}`,
    cause,
  });

/** Persistent local ingestion on both hosts; process hosts also ingest and follow other owners. */
export const triggersOf = <B>(
  projection: string,
  declarations: ReadonlyArray<WakeOutput<{}>>,
  match: (id: StreamId) => Option.Option<readonly [string, B]>,
  marks: ProjectionMarksApi,
  host: "process" | "wakeup" = "process",
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

    const streams = new Map<StreamId, string>();
    const remote = new Map<StreamId, RemoteTrigger>();

    // These services share one owner. Wrappers may drop its non-enumerable
    // metadata, so check all three; at least one must remain unwrapped.
    const isRemote = (service: Option.Option<{ readonly owner: object }>) =>
      Option.isSome(service) &&
      !Array.some([storage, owner, transaction], (local) => service.value.owner === ownerOf(local));

    for (const declaration of declarations) {
      const { id } = declaration.address({});
      const service = yield* Effect.serviceOption(declaration);

      if (Option.isSome(service) && isRemote(service)) {
        if (host === "wakeup")
          return yield* Effect.die(
            new RangeError(
              `Trigger ${id}: ProjectionWakeup.attach accepts only triggers on its own storage owner`,
            ),
          );
        remote.set(id, {
          wire: service.value.wire,
          ahead: true,
          changed: yield* Deferred.make<void>(),
        });
      }

      streams.set(id, encodeKey(["streamsy.projection.trigger.v1", projection, id]));
    }

    // A follower waits for a pass or reset after observing a page, rather than
    // repeatedly reading the same uncommitted page while a pass backs off.
    const changed = (state: RemoteTrigger) =>
      Effect.gen(function* () {
        const previous = state.changed;
        state.changed = yield* Deferred.make<void>();
        yield* Deferred.succeed(previous, undefined);
      });

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
          const state = remote.get(id);

          if (state !== undefined) return state.ahead;
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
        if (remote.has(id)) continue;
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

    let remoteFailed = false;

    const readRemote = Effect.gen(function* () {
      let first: StorageFault | undefined;

      for (const [id, state] of remote) {
        const key = streams.get(id)!;

        const attempt = yield* permit
          .withPermit(
            Effect.suspend(() =>
              stopped.has(id)
                ? Effect.void
                : Effect.gen(function* () {
                    const from = yield* cursor(key);

                    // Network I/O must finish before opening the local transaction.
                    const page = yield* state.wire.read(id, { offset: from }).pipe(
                      Effect.asSome,
                      Effect.catchTags({
                        StreamNotFound: () => Effect.succeedNone,
                        StreamGone: () => Effect.succeedNone,
                        InvalidReadRequest: (cause) =>
                          Effect.fail(invalid(id, "Invalid trigger read", cause)),
                        TransportFault: (cause) =>
                          Effect.fail(
                            new StorageFault({
                              operation: "projection.triggers.remote",
                              retryable: true,
                              message: `Cannot read remote trigger ${id}`,
                              cause,
                            }),
                          ),
                        StorageFault: (cause) =>
                          Effect.fail(
                            new StorageFault({
                              operation: "projection.triggers.remote",
                              retryable: true,
                              message: `Cannot read remote trigger ${id}`,
                              cause,
                            }),
                          ),
                      }),
                    );

                    if (Option.isNone(page)) {
                      state.ahead = false;

                      return;
                    }

                    const result = page.value;

                    const offset = yield* Schema.decodeEffect(Offset)(result.nextOffset).pipe(
                      Effect.mapError((cause) => invalid(id, "Invalid trigger page cursor", cause)),
                    );

                    const found = new Set<string>();

                    for (const message of result.messages) {
                      const event = yield* decodeWakeMessage(decoder.decode(message.data)).pipe(
                        Effect.mapError((cause) =>
                          invalid(id, `Cannot decode trigger page after ${from}`, cause),
                        ),
                      );

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

                    // Durable marks and the page cursor commit together. Private marks
                    // can survive rollback, as on the local path above.
                    yield* transaction.withTransaction(
                      Effect.gen(function* () {
                        if (found.size > 0) yield* marks.mark(projection, [...found]);

                        if (offset !== from) yield* owner.write(key, offset);
                      }),
                    );
                    cursors.set(key, offset);
                    state.ahead = !result.upToDate;
                    yield* changed(state);
                  }).pipe(
                    Effect.catchTag("SqlError", (cause) =>
                      Effect.fail(storageFault("projection.triggers")(cause)),
                    ),
                    Effect.onError(() =>
                      Effect.sync(() => {
                        cursors.delete(key);
                        state.ahead = false;
                      }),
                    ),
                    Effect.catchTag("ProjectionFault", (fault) =>
                      Effect.gen(function* () {
                        stopped.set(
                          id,
                          runnerFailure(Cause.fail(fault), yield* Clock.currentTimeMillis),
                        );
                        yield* changed(state);
                        yield* Effect.logError(
                          `Projection ${projection} trigger ${id} stopped`,
                          fault,
                        );
                      }),
                    ),
                  ),
            ),
          )
          .pipe(Effect.result);

        // One unavailable owner must not prevent the other remote pages from committing.
        if (Result.isFailure(attempt) && first === undefined) first = attempt.failure;
      }

      remoteFailed = first !== undefined;

      if (first !== undefined) return yield* first;

      return undefined;
    });

    const follow = (signal: Effect.Effect<void>, policy: Schedule.Schedule<unknown>) =>
      Effect.forEach(
        remote,
        ([id, state]) =>
          Effect.gen(function* () {
            const key = streams.get(id)!;
            let liveCursor: string | undefined;
            let step = yield* Schedule.toStep(policy);

            const backoff = Effect.gen(function* () {
              const now = yield* Clock.currentTimeMillis;

              const delay = yield* Effect.suspend(() => step(now, undefined)).pipe(
                Effect.map(([, duration]) => duration),
                Pull.catchDone(() => Effect.never),
                Effect.catchCause((cause) =>
                  Effect.logWarning(
                    `Projection ${projection} trigger ${id} follower retry failed`,
                    cause,
                  ).pipe(Effect.andThen(Effect.never)),
                ),
              );

              yield* Effect.sleep(delay);
            });

            while (true) {
              const snapshot = yield* permit
                .withPermit(
                  Effect.gen(function* () {
                    return {
                      from: yield* cursor(key),
                      changed: state.changed,
                      stopped: stopped.has(id),
                    };
                  }),
                )
                .pipe(Effect.result);

              if (Result.isFailure(snapshot)) {
                yield* Effect.logWarning(
                  `Projection ${projection} trigger ${id} follower cursor failed`,
                  snapshot.failure,
                );
                yield* backoff;
                continue;
              }

              const { from, changed: committed, stopped: isStopped } = snapshot.success;

              if (isStopped) {
                yield* Deferred.await(committed);
                continue;
              }

              const result = yield* state.wire
                .readNext(id, { offset: from, cursor: liveCursor })
                .pipe(Effect.result);

              if (Result.isFailure(result)) {
                yield* Effect.logWarning(
                  `Projection ${projection} trigger ${id} follower read failed`,
                  result.failure,
                );
                yield* backoff;
                continue;
              }

              const page = result.success;
              liveCursor = page.cursor;
              step = yield* Schedule.toStep(policy);

              if (page.messages.length > 0 || page.nextOffset > from) {
                yield* permit.withPermit(
                  Effect.gen(function* () {
                    // A pass can ingest the hinted page while readNext is in flight.
                    if (!stopped.has(id) && (yield* cursor(key)) === from) {
                      state.ahead = true;
                      yield* signal;
                    }
                  }),
                );
                yield* Deferred.await(committed);
              } else if (page.closed) {
                yield* Deferred.await(committed);
              }
              // timedOut answers loop directly; the server supplies the long-poll wait.
            }
          }).pipe(Effect.forkScoped),
        { discard: true },
      );

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
                const state = remote.get(StreamId.make(id));

                if (state !== undefined) state.ahead = true;
              }),
            ),
            Effect.tap(() => {
              const state = remote.get(StreamId.make(id));

              return state === undefined ? Effect.void : changed(state);
            }),
            Effect.onError(() => Effect.sync(() => cursors.delete(key))),
          );

          return undefined;
        }),
      );

    return {
      next,
      read,
      readRemote,
      retrying: () => remoteFailed,
      follow,
      rescan,
      retry: () =>
        permit.withPermit(
          Effect.gen(function* () {
            stopped.clear();

            for (const state of remote.values()) {
              state.ahead = true;
              yield* changed(state);
            }
          }),
        ),
      status: () =>
        Array.map([...streams.keys()], (stream) => ({
          stream,
          failure: Option.fromUndefinedOr(stopped.get(stream)),
        })),
    };
  });

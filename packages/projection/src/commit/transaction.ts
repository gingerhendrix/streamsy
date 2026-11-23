import { Array, Effect, Option, Predicate, Record } from "effect";
import { SqlClient } from "effect/sql";
import { isStreamsFault, ZERO_OFFSET } from "@streamsy/core";
import { CurrentOwner, isForeignOwner, ownerOf } from "@streamsy/core/internal";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { make, type RetireRequest } from "./model.ts";
import { CheckpointStore } from "../checkpoint-store.ts";
import { Transaction } from "../transaction.ts";
import { ProjectionFault } from "../fault.ts";
import { createOutput } from "../outputs.ts";
import { TransactionGenerationOutputs } from "../per-generation.ts";
import { detailsOf, load, remove, retire, settle, type Result } from "./internal.ts";

import { inherit, withPrepare, withAccepted } from "./range.ts";
import type { ProjectionKey } from "../checkpoint-store.ts";
import {
  requireRouteRecords,
  RoutedOutputs,
  routedUnit,
  TransactionRoutedOutputs,
} from "../routed-outputs.ts";

const removeWithRoutes = (
  key: ProjectionKey,
): Effect.Effect<void, ProjectionFault, CheckpointStore | Transaction> =>
  Effect.gen(function* () {
    const store = yield* CheckpointStore;
    const tx = yield* Transaction;

    if (ownerOf(store.load) !== tx.owner)
      return yield* new ProjectionFault({
        phase: "checkpoint",
        reason: "unsupported-composition",
        message: `The checkpoint store for ${key.id} does not share the transaction owner`,
      });

    const records = yield* requireRouteRecords(store, "checkpoint");

    // Load first to serialize with retirement. A tombstone preserves records for close/remove.
    const live = yield* store.load(key).pipe(
      Effect.map(({ record }) => Option.isSome(record)),
      Effect.catchIf(
        (fault) => fault.reason === "retired-generation",
        () => Effect.succeed(false),
      ),
      Effect.catchIf(
        (fault) => fault.reason === "invalid-record",
        () => Effect.succeed(true),
      ),
    );

    if (live) {
      yield* records.clear(key);
      yield* store.remove(key);
    }

    return yield* Effect.void;
  });

const retireInTransaction = (request: RetireRequest) =>
  Effect.gen(function* () {
    const tx = yield* Transaction;
    const store = yield* CheckpointStore;

    if (ownerOf(store.load) !== tx.owner)
      return yield* new ProjectionFault({
        phase: "checkpoint",
        reason: "unsupported-composition",
        message: `The checkpoint store for ${request.key.id} does not share the transaction owner`,
      });

    return yield* tx.withTransaction(
      retire(request).pipe(Effect.provideService(CurrentOwner, tx.owner)),
    );
  });

/** Run and checkpoint a unit on one SQL owner. Entering from another SQL owner's transaction dies. */
export const transaction = withPrepare(
  make<Result, SqlClient.SqlClient, CheckpointStore | Transaction>({
    name: "transaction",
    guarantee: "exactly-once",
    load,
    remove: (key) =>
      RoutedOutputs in key
        ? Transaction.use((tx) => tx.withTransaction(removeWithRoutes(key)))
        : remove(key),
    retire: retireInTransaction,
    apply: (pending) =>
      Effect.gen(function* () {
        const tx = yield* Transaction;
        const store = yield* CheckpointStore;
        const { outputs, prepare } = detailsOf(pending);
        const hasRoutes = RoutedOutputs in pending.key;

        if (hasRoutes && ownerOf(store.load) !== tx.owner)
          return yield* new ProjectionFault({
            phase: "checkpoint",
            reason: "unsupported-composition",
            message: `The checkpoint store for ${pending.key.id} does not share the transaction owner`,
          });

        for (const { address, service } of Record.values(outputs)) {
          if (service.owner !== tx.owner)
            return yield* new ProjectionFault({
              phase: "load",
              reason: "unsupported-composition",
              message: `Output ${address.id} does not share the transaction owner`,
            });
        }

        if (prepare !== undefined) yield* prepare;
        yield* tx.withTransaction(
          Effect.gen(function* () {
            const again = yield* store.load(pending.key);

            if (again.token !== pending.loaded.token)
              return yield* new ProjectionFault({
                phase: "checkpoint",
                reason: "token-conflict",
                message: `Record of ${pending.key.id} changed while the unit was read. A second writer on the same key is the likely cause. Use one writer per key: the follower or one run at a time.`,
              });

            if (Option.isNone(again.record)) {
              for (const { address, service, generation } of Record.values(outputs)) {
                if (generation === undefined) continue;
                const created = yield* createOutput(address, service.wire);

                if (!generation.shared && (created.closed || created.nextOffset !== ZERO_OFFSET))
                  return yield* new ProjectionFault({
                    phase: "process",
                    reason: "invalid-output",
                    message: `Generation-bound output ${address.id} must start empty and open`,
                  });
              }
            }

            const result = yield* pending.run.pipe(
              Effect.provideService(SqlClient.SqlClient, tx.sql),
              Effect.provideService(CurrentOwner, tx.owner),
              Effect.provideService(TransactionGenerationOutputs, pending.generationOutputs),
              Effect.provideService(
                TransactionRoutedOutputs,
                hasRoutes ? routedUnit(pending.key, store) : undefined,
              ),
              Effect.catchDefect((defect) =>
                isForeignOwner(defect)
                  ? Effect.fail(
                      new ProjectionFault({
                        phase: "process",
                        reason: "unsupported-composition",
                        message: defect.message,
                      }),
                    )
                  : Effect.die(defect),
              ),
            );

            if (result !== undefined && "outputs" in result) {
              for (const [name, output] of Record.toEntries(result.outputs)) {
                if (!Array.isReadonlyArrayNonEmpty(output.items)) continue;
                const resolved = outputs[name];

                if (resolved === undefined)
                  return yield* new ProjectionFault({
                    phase: "process",
                    reason: "invalid-output",
                    message: `Undeclared output ${name}`,
                  });
                yield* Streams.append(output.address, output.items).pipe(
                  Effect.provideService(StreamsWriter, resolved.service.wire),
                  Effect.mapError(
                    (cause) =>
                      new ProjectionFault({
                        phase: "process",
                        reason: isStreamsFault(cause)
                          ? "storage-failure"
                          : Predicate.isTagged(cause, "StreamBusy")
                            ? "output-busy"
                            : "invalid-output",
                        message: `Cannot append to ${output.address.id}`,
                        cause,
                      }),
                  ),
                );
              }
            }

            yield* settle(pending, result);

            return yield* Effect.void;
          }),
        );

        return yield* Effect.void;
      }),
  }),
).pipe(withAccepted);

/** Clean up binding state in the owner transaction on both forget and retire, using the same nesting rules as `transaction`. */
export const transactionWithCleanup = (
  cleanup: (key: ProjectionKey, sql: SqlClient.SqlClient) => Effect.Effect<void, ProjectionFault>,
) =>
  inherit(transaction, {
    ...transaction,
    remove: (key: ProjectionKey) =>
      Transaction.use((tx) =>
        tx.withTransaction(
          (RoutedOutputs in key ? removeWithRoutes(key) : remove(key)).pipe(
            Effect.andThen(() => cleanup(key, tx.sql)),
          ),
        ),
      ),
    retire: (request: RetireRequest) =>
      Transaction.use((tx) =>
        retireInTransaction({
          ...request,
          cleanup: request.cleanup.pipe(Effect.andThen(() => cleanup(request.key, tx.sql))),
        }),
      ),
  });

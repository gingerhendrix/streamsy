import { Array, Effect, Option, Predicate, Record } from "effect";
import { SqlClient } from "effect/sql";
import { isStreamsFault, ZERO_OFFSET } from "@streamsy/core";
import { CurrentOwner, isForeignOwner, ownerOf } from "@streamsy/core/internal";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { make, type Commit, type RetireRequest } from "./model.ts";
import { CheckpointStore } from "../checkpoint-store.ts";
import { Transaction } from "../transaction.ts";
import { OwnerTransaction } from "../owner-transaction.ts";
import { ProjectionFault } from "../fault.ts";
import { createOutput } from "../outputs.ts";
import { TransactionGenerationOutputs } from "../per-generation.ts";
import {
  detailsOf,
  load,
  remove,
  retire,
  settle,
  type Result as TransactionResult,
} from "./internal.ts";

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
  owner: OwnerBoundary["owner"],
): Effect.Effect<void, ProjectionFault, CheckpointStore> =>
  Effect.gen(function* () {
    const store = yield* CheckpointStore;

    if (ownerOf(store.load) !== owner)
      return yield* new ProjectionFault({
        phase: "checkpoint",
        reason: "unsupported-composition",
        message: `The checkpoint store for ${key.id} does not share the transaction owner`,
      });

    const records = yield* requireRouteRecords(store, "checkpoint");

    // An existing row is locked by the load. An absent row cannot be locked,
    // so do not remove a checkpoint that another runner might create afterward.
    const checkpoint = yield* store.load(key).pipe(
      Effect.map(({ record }): "present" | "absent" =>
        Option.isSome(record) ? "present" : "absent",
      ),
      Effect.catchIf(
        (fault) => fault.reason === "retired-generation",
        () => Effect.succeed("retired" as const),
      ),
      Effect.catchIf(
        (fault) => fault.reason === "invalid-record",
        () => Effect.succeed("present" as const),
      ),
    );

    if (checkpoint === "absent") {
      yield* records.forget(key, { ignoreUnsupportedList: true });
    } else if (checkpoint === "present") {
      yield* records.forget(key);
      yield* store.remove(key);
    }

    return yield* Effect.void;
  });

type OwnerBoundary = Pick<typeof Transaction.Service, "owner" | "withTransaction">;

interface TransactionContext<P> extends OwnerBoundary {
  readonly provide: <A, E>(body: Effect.Effect<A, E, P>) => Effect.Effect<A, E>;
}

const retireInTransaction = (tx: OwnerBoundary, request: RetireRequest) =>
  Effect.gen(function* () {
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

const makeTransaction = <P, Q>(
  strategy: string,
  context: Effect.Effect<TransactionContext<P>, never, Q>,
  requireSharedOwner = false,
) =>
  withPrepare(
    make<TransactionResult, P, CheckpointStore | Q>({
      name: strategy,
      guarantee: "exactly-once",
      load,
      remove: (key) =>
        RoutedOutputs in key
          ? Effect.flatMap(context, (tx) => tx.withTransaction(removeWithRoutes(key, tx.owner)))
          : remove(key),
      retire: (request) => Effect.flatMap(context, (tx) => retireInTransaction(tx, request)),
      apply: (pending) =>
        Effect.gen(function* () {
          const tx = yield* context;
          const store = yield* CheckpointStore;
          const { outputs, prepare } = detailsOf(pending);
          const hasRoutes = RoutedOutputs in pending.key;

          if ((requireSharedOwner || hasRoutes) && ownerOf(store.load) !== tx.owner)
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
                tx.provide,
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

/**
 * Run and checkpoint a unit on one SQL owner. On the same client the unit joins
 * the outer transaction without a savepoint. With two boundaries over one client,
 * only the outer boundary's hooks and `committed` subscribers see the commit.
 *
 * On another SQL client the unit opens its own transaction. It commits first and
 * survives an outer rollback. Use one client per database for nested work. With
 * two clients on one database, SQLite can wait until it reports a busy lock.
 * Postgres or MySQL can wait for a row lock that the outer transaction holds.
 *
 * Durable Object clients share the driver's transaction, so a second client
 * joins the first. Using a second object's storage on one fiber is unsupported.
 */
export const transaction = makeTransaction<SqlClient.SqlClient, Transaction>(
  "transaction",
  Effect.map(Transaction, (tx) => ({
    owner: tx.owner,
    withTransaction: (body) => tx.withTransaction(body),
    provide: (body) => body.pipe(Effect.provideService(SqlClient.SqlClient, tx.sql)),
  })),
);

/**
 * Clean up binding state in the owner's SQL transaction on forget and retire.
 * The boundary follows the same nesting rules as {@link transaction}.
 */
export const transactionWithCleanup = (
  cleanup: (key: ProjectionKey, sql: SqlClient.SqlClient) => Effect.Effect<void, ProjectionFault>,
) =>
  inherit(transaction, {
    ...transaction,
    remove: (key: ProjectionKey) =>
      Transaction.use((tx) =>
        tx.withTransaction(
          (RoutedOutputs in key ? removeWithRoutes(key, tx.owner) : remove(key)).pipe(
            Effect.andThen(() => cleanup(key, tx.sql)),
          ),
        ),
      ),
    retire: (request: RetireRequest) =>
      Transaction.use((tx) =>
        retireInTransaction(tx, {
          ...request,
          cleanup: request.cleanup.pipe(Effect.andThen(() => cleanup(request.key, tx.sql))),
        }),
      ),
  });

const OwnerCommitTypeId = Symbol("@streamsy/projection/OwnerCommit");

/** A commit that joins the storage owner's transaction and supplies no handler services. */
export interface OwnerCommit<in Result, out Requires> extends Commit<Result, never, Requires> {
  readonly [OwnerCommitTypeId]: true;
}

const removeInOwnerTransaction = <R>(
  tx: OwnerBoundary,
  key: ProjectionKey,
  cleanup: Effect.Effect<void, ProjectionFault, R>,
) =>
  tx.withTransaction(
    Effect.gen(function* () {
      if (RoutedOutputs in key) yield* removeWithRoutes(key, tx.owner);
      else {
        const store = yield* CheckpointStore;

        if (ownerOf(store.load) !== tx.owner)
          return yield* new ProjectionFault({
            phase: "checkpoint",
            reason: "unsupported-composition",
            message: `The checkpoint store for ${key.id} does not share the transaction owner`,
          });

        yield* store.remove(key);
      }

      return yield* cleanup.pipe(Effect.provideService(CurrentOwner, tx.owner));
    }),
  );

/** Commit the unit and checkpoint together on a memory or SQL storage host. */
export const ownerTransaction: OwnerCommit<TransactionResult, CheckpointStore | OwnerTransaction> =
  Object.assign(
    makeTransaction<never, OwnerTransaction>(
      "ownerTransaction",
      Effect.map(OwnerTransaction, (tx) => ({
        owner: tx.owner,
        withTransaction: (body) => tx.withTransaction(body),
        provide: (body) => body,
      })),
      true,
    ),
    {
      [OwnerCommitTypeId]: true as const,
      remove: (key: ProjectionKey) =>
        OwnerTransaction.use((tx) => removeInOwnerTransaction(tx, key, Effect.void)),
    },
  );

/** Clean up binding state in the owner transaction on both forget and retire. */
export const ownerTransactionWithCleanup = <R>(
  cleanup: (key: ProjectionKey) => Effect.Effect<void, ProjectionFault, R>,
): OwnerCommit<TransactionResult, CheckpointStore | OwnerTransaction | R> =>
  inherit(ownerTransaction, {
    ...ownerTransaction,
    remove: (key: ProjectionKey) =>
      OwnerTransaction.use((tx) =>
        removeInOwnerTransaction(
          tx,
          key,
          Effect.suspend(() => cleanup(key)),
        ),
      ),
    retire: (request: RetireRequest) =>
      Effect.gen(function* () {
        const tx = yield* OwnerTransaction;
        const services = yield* Effect.context<R>();

        return yield* retireInTransaction(tx, {
          ...request,
          cleanup: request.cleanup.pipe(
            Effect.andThen(() => cleanup(request.key).pipe(Effect.provideContext(services))),
          ),
        });
      }),
  });

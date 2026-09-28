import { Context, Effect, Option } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { CurrentOwner, isForeignOwner } from "@streamsy/core/internal/memory";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { make, type HandlerFailed } from "./model.ts";
import { CheckpointStore } from "../checkpoint-store.ts";
import { Transaction } from "../transaction.ts";
import { ProjectionFault } from "../fault.ts";
import { detailsOf, load, remove, settle, type Result } from "./internal.ts";

export const transaction = make<Result, SqlClient.SqlClient, CheckpointStore | Transaction>({
  name: "transaction",
  guarantee: "exactly-once",
  load,
  remove,
  apply: (pending) =>
    Effect.gen(function* () {
      const tx = yield* Transaction;
      const store = yield* CheckpointStore;
      const outputs = detailsOf(pending).outputs;
      for (const { address, service } of Object.values(outputs)) {
        if (service.owner !== tx.owner)
          return yield* new ProjectionFault({
            phase: "load",
            reason: "unsupported-composition",
            message: `Output ${address.id} does not share the transaction owner`,
          });
      }
      const prepare = detailsOf(pending).prepare;
      if (prepare !== undefined) yield* prepare;
      yield* tx.withTransaction(
        Effect.gen(function* () {
          const again = yield* store.load(pending.key);
          if (again.token !== pending.loaded.token)
            return yield* new ProjectionFault({
              phase: "checkpoint",
              reason: "token-conflict",
              message: `Record of ${pending.key.id} changed while the unit was read`,
            });
          // SAFETY: with no SQL client, remove ambient SQL and translate only the resulting missing-service defect.
          const run = Option.isSome(tx.sql)
            ? Effect.provideService(pending.run, SqlClient.SqlClient, tx.sql.value)
            : Effect.updateContext(
                pending.run as Effect.Effect<Result, HandlerFailed>,
                (context: Context.Context<never>) => Context.omit(SqlClient.SqlClient)(context),
              );
          const result = yield* run.pipe(
            Effect.provideService(CurrentOwner, tx.owner),
            Effect.catchDefect((defect) =>
              isForeignOwner(defect) ||
              (Option.isNone(tx.sql) &&
                defect instanceof Error &&
                defect.message === "Service not found: effect/sql/SqlClient")
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
            for (const [name, output] of Object.entries(result.outputs)) {
              if (output.items.length === 0) continue;
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
                      reason: "invalid-output",
                      message: `Cannot append to ${output.address.id}`,
                      cause,
                    }),
                ),
              );
            }
          }
          yield* settle(pending, result);
        }),
      );
    }),
});

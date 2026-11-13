import { Effect } from "effect";
import type { PruneOptions } from "./store.ts";
import { OutboxUnavailable } from "./errors.ts";

/** Keep SQL and memory bounds identical; refuse unsafe numbers before a query. */
export function validateOptions(
  operation: string,
  options: {
    readonly limit?: number;
    readonly afterId?: number;
    readonly dueBeforeMs?: number;
  },
): Effect.Effect<void, OutboxUnavailable> {
  const { limit, afterId, dueBeforeMs } = options;
  const detail =
    limit !== undefined && (!Number.isSafeInteger(limit) || limit <= 0)
      ? "limit must be a positive safe integer"
      : afterId !== undefined && (!Number.isSafeInteger(afterId) || afterId < 0)
        ? "afterId must be a nonnegative safe integer"
        : dueBeforeMs !== undefined && !Number.isSafeInteger(dueBeforeMs)
          ? "dueBeforeMs must be safe integer epoch milliseconds"
          : undefined;
  return detail === undefined
    ? Effect.void
    : Effect.fail(new OutboxUnavailable({ operation, detail }));
}

export const validatePrune = (options: PruneOptions): Effect.Effect<void, OutboxUnavailable> =>
  (options.state !== "delivered" && options.state !== "dead") ||
  !Number.isSafeInteger(options.settledBeforeMs) ||
  !Number.isSafeInteger(options.limit) ||
  options.limit <= 0
    ? Effect.fail(
        new OutboxUnavailable({
          operation: "prune",
          detail:
            "prune requires a delivered or dead state, safe integer settledBeforeMs and positive safe integer limit",
        }),
      )
    : Effect.void;

export const validateRequeue = (
  id: number,
  atMs: number,
): Effect.Effect<void, OutboxUnavailable> =>
  Number.isSafeInteger(id) && id > 0 && Number.isSafeInteger(atMs)
    ? Effect.void
    : Effect.fail(
        new OutboxUnavailable({
          operation: "requeue",
          detail: "requeue requires a positive safe integer id and safe integer atMs",
        }),
      );

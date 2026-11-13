import { Effect } from "effect";
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

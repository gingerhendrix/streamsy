import { Effect, Schedule } from "effect";

export interface TransactionRetryPolicy {
  readonly attempts: number;
  readonly delayMs: number;
}

export const DEFAULT_TRANSACTION_RETRY_ATTEMPTS = 16;
export const DEFAULT_TRANSACTION_RETRY_DELAY_MS = 25;

export const transactionRetryPolicy = (options: {
  readonly transactionRetryAttempts?: number;
  readonly transactionRetryDelayMs?: number;
}): TransactionRetryPolicy => {
  const attempts = options.transactionRetryAttempts ?? DEFAULT_TRANSACTION_RETRY_ATTEMPTS;
  const delayMs = options.transactionRetryDelayMs ?? DEFAULT_TRANSACTION_RETRY_DELAY_MS;
  if (!Number.isSafeInteger(attempts) || attempts <= 0)
    throw new RangeError("transactionRetryAttempts must be a positive safe integer");
  if (!Number.isFinite(delayMs) || delayMs <= 0)
    throw new RangeError("transactionRetryDelayMs must be positive");
  return { attempts, delayMs };
};

/** Retry only explicitly classified failures; defects and interruption are never retried. */
export const retryWithPolicy = <A, E, R>(
  transaction: Effect.Effect<A, E, R>,
  policy: TransactionRetryPolicy,
  isRetryable: (error: E) => boolean,
): Effect.Effect<A, E, R> =>
  transaction.pipe(
    Effect.retry({
      times: policy.attempts - 1,
      schedule: Schedule.spaced(policy.delayMs),
      while: isRetryable,
    }),
  );

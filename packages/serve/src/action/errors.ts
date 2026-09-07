/**
 * Typed action-sink failures.
 *
 * An action sink has two failure domains and they are deliberately separate.
 * `OutboxUnavailable` is a storage failure: the durable outbox itself could not
 * be read or written, and no delivery decision can be trusted until it
 * recovers. `ActionSinkDeliveryFailure` is a *handler* outcome: the delivery was
 * attempted and the external effect refused it. The first is an operational
 * error the host reports; the second is ordinary traffic for the retry policy
 * and never leaves the delivery loop.
 */
import { Schema } from "effect";
import type * as Cause from "effect/Cause";

export const ACTION_SINK_ERROR_TAGS = ["ActionSinkDeliveryFailure", "OutboxUnavailable"] as const;

export type ActionSinkErrorTag = (typeof ACTION_SINK_ERROR_TAGS)[number];

/** The durable outbox could not be read or written. Never a delivery outcome. */
type OutboxUnavailableSchema = Schema.TaggedStruct<
  "OutboxUnavailable",
  { readonly operation: Schema.String; readonly detail: Schema.String }
>;
const OutboxUnavailableBase: Schema.Class<
  OutboxUnavailable,
  OutboxUnavailableSchema,
  Cause.YieldableError
> = Schema.TaggedError<OutboxUnavailable>()("OutboxUnavailable", {
  operation: Schema.String,
  detail: Schema.String,
});
export class OutboxUnavailable extends OutboxUnavailableBase {}

/**
 * A handler refused one delivery.
 *
 * `retryable` is the handler's own judgement. A handler that knows the failure
 * cannot change says so, and the runtime dead-letters immediately instead of
 * spending the whole retry budget on an outcome that is already settled.
 */
type ActionSinkDeliveryFailureSchema = Schema.TaggedStruct<
  "ActionSinkDeliveryFailure",
  {
    readonly handler: Schema.String;
    readonly idempotencyKey: Schema.String;
    readonly detail: Schema.String;
    readonly retryable: Schema.Boolean;
  }
>;
const ActionSinkDeliveryFailureBase: Schema.Class<
  ActionSinkDeliveryFailure,
  ActionSinkDeliveryFailureSchema,
  Cause.YieldableError
> = Schema.TaggedError<ActionSinkDeliveryFailure>()("ActionSinkDeliveryFailure", {
  handler: Schema.String,
  idempotencyKey: Schema.String,
  detail: Schema.String,
  retryable: Schema.Boolean,
});
export class ActionSinkDeliveryFailure extends ActionSinkDeliveryFailureBase {
  static retryable(
    handler: string,
    idempotencyKey: string,
    detail: string,
  ): ActionSinkDeliveryFailure {
    return new ActionSinkDeliveryFailure({ handler, idempotencyKey, detail, retryable: true });
  }
  static permanent(
    handler: string,
    idempotencyKey: string,
    detail: string,
  ): ActionSinkDeliveryFailure {
    return new ActionSinkDeliveryFailure({ handler, idempotencyKey, detail, retryable: false });
  }
}

/** Why one outbox entry stopped being retried. */
export const DEAD_LETTER_REASONS = ["attempts-exhausted", "permanent", "payload-poison"] as const;
export type DeadLetterReason = (typeof DEAD_LETTER_REASONS)[number];

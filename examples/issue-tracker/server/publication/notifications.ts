/**
 * The `effectSink` runtime for `issue-tracker.assignment-notifications`.
 *
 * Three things live here and they are deliberately apart:
 *
 * - `assignmentDrafts` decides *what* to deliver. It is pure: it folds the one
 *   accepted fact through the declaration's own reducer, reads the resulting
 *   change, and lowers it to outbox drafts. Being pure is what lets the command
 *   path write those drafts inside the same durable step as the receipt.
 * - `NotificationTarget` is *where* a delivery goes. It is the external effect,
 *   behind a service, so the host points the sink at a real notifier and a test
 *   points it at a recording one without either appearing in the declaration.
 * - `assignmentHandler` and `drainAssignments` are *how* delivery is attempted:
 *   the declared retry budget, applied by the package's serialized runtime.
 */
import { drain, draftsFor, effectSinkHandler } from "@streamsy/sinks/action/runtime";
import { type OutboxDraft } from "@streamsy/sinks/action/outbox";
import { Context, Effect, Layer, Schema } from "effect";
import { assignmentNotifications, issueLifecycle, issues } from "../../domain/declaration.ts";
import { assignmentOf, type AssignmentNotification } from "../../domain/notifications.ts";
import { decodeIssueRow, type IssueEvent, type IssueRow } from "../../domain/issue.ts";
import { maintain } from "../../views/engine.ts";

const decodeJsonObject = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Json));

/** Why a notification target refused one delivery. */
export interface NotificationRefusal {
  readonly detail: string;
  /** True when no later attempt can succeed for this notification. */
  readonly permanent: boolean;
}

export interface NotificationTargetService {
  /**
   * Perform the external effect, once per idempotency key.
   *
   * Delivery is at-least-once by construction — a host can die between a
   * successful call and the outbox write that records it — so absorbing a key
   * this target has already accepted is the target's half of the contract.
   */
  readonly accept: (
    idempotencyKey: string,
    notification: AssignmentNotification,
  ) => Effect.Effect<"accepted" | "absorbed", NotificationRefusal>;
  /** What the target actually accepted, in acceptance order. */
  readonly accepted: (workspaceId: string) => Effect.Effect<readonly AssignmentNotification[]>;
}

export class NotificationTarget extends Context.Service<
  NotificationTarget,
  NotificationTargetService
>()("issue-tracker/NotificationTarget") {}

export interface NotificationTargetOptions {
  /** Refuse selected notifications, so a host or a test can drive the retry policy. */
  readonly refuse?: (notification: AssignmentNotification) => NotificationRefusal | undefined;
}

/**
 * The local notification target: an append-only log of what was notified.
 *
 * A log is a real external effect for this slice — it is the observable the
 * product surfaces — and it keeps the delivery path honest without inviting a
 * mail transport into an example.
 */
export const notificationTargetLayer = (
  options: NotificationTargetOptions = {},
): Layer.Layer<NotificationTarget> =>
  Layer.sync(NotificationTarget, () => {
    const accepted: AssignmentNotification[] = [];
    const keys = new Set<string>();
    return NotificationTarget.of({
      accept: Effect.fn("NotificationTarget.accept")(function* (
        idempotencyKey: string,
        notification: AssignmentNotification,
      ) {
        const refusal = options.refuse?.(notification);
        if (refusal !== undefined) return yield* Effect.fail(refusal);
        if (keys.has(idempotencyKey)) return "absorbed" as const;
        keys.add(idempotencyKey);
        accepted.push(notification);
        return "accepted" as const;
      }),
      accepted: Effect.fn("NotificationTarget.accepted")((workspaceId: string) =>
        Effect.sync(() =>
          accepted.filter((notification) => notification.workspaceId === workspaceId),
        ),
      ),
    });
  });

/** The declared handler, bound to whatever target the host's layer supplies. */
export const assignmentHandler = effectSinkHandler<
  AssignmentNotification,
  typeof assignmentNotifications.from,
  NotificationTarget
>(assignmentNotifications, (delivery, refuse) =>
  Effect.gen(function* () {
    const target = yield* NotificationTarget;
    return yield* target.accept(delivery.idempotencyKey, delivery.payload).pipe(
      Effect.mapError((refusal) =>
        refusal.permanent ? refuse.permanent(refusal.detail) : refuse.retryable(refusal.detail),
      ),
      Effect.asVoid,
    );
  }),
);

/** One serialized delivery pass over a workspace's pending assignment notifications. */
export const drainAssignments = Effect.fn("Notifications.drain")(function* (workspaceId: string) {
  return yield* drain(assignmentNotifications, assignmentHandler, { partitionId: workspaceId });
});

/**
 * The outbox drafts one accepted canonical fact implies.
 *
 * The fact is folded through the *declared* reducer rather than through a
 * hand-written rule, so what the sink observes is the same change the
 * maintenance pass will commit. `before` is the maintained row as the command
 * path read it; an absent row means the fact creates one.
 */
export function assignmentDrafts(
  event: IssueEvent,
  before: IssueRow | undefined,
  enqueuedAtMs: number,
): readonly OutboxDraft[] {
  const current =
    before === undefined ? new Map<string, IssueRow>() : new Map([[before.issueId, before]]);
  const folded = maintain<IssueRow>({
    plan: issues.plan,
    reducer: issueLifecycle,
    decodeRow: decodeIssueRow,
    current,
    items: [decodeJsonObject(event)],
  });
  const payloads: AssignmentNotification[] = [];
  for (const change of folded.changes) {
    const notification = assignmentOf(change, event);
    if (notification !== undefined) payloads.push(notification);
  }
  return draftsFor(assignmentNotifications, payloads, enqueuedAtMs);
}

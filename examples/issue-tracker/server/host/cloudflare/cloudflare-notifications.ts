/* oxlint-disable effecttsgo/global-date -- Acceptance time is recorded at the Cloudflare target edge. */
import type { DurableObjectStorage, SqlStorageValue } from "@cloudflare/workers-types";
import { Effect, Layer, Schema } from "effect";
import {
  NotificationTarget,
  type NotificationTargetService,
} from "../../publication/notifications.ts";
import {
  AssignmentNotification,
  decodeAssignmentNotification,
} from "../../../domain/notifications.ts";

const NOTIFICATION_SCHEMA = `CREATE TABLE IF NOT EXISTS issue_tracker_notification_acceptances (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  idempotency_key TEXT NOT NULL UNIQUE,
  workspace_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  accepted_at_ms INTEGER NOT NULL
)`;

interface NotificationRow extends Record<string, SqlStorageValue> {
  readonly payload_json: string;
}

const decodePayload = Schema.decodeUnknownSync(Schema.fromJsonString(AssignmentNotification));

/** Durable target colocated with one workspace object's outbox. */
export const cloudflareNotificationTargetLayer = (
  storage: DurableObjectStorage,
  interruptAfterAccept: () => boolean,
): Layer.Layer<NotificationTarget> =>
  Layer.sync(NotificationTarget, () => {
    const sql = storage.sql;
    sql.exec(NOTIFICATION_SCHEMA);
    return NotificationTarget.of({
      accept: Effect.fn("CloudflareNotificationTarget.accept")(function* (
        idempotencyKey: string,
        notification: AssignmentNotification,
      ) {
        const accepted = storage.transactionSync(() => {
          const existing = [
            ...sql.exec<{ readonly present: number }>(
              "SELECT 1 present FROM issue_tracker_notification_acceptances" +
                " WHERE idempotency_key = ?",
              idempotencyKey,
            ),
          ][0];
          if (existing !== undefined) return false;
          sql.exec(
            "INSERT INTO issue_tracker_notification_acceptances" +
              " (idempotency_key, workspace_id, payload_json, accepted_at_ms) VALUES (?, ?, ?, ?)",
            idempotencyKey,
            notification.workspaceId,
            JSON.stringify(notification),
            Date.now(),
          );
          return true;
        });
        if (accepted && interruptAfterAccept()) {
          return yield* Effect.interrupt.pipe(Effect.as("accepted" as const));
        }
        return accepted ? "accepted" : "absorbed";
      }),
      accepted: Effect.fn("CloudflareNotificationTarget.accepted")((workspaceId: string) =>
        Effect.sync(() =>
          [
            ...sql.exec<NotificationRow>(
              "SELECT payload_json FROM issue_tracker_notification_acceptances" +
                " WHERE workspace_id = ? ORDER BY id",
              workspaceId,
            ),
          ].map((row) => decodeAssignmentNotification(decodePayload(row.payload_json))),
        ),
      ),
    } satisfies NotificationTargetService);
  });

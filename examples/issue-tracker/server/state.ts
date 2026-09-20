import { Streams, ZERO_OFFSET } from "@streamsy/core";
import { Effect, Stream } from "effect";
import type { CommandRequest } from "../shared/api.ts";
import { foldIssue, type IssueEvent, type IssueRow } from "../domain/issue.ts";
import { events } from "./streams.ts";

export const foldEvents = (items: ReadonlyArray<IssueEvent>): Map<string, IssueRow> => {
  const rows = new Map<string, IssueRow>();
  for (const item of items) {
    const next = foldIssue(rows.get(item.issueId), item);
    if (next !== undefined) rows.set(item.issueId, next);
  }
  return rows;
};

export const transact = (workspaceId: string, command: CommandRequest) =>
  Effect.gen(function* () {
    const ref = events.ref({ workspaceId });
    const batches = yield* Streams.read(ref).pipe(Stream.runCollect);
    const items = batches.flatMap((batch) => batch.items);
    const rows = foldEvents(items);
    const head = batches.at(-1)?.nextOffset ?? ZERO_OFFSET;
    const previous = command.type === "create" ? undefined : rows.get(command.issueId);
    if (command.type !== "create" && previous === undefined)
      return yield* Effect.fail({ _tag: "UnknownIssue" as const });
    const sequence = items.reduce((highest, item) => Math.max(highest, item.sequence), -1) + 1;
    const occurredAt = new Date().toISOString();
    const common = {
      eventId: command.commandId,
      workspaceId,
      issueId: command.issueId,
      sequence,
      occurredAt,
    };
    const event: IssueEvent =
      command.type === "create"
        ? {
            type: "IssueCreated",
            ...common,
            projectId: command.projectId,
            title: command.title,
            status: command.status ?? "backlog",
          }
        : command.type === "status"
          ? { type: "IssueStatusChanged", ...common, status: command.status }
          : {
              type: "IssueAssigned",
              ...common,
              status: previous!.status,
              assigneeId: command.assigneeId,
            };
    const ack = yield* Streams.append(ref, [event], { expectedOffset: head });
    return { event, ack };
  }).pipe(Effect.retry({ times: 3, while: (error) => error._tag === "OffsetMismatch" }));

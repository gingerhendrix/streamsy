import { Projection } from "@streamsy/projection";
import { Effect, Match, Schema } from "effect";
import { SqlClient } from "effect/sql";
import type { CommandRequest } from "../shared/api.ts";
import type { IssueEvent } from "../domain/issue.ts";
import { issueRows, storedIssue } from "./projection.ts";

/** A command names an issue that the workspace does not have. */
export class UnknownIssue extends Schema.TaggedError<UnknownIssue>()("UnknownIssue", {}) {}

export const transact = (workspaceId: string, command: CommandRequest) =>
  Projection.decide(issueRows.at({ workspaceId }), {
    input: "events",
    decide: () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        const previous =
          command.type === "create"
            ? undefined
            : yield* storedIssue(sql, workspaceId, command.issueId);

        if (command.type !== "create" && previous === undefined) return yield* new UnknownIssue();

        // Postgres and MySQL may return an aggregate as a `bigint` or a string.
        const [last] = yield* sql<{
          readonly sequence: number | bigint | string | null;
        }>`SELECT MAX(sequence) AS sequence FROM issues WHERE workspace_id = ${workspaceId}`;

        const sequence = (last?.sequence == null ? -1 : Number(last.sequence)) + 1;
        const occurredAt = new Date().toISOString();

        const common = {
          eventId: command.commandId,
          workspaceId,
          issueId: command.issueId,
          sequence,
          occurredAt,
        };

        const event: IssueEvent = Match.value(command).pipe(
          Match.discriminatorsExhaustive("type")({
            create: (create): IssueEvent => ({
              type: "IssueCreated",
              ...common,
              projectId: create.projectId,
              title: create.title,
              status: create.status ?? "backlog",
            }),
            status: (status): IssueEvent => ({
              type: "IssueStatusChanged",
              ...common,
              status: status.status,
            }),
            assign: (assign): IssueEvent => ({
              type: "IssueAssigned",
              ...common,
              status: previous!.status,
              assigneeId: assign.assigneeId,
            }),
          }),
        );

        return { value: event, items: [event] };
      }),
  }).pipe(Effect.map(({ value: event, offset }) => ({ event, offset })));

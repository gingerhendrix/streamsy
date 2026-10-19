import { Projection } from "@streamsy/projection";
import { DateTime, Effect, Match, Schema } from "effect";
import type { CommandRequest } from "../shared/api.ts";
import type { IssueEvent } from "../domain/issue.ts";
import { commandView } from "./command-view.ts";

/** A command names an issue that the workspace does not have. */
export class UnknownIssue extends Schema.TaggedError<
  UnknownIssue,
  { readonly brand: unique symbol }
>()("UnknownIssue", {}) {}

export const transact = (workspaceId: string, command: CommandRequest) =>
  Projection.decide(commandView.at({ workspaceId }), {
    input: "events",
    decide: ({ state }) =>
      Effect.gen(function* () {
        const previous = Object.hasOwn(state.issues, command.issueId)
          ? state.issues[command.issueId]
          : undefined;

        if (command.type !== "create" && previous === undefined) return yield* new UnknownIssue();
        const sequence = state.nextSequence;
        const occurredAt = DateTime.formatIso(yield* DateTime.now);

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

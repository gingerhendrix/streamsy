import { Projection } from "@streamsy/projection";
import { Schema } from "effect";
import { Identifier, IssueStatus } from "../domain/issue.ts";
import { Events } from "./streams.ts";

const CommandIssue = Schema.Struct({ status: IssueStatus, sequence: Schema.Finite });

/** Only the facts command checks need. Rows and outputs are written by watchers. */
export const CommandView = Schema.Struct({
  issues: Schema.Record(Schema.String, CommandIssue),
  nextSequence: Schema.Finite,
});

export const commandView = Projection.fold({
  id: "issue-commands",
  params: { workspaceId: Identifier },
  inputs: { events: Events },
  state: { schema: CommandView, initial: { issues: {}, nextSequence: 0 } },
  fold: (view, { item: event }) => {
    const previous = view.issues[event.issueId];

    const changesIssue =
      (event.type === "IssueCreated" || previous !== undefined) &&
      (previous === undefined || event.sequence > previous.sequence);

    return {
      nextSequence: Math.max(view.nextSequence, event.sequence + 1),
      issues: changesIssue
        ? { ...view.issues, [event.issueId]: { status: event.status, sequence: event.sequence } }
        : view.issues,
    };
  },
});

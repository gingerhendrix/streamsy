import { Match } from "effect";
import { BadRequest, Conflict, NotFound } from "../../shared/api-schema.ts";
import type { WorkspaceNotFound, WriteConflict } from "../issue-tracker.ts";
import type { IssueNotFound, UnknownReference } from "../state.ts";

type DomainError = WorkspaceNotFound | WriteConflict | UnknownReference | IssueNotFound;

/** The HTTP answer to each domain failure. */
export function toApiError(error: DomainError): BadRequest | NotFound | Conflict {
  return Match.valueTags(error, {
    WorkspaceNotFound: () => new NotFound({ error: "Unknown workspace" }),
    IssueNotFound: () => new NotFound({ error: "Issue not found" }),
    UnknownReference: ({ field }) => new BadRequest({ error: `Unknown ${field}` }),
    WriteConflict: () => new Conflict({ error: "Concurrent updates, please retry" }),
  });
}

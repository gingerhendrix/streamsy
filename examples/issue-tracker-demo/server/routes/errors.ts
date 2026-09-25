import { BadRequest, Conflict, NotFound } from "../../shared/api-schema.ts";
import type { WorkspaceNotFound, WriteConflict } from "../issue-tracker.ts";
import type { IssueNotFound, UnknownReference } from "../state.ts";

type DomainError = WorkspaceNotFound | WriteConflict | UnknownReference | IssueNotFound;

/** The HTTP answer to each domain failure. */
export function toApiError(error: DomainError): BadRequest | NotFound | Conflict {
  switch (error._tag) {
    case "WorkspaceNotFound":
      return new NotFound({ error: "Unknown workspace" });
    case "IssueNotFound":
      return new NotFound({ error: "Issue not found" });
    case "UnknownReference":
      return new BadRequest({ error: `Unknown ${error.field}` });
    case "WriteConflict":
      return new Conflict({ error: "Concurrent updates, please retry" });
  }
}

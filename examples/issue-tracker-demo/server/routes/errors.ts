import { Effect, Match } from "effect";
import type {
  ReadError,
  AppendErrorFor,
  StreamNotFound,
  StreamGone,
  OffsetMismatch,
  StreamsFault,
  DecodeFault,
  EncodeFault,
} from "@streamsy/core";
import { BadRequest, Conflict, InternalServerError, NotFound } from "../../shared/api-schema.ts";
import type { WorkspaceNotFound, WriteConflict } from "../issue-tracker.ts";
import type { IssueNotFound, UnknownReference } from "../state.ts";

type DomainError = WorkspaceNotFound | WriteConflict | UnknownReference | IssueNotFound;

type StoreError =
  | Exclude<
      ReadError | AppendErrorFor<{ expectedOffset: string }>,
      StreamNotFound | StreamGone | OffsetMismatch
    >
  | StreamsFault
  | DecodeFault
  | EncodeFault;

/** Answer domain failures with their status; log other failures and answer 500. */
export const apiErrors = <A, R>(handler: Effect.Effect<A, StoreError | DomainError, R>) =>
  handler.pipe(
    Effect.catch((error) =>
      Match.value(error).pipe(
        Match.tag("WorkspaceNotFound", () =>
          Effect.fail(new NotFound({ error: "Unknown workspace" })),
        ),
        Match.tag("IssueNotFound", () => Effect.fail(new NotFound({ error: "Issue not found" }))),
        Match.tag("UnknownReference", ({ field }) =>
          Effect.fail(new BadRequest({ error: `Unknown ${field}` })),
        ),
        Match.tag("WriteConflict", () =>
          Effect.fail(new Conflict({ error: "Concurrent updates, please retry" })),
        ),
        Match.orElse((cause) =>
          Effect.logError(cause).pipe(
            Effect.andThen(
              Effect.fail(new InternalServerError({ error: "Internal server error" })),
            ),
          ),
        ),
      ),
    ),
  );

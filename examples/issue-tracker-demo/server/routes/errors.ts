import { Effect, Match } from "effect";
import type { ReadFailure, AppendFailure, CreateFailure } from "@streamsy/core";
import type { PlatformError } from "effect";
import {
  BadRequest,
  Conflict,
  InternalServerError,
  NotFound,
  WipLimitReached,
} from "@streamsy/issue-tracker-demo-contract/api-schema";
import type { WorkspaceNotFound, WriteConflict } from "../issue-tracker.ts";
import type { IssueNotFound, TooManyInProgress, UnknownReference } from "../state.ts";

type DomainError = WorkspaceNotFound | WriteConflict | UnknownReference | IssueNotFound;

type StoreError =
  | ReadFailure
  | AppendFailure<{ expectedOffset: string }>
  | CreateFailure
  | PlatformError.PlatformError;

/** Answer a domain failure with its status; log any other failure and answer 500. */
const toApiError = (error: StoreError | DomainError) =>
  Match.value(error).pipe(
    Match.tag("WorkspaceNotFound", () => Effect.fail(new NotFound({ error: "Unknown workspace" }))),
    Match.tag("IssueNotFound", () => Effect.fail(new NotFound({ error: "Issue not found" }))),
    Match.tag("UnknownReference", ({ field }) =>
      Effect.fail(new BadRequest({ error: `Unknown ${field}` })),
    ),
    Match.tag("WriteConflict", () =>
      Effect.fail(new Conflict({ error: "Concurrent updates, please retry" })),
    ),
    Match.orElse((cause) =>
      Effect.logError(cause).pipe(
        Effect.andThen(Effect.fail(new InternalServerError({ error: "Internal server error" }))),
      ),
    ),
  );

export const apiErrors = <A, R>(handler: Effect.Effect<A, StoreError | DomainError, R>) =>
  handler.pipe(Effect.catch(toApiError));

/** Issue writes can also break the work-in-progress rule, which answers 422. */
export const issueApiErrors = <A, R>(
  handler: Effect.Effect<A, StoreError | DomainError | TooManyInProgress, R>,
) =>
  handler.pipe(
    Effect.catch((error) =>
      Match.value(error).pipe(
        Match.tag("TooManyInProgress", ({ limit }) =>
          Effect.fail(
            new WipLimitReached({ error: `This project already has ${limit} issues in progress` }),
          ),
        ),
        Match.orElse(toApiError),
      ),
    ),
  );

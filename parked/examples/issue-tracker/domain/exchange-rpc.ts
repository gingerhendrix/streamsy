/** Private, schema-checked wire contracts between issue-tracker domain objects. */
import { Schema } from "effect";
import { AssignmentActivity } from "./exchange.ts";
import { InboxRow } from "./inbox.ts";
import { Identifier, Sequence } from "./issue.ts";

export const EXCHANGE_NAME = "issue-tracker.assignment-inbox" as const;
export const EXCHANGE_VERSION = 1 as const;
const ExchangeIdentity = {
  exchange: Schema.Literal(EXCHANGE_NAME),
  version: Schema.Literal(EXCHANGE_VERSION),
};
export const WorkspaceSource = Schema.Struct({ kind: Schema.Literal("workspace"), id: Identifier });
export const UserDestination = Schema.Struct({ kind: Schema.Literal("user"), id: Identifier });

export const RegisterSourceRequest = Schema.Struct({
  operationId: Schema.String,
  ...ExchangeIdentity,
  source: WorkspaceSource,
});
export type RegisterSourceRequest = typeof RegisterSourceRequest.Type;

export const ReadAssignmentPageRequest = Schema.Struct({
  operationId: Schema.String,
  ...ExchangeIdentity,
  source: WorkspaceSource,
  afterArrival: Sequence,
  limit: Sequence,
});
export type ReadAssignmentPageRequest = typeof ReadAssignmentPageRequest.Type;

export const ReadAssignmentPageResult = Schema.Struct({
  operationId: Schema.String,
  requestHash: Schema.String,
  source: WorkspaceSource,
  fromArrival: Sequence,
  toArrival: Sequence,
  upToDate: Schema.Boolean,
  records: Schema.Array(AssignmentActivity),
});
export type ReadAssignmentPageResult = typeof ReadAssignmentPageResult.Type;

export const ApplyInboxBatchRequest = Schema.Struct({
  operationId: Schema.String,
  ...ExchangeIdentity,
  source: WorkspaceSource,
  destination: UserDestination,
  fromArrival: Sequence,
  toArrival: Sequence,
  rows: Schema.Array(InboxRow),
  payloadHash: Schema.String,
});
export type ApplyInboxBatchRequest = typeof ApplyInboxBatchRequest.Type;

export const ApplyInboxBatchResult = Schema.Struct({
  operationId: Schema.String,
  payloadHash: Schema.String,
  applied: Sequence,
});
export type ApplyInboxBatchResult = typeof ApplyInboxBatchResult.Type;

export type StableHashInput =
  | RegisterSourceRequest
  | ReadAssignmentPageRequest
  | {
      readonly source: typeof WorkspaceSource.Type;
      readonly destination: typeof UserDestination.Type;
      readonly fromArrival: number;
      readonly toArrival: number;
      readonly rows: readonly (typeof InboxRow.Type)[];
    };

export async function stableHash(value: StableHashInput): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify(value));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((part) => part.toString(16).padStart(2, "0")).join("");
}

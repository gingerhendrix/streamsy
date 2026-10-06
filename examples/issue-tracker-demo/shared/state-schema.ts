import { createStateSchema } from "@durable-streams/state";
import { Schema } from "effect";

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | ReadonlyArray<JsonValue>
  | { readonly [key: string]: JsonValue | undefined };

export const Project = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  description: Schema.String,
  createdAt: Schema.String,
});
export type Project = typeof Project.Type;

/** The issue statuses accepted on the wire, in UI order. */
export const issueStatuses = ["open", "in_progress", "done"] as const;

export const IssueStatus = Schema.Literals(issueStatuses);
export type IssueStatus = typeof IssueStatus.Type;

/** A project holds at most this many issues `in_progress` at once. */
export const wipLimit = 3;

/** The author name when the writer gives none. */
export const anonymousAuthor = "anonymous";

export const Issue = Schema.Struct({
  id: Schema.String,
  projectId: Schema.String,
  title: Schema.String,
  status: IssueStatus,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  /** The display name of the last writer, when the writer gave one. */
  updatedBy: Schema.optionalKey(Schema.String),
});
export type Issue = typeof Issue.Type;

export const Comment = Schema.Struct({
  id: Schema.String,
  issueId: Schema.String,
  author: Schema.String,
  body: Schema.String,
  createdAt: Schema.String,
});
export type Comment = typeof Comment.Type;

/** The browser's StreamDB collections, checked by the same Effect schemas as the server. */
export const issueTrackerState = createStateSchema({
  projects: { schema: Schema.toStandardSchemaV1(Project), type: "project", primaryKey: "id" },
  issues: { schema: Schema.toStandardSchemaV1(Issue), type: "issue", primaryKey: "id" },
  comments: { schema: Schema.toStandardSchemaV1(Comment), type: "comment", primaryKey: "id" },
});

/** Change operations `MaterializedState` understands. */
const ChangeHeaders = Schema.Struct({
  operation: Schema.Literals(["delete", "upsert"]),
  txid: Schema.optionalKey(Schema.String),
  timestamp: Schema.optionalKey(Schema.String),
  from: Schema.optionalKey(Schema.String),
  offset: Schema.optionalKey(Schema.String),
});

const changeEvent = <const Type extends string, Value extends Schema.Top>(
  type: Type,
  value: Value,
) =>
  Schema.Struct({
    type: Schema.Literal(type),
    key: Schema.String,
    value: Schema.optionalKey(value),
    old_value: Schema.optionalKey(value),
    headers: ChangeHeaders,
  });

/** One change event read from the public workspace stream. */
export const StateEvent = Schema.Union([
  changeEvent("project", Project),
  changeEvent("issue", Issue),
  changeEvent("comment", Comment),
]);
export type StateEvent = typeof StateEvent.Type;

/** Decode a stream read body. Unknown extension fields are ignored. */
export const decodeStateEvents = Schema.decodeUnknownResult(Schema.Array(StateEvent));

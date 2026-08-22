import { createStateSchema, type ChangeEvent } from "@durable-streams/state";
import { z } from "zod";

const projectSchema = z.object({
  id: z.string(),
  name: z.string(),
  description: z.string(),
  createdAt: z.string(),
});

/** The issue statuses accepted on the wire, in UI order. */
export const issueStatuses = ["open", "in_progress", "done"] as const;

const issueSchema = z.object({
  id: z.string(),
  projectId: z.string(),
  title: z.string(),
  status: z.enum(issueStatuses),
  createdAt: z.string(),
  updatedAt: z.string(),
});

const commentSchema = z.object({
  id: z.string(),
  issueId: z.string(),
  author: z.string(),
  body: z.string(),
  createdAt: z.string(),
});

export const issueTrackerState = createStateSchema({
  projects: {
    schema: projectSchema,
    type: "project",
    primaryKey: "id",
  },
  issues: {
    schema: issueSchema,
    type: "issue",
    primaryKey: "id",
  },
  comments: {
    schema: commentSchema,
    type: "comment",
    primaryKey: "id",
  },
});

/**
 * Mutation bodies carry a partial entity: absent fields take a server default,
 * present fields must already match the wire schema.
 */
export const projectInput = projectSchema.partial();
export const issueInput = issueSchema.partial();
export const commentInput = commentSchema.partial();

export type Project = z.infer<typeof projectSchema>;
export type IssueStatus = z.infer<typeof issueSchema>["status"];
export type Issue = z.infer<typeof issueSchema>;
export type Comment = z.infer<typeof commentSchema>;

export type EntityType = "project" | "issue" | "comment";
export type EntityByType = {
  project: Project;
  issue: Issue;
  comment: Comment;
};

export type StateEvent<T extends EntityType = EntityType> = ChangeEvent<EntityByType[T]>;

const entitySchemaByType = {
  project: projectSchema,
  issue: issueSchema,
  comment: commentSchema,
} as const satisfies Record<EntityType, z.ZodType>;

/** Change operations `MaterializedState` understands. */
const changeOperations = new Set<string>(["insert", "update", "delete", "upsert"]);

function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isOptionalString(value: unknown): value is string | undefined {
  return value === undefined || typeof value === "string";
}

export function isIssueStatus(value: unknown): value is IssueStatus {
  return typeof value === "string" && issueStatuses.some((status) => status === value);
}

function isEntityType(value: unknown): value is EntityType {
  return typeof value === "string" && Object.hasOwn(entitySchemaByType, value);
}

function isChangeHeaders(value: unknown): value is StateEvent["headers"] {
  return (
    isJsonObject(value) &&
    typeof value.operation === "string" &&
    changeOperations.has(value.operation) &&
    isOptionalString(value.txid) &&
    isOptionalString(value.timestamp) &&
    isOptionalString(value.from) &&
    isOptionalString(value.offset)
  );
}

/**
 * Structural check for one payload read back from a workspace stream: the
 * change-event envelope, plus the entity payload validated against the schema
 * for its declared `type`. The stream is public (`/streams/workspace/<id>`
 * accepts direct appends), so a read cannot assume its own writer produced the
 * event.
 */
export function isStateEvent(value: unknown): value is StateEvent {
  if (!isJsonObject(value)) return false;
  if (!isEntityType(value.type) || typeof value.key !== "string") return false;
  if (!isChangeHeaders(value.headers)) return false;

  const schema = entitySchemaByType[value.type];
  const matchesSchema = (entity: unknown): boolean =>
    entity === undefined || schema.safeParse(entity).success;
  return matchesSchema(value.value) && matchesSchema(value.old_value);
}

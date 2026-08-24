import { createStateSchema, type ChangeEvent } from "@durable-streams/state";
import { z } from "zod";

export const projectSchema = z
  .object({
    id: z.string(),
    name: z.string(),
    description: z.string(),
    createdAt: z.string(),
  })
  .catchall(z.json());

/** The issue statuses accepted on the wire, in UI order. */
export const issueStatuses = ["open", "in_progress", "done"] as const;

export const issueStatusSchema = z.enum(issueStatuses);

export const issueSchema = z
  .object({
    id: z.string(),
    projectId: z.string(),
    title: z.string(),
    status: issueStatusSchema,
    createdAt: z.string(),
    updatedAt: z.string(),
  })
  .catchall(z.json());

export const commentSchema = z
  .object({
    id: z.string(),
    issueId: z.string(),
    author: z.string(),
    body: z.string(),
    createdAt: z.string(),
  })
  .catchall(z.json());

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

/** Change operations `MaterializedState` understands. */
const changeOperationSchema = z.enum(["insert", "update", "delete", "upsert"]);

const changeHeadersSchema = z
  .object({
    operation: changeOperationSchema,
    txid: z.string().optional(),
    timestamp: z.string().optional(),
    from: z.string().optional(),
    offset: z.string().optional(),
  })
  .catchall(z.json());

const projectEventSchema = z
  .object({
    type: z.literal("project"),
    key: z.string(),
    value: projectSchema.optional(),
    old_value: projectSchema.optional(),
    headers: changeHeadersSchema,
  })
  .catchall(z.json());

const issueEventSchema = z
  .object({
    type: z.literal("issue"),
    key: z.string(),
    value: issueSchema.optional(),
    old_value: issueSchema.optional(),
    headers: changeHeadersSchema,
  })
  .catchall(z.json());

const commentEventSchema = z
  .object({
    type: z.literal("comment"),
    key: z.string(),
    value: commentSchema.optional(),
    old_value: commentSchema.optional(),
    headers: changeHeadersSchema,
  })
  .catchall(z.json());

/** Shared decoder for values read from the public workspace stream. */
export const stateEventSchema: z.ZodType<StateEvent> = z.discriminatedUnion("type", [
  projectEventSchema,
  issueEventSchema,
  commentEventSchema,
]);
export const stateEventsSchema = z.array(stateEventSchema);

/** Shared HTTP response contracts used by the browser and smoke client. */
export const mutationResultSchema = z
  .object({
    awaitOffset: z.string(),
    txid: z.string(),
    project: z.object({ id: z.string() }).catchall(z.json()).optional(),
    issue: z.object({ id: z.string(), status: issueStatusSchema }).catchall(z.json()).optional(),
    comment: z.object({ id: z.string() }).catchall(z.json()).optional(),
  })
  .catchall(z.json());

export const workspaceResultSchema = z.object({ id: z.string() }).catchall(z.json());
export const errorResponseSchema = z.object({ error: z.string() }).catchall(z.json());
export const jsonObjectSchema = z.object({}).catchall(z.json());
export const txIdSchema = z.string().refine((value) => value.split("-").length === 5);

export type MutationResult = z.infer<typeof mutationResultSchema>;
export type MutationBody = z.infer<typeof jsonObjectSchema>;
export type TxId = z.infer<typeof txIdSchema>;

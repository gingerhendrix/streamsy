import { createStateSchema, type ChangeEvent } from "@durable-streams/state";
import { z } from "zod";

const projectSchema = z.object({
  id: z.string(),
  name: z.string(),
  description: z.string(),
  createdAt: z.string(),
});

const issueSchema = z.object({
  id: z.string(),
  projectId: z.string(),
  title: z.string(),
  status: z.enum(["open", "in_progress", "done"]),
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

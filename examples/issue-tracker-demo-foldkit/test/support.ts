import type {
  Comment,
  Issue,
  Project,
  StateEvent,
} from "@streamsy/issue-tracker-demo-contract/types";
import type { Stamp } from "../src/domain/intent.ts";

export const project: Project = {
  id: "proj_demo",
  name: "Demo",
  description: "",
  createdAt: "2026-10-25T10:00:00.000Z",
};

export const issue = (id: string, fields: Partial<Issue> = {}): Issue => ({
  id,
  projectId: project.id,
  title: `Issue ${id}`,
  status: "open",
  createdAt: "2026-10-25T10:00:00.000Z",
  updatedAt: "2026-10-25T10:00:00.000Z",
  ...fields,
});

export const comment = (id: string, issueId: string, createdAt: string): Comment => ({
  id,
  issueId,
  author: "anonymous",
  body: `Comment ${id}`,
  createdAt,
});

/** An upsert change as the State stream serves it. */
export const upsertProject = (value: Project, txid?: string): StateEvent => ({
  type: "project",
  key: value.id,
  value,
  headers: txid === undefined ? { operation: "upsert" } : { operation: "upsert", txid },
});

export const upsertIssue = (value: Issue, txid?: string): StateEvent => ({
  type: "issue",
  key: value.id,
  value,
  headers: txid === undefined ? { operation: "upsert" } : { operation: "upsert", txid },
});

export const upsertComment = (value: Comment, txid?: string): StateEvent => ({
  type: "comment",
  key: value.id,
  value,
  headers: txid === undefined ? { operation: "upsert" } : { operation: "upsert", txid },
});

export const stamp = (txid: string): Stamp => ({
  entropy: "abcdef12-0000-4000-8000-000000000000",
  txid,
  at: "2026-10-25T12:00:00.000Z",
});

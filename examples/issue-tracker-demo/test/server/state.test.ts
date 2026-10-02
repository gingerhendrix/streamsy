import { describe, expect, test } from "bun:test";
import { Effect, Schema } from "effect";
import { CommentInput, IssueInput, ProjectInput } from "../../shared/api-schema.ts";
import {
  createComment,
  createIssue,
  newComment,
  newIssue,
  newProject,
  nextIssue,
  type Stamp,
  type WorkspaceState,
} from "../../server/state.ts";

const stamp: Stamp = {
  id: "fixed_1",
  now: "2026-09-26T00:00:00.000Z",
  txid: "7b0c4f5e-6c1e-4b8e-9a51-0d6f3c0a4e21",
};

const projectInput = Schema.decodeUnknownSync(ProjectInput);
const issueInput = Schema.decodeUnknownSync(IssueInput);
const commentInput = Schema.decodeUnknownSync(CommentInput);

describe("newProject", () => {
  test("fills server defaults for absent fields", () => {
    const project = newProject(projectInput({}), stamp);
    expect(project.id).toBe(stamp.id);
    expect(project.name).toBe("Untitled project");
    expect(project.description).toBe("");
    expect(project.createdAt).toBe(stamp.now);
  });

  test("trims the supplied name and description", () => {
    const project = newProject(projectInput({ name: "  Streamsy  ", description: " hi " }), stamp);
    expect(project.name).toBe("Streamsy");
    expect(project.description).toBe("hi");
  });
});

describe("newIssue", () => {
  test("defaults status to open and updatedAt to createdAt", () => {
    const issue = newIssue(issueInput({ projectId: "proj_1", title: " Ship " }), stamp);
    expect(issue.status).toBe("open");
    expect(issue.title).toBe("Ship");
    expect(issue.projectId).toBe("proj_1");
    expect(issue.updatedAt).toBe(issue.createdAt);
  });

  test("keeps a supplied status", () => {
    expect(newIssue(issueInput({ status: "done" }), stamp).status).toBe("done");
  });
});

describe("nextIssue", () => {
  const previous = newIssue(issueInput({ projectId: "proj_1", title: "Ship" }), stamp);
  const later = { ...stamp, now: "2026-09-27T00:00:00.000Z" };

  test("keeps the previous title and status when the patch omits them", () => {
    const next = nextIssue(previous, issueInput({}), later);
    expect(next.title).toBe(previous.title);
    expect(next.status).toBe(previous.status);
    expect(next.updatedAt).toBe(later.now);
  });

  test("applies a trimmed title and a new status", () => {
    const next = nextIssue(previous, issueInput({ title: " Shipped ", status: "done" }), later);
    expect(next.title).toBe("Shipped");
    expect(next.status).toBe("done");
  });
});

describe("newComment", () => {
  test("falls back to the default author for a blank one", () => {
    const comment = newComment(commentInput({ author: "   ", body: " hi " }), stamp);
    expect(comment.author).toBe("you");
    expect(comment.body).toBe("hi");
    expect(comment.issueId).toBe("");
  });
});

describe("mutation attempts", () => {
  const project = newProject(projectInput({ id: "proj_1", name: "P" }), stamp);
  const issue = newIssue(issueInput({ id: "issue_1", projectId: project.id }), stamp);
  const state: WorkspaceState = {
    getProject: (projectId) => (projectId === project.id ? project : undefined),
    getIssue: (issueId) => (issueId === issue.id ? issue : undefined),
    hasProjects: () => true,
  };

  test("an issue under a known project becomes one upsert with the stamp's txid", () => {
    const { change, value } = Effect.runSync(createIssue({ projectId: project.id }, stamp)(state));
    expect(value.projectId).toBe(project.id);
    expect(change).toMatchObject({
      _tag: "Upsert",
      type: "issue",
      value: { id: stamp.id },
      headers: { timestamp: stamp.now, txid: stamp.txid },
    });
  });

  test("a comment on an unknown issue names the issueId field", () => {
    const failure = Effect.runSync(Effect.flip(createComment({ issueId: "nope" }, stamp)(state)));
    expect(failure).toMatchObject({ _tag: "UnknownReference", field: "issueId", id: "nope" });
  });
});

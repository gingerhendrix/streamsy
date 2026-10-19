import { describe, expect, test } from "vitest";
import { Effect, Schema } from "effect";
import {
  CommentInput,
  IssueInput,
  ProjectInput,
} from "@streamsy/issue-tracker-demo-contract/api-schema";
import type { Issue } from "@streamsy/issue-tracker-demo-contract/state-schema";
import {
  createComment,
  createIssue,
  newComment,
  newIssue,
  newProject,
  nextIssue,
  updateIssue,
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

  test("takes its time from the stamp, not the body", () => {
    const project = newProject(projectInput({ createdAt: "1999-01-01T00:00:00.000Z" }), stamp);
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

  test("stamps both times and ignores times in the body", () => {
    const issue = newIssue(
      issueInput({ createdAt: "1999-01-01T00:00:00.000Z", updatedAt: "1999-01-02T00:00:00.000Z" }),
      stamp,
    );

    expect(issue.createdAt).toBe(stamp.now);
    expect(issue.updatedAt).toBe(stamp.now);
  });

  test("names the writer, and leaves out a blank name", () => {
    expect(newIssue(issueInput({ updatedBy: " Ann " }), stamp).updatedBy).toBe("Ann");
    expect(newIssue(issueInput({ updatedBy: "  " }), stamp)).not.toHaveProperty("updatedBy");
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

  test("names the latest writer only", () => {
    const byAnn = nextIssue(previous, issueInput({ updatedBy: "Ann" }), later);
    expect(byAnn.updatedBy).toBe("Ann");
    expect(nextIssue(byAnn, issueInput({ status: "done" }), later)).not.toHaveProperty("updatedBy");
  });

  test("keeps createdAt and ignores updatedAt in the body", () => {
    const next = nextIssue(previous, issueInput({ updatedAt: "1999-01-01T00:00:00.000Z" }), later);
    expect(next.createdAt).toBe(previous.createdAt);
    expect(next.updatedAt).toBe(later.now);
  });
});

describe("newComment", () => {
  test("falls back to the anonymous author for a blank one", () => {
    const comment = newComment(commentInput({ author: "   ", body: " hi " }), stamp);
    expect(comment.author).toBe("anonymous");
    expect(comment.body).toBe("hi");
    expect(comment.issueId).toBe("");
  });
});

describe("mutation attempts", () => {
  const project = newProject(projectInput({ id: "proj_1", name: "P" }), stamp);
  const issue = newIssue(issueInput({ id: "issue_1", projectId: project.id }), stamp);

  /** A workspace whose project has `inProgress` issues in progress. */
  const stateWith = (inProgress: number, current: Issue = issue): WorkspaceState => ({
    getProject: (projectId) => (projectId === project.id ? project : undefined),
    getIssue: (issueId) => (issueId === current.id ? current : undefined),
    hasProjects: () => true,
    countInProgress: (projectId) => (projectId === project.id ? inProgress : 0),
  });

  const state = stateWith(0);

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

  describe("the work-in-progress limit", () => {
    const start = updateIssue(issue.id, { status: "in_progress" }, stamp);

    test("an issue may start while the project is below the limit", () => {
      const { value } = Effect.runSync(start(stateWith(2)));
      expect(value.status).toBe("in_progress");
    });

    test("an issue may not start when the project is at the limit", () => {
      expect(Effect.runSync(Effect.flip(start(stateWith(3))))).toMatchObject({
        _tag: "TooManyInProgress",
        projectId: project.id,
        limit: 3,
      });
    });

    test("an issue already in progress may change at the limit", () => {
      const started: Issue = { ...issue, status: "in_progress" };
      const rename = updateIssue(issue.id, { title: "Renamed" }, stamp);
      const { value } = Effect.runSync(rename(stateWith(3, started)));
      expect(value).toMatchObject({ title: "Renamed", status: "in_progress" });
    });

    test("a new issue may not start in progress at the limit", () => {
      const create = createIssue({ projectId: project.id, status: "in_progress" }, stamp);
      expect(Effect.runSync(Effect.flip(create(stateWith(3))))).toMatchObject({
        _tag: "TooManyInProgress",
      });
    });
  });
});

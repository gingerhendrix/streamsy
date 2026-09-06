import { describe, expect, test } from "bun:test";
import { commentInput, issueInput, projectInput } from "../shared/state-schema.ts";
import { newComment, newIssue, newProject, nextIssue } from "./state.ts";

describe("newProject", () => {
  test("fills server defaults for absent fields", () => {
    const project = newProject(projectInput.parse({}));
    expect(project.id).toStartWith("proj_");
    expect(project.name).toBe("Untitled project");
    expect(project.description).toBe("");
    expect(project.createdAt).not.toBe("");
  });

  test("trims the supplied name and description", () => {
    const project = newProject(projectInput.parse({ name: "  Streamsy  ", description: " hi " }));
    expect(project.name).toBe("Streamsy");
    expect(project.description).toBe("hi");
  });
});

describe("newIssue", () => {
  test("defaults status to open and updatedAt to createdAt", () => {
    const issue = newIssue(issueInput.parse({ projectId: "proj_1", title: " Ship " }));
    expect(issue.status).toBe("open");
    expect(issue.title).toBe("Ship");
    expect(issue.projectId).toBe("proj_1");
    expect(issue.updatedAt).toBe(issue.createdAt);
  });

  test("keeps a supplied status", () => {
    expect(newIssue(issueInput.parse({ status: "done" })).status).toBe("done");
  });

  test("rejects a status outside the schema before it reaches the builder", () => {
    expect(issueInput.safeParse({ status: "archived" }).success).toBe(false);
  });
});

describe("nextIssue", () => {
  const previous = newIssue(issueInput.parse({ projectId: "proj_1", title: "Ship" }));

  test("keeps the previous title and status when the patch omits them", () => {
    const next = nextIssue(previous, issueInput.parse({}));
    expect(next.title).toBe(previous.title);
    expect(next.status).toBe(previous.status);
  });

  test("applies a trimmed title and a new status", () => {
    const next = nextIssue(previous, issueInput.parse({ title: " Shipped ", status: "done" }));
    expect(next.title).toBe("Shipped");
    expect(next.status).toBe("done");
  });
});

describe("newComment", () => {
  test("falls back to the default author for a blank one", () => {
    const comment = newComment(commentInput.parse({ author: "   ", body: " hi " }));
    expect(comment.author).toBe("you");
    expect(comment.body).toBe("hi");
    expect(comment.issueId).toBe("");
  });
});

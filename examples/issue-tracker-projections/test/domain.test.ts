import { describe, expect, test } from "vitest";
import {
  assertIdentifier,
  boardRow,
  evolveIssue,
  streamNames,
  type IssueDetail,
  type IssueEvent,
} from "../shared/domain.ts";

const created: IssueEvent = {
  type: "IssueCreated",
  commandId: "c1",
  at: "2026-08-06T10:00:00.000Z",
  issueId: "issue-1",
  issueKey: "SHIP-100",
  projectId: "launch",
  title: "Ship it",
  status: "backlog",
  priority: "medium",
  creatorId: "ada",
};

function fold(events: readonly IssueEvent[]): IssueDetail | undefined {
  let state: IssueDetail | undefined;
  for (const event of events) state = evolveIssue(state, event);
  return state;
}

describe("issue domain", () => {
  test("folds a complete edit history", () => {
    const detail = fold([
      created,
      {
        type: "IssueRenamed",
        commandId: "c2",
        at: "2026-08-06T10:01:00.000Z",
        title: "Ship it well",
      },
      {
        type: "IssuePriorityChanged",
        commandId: "c3",
        at: "2026-08-06T10:02:00.000Z",
        priority: "urgent",
      },
      {
        type: "IssueAssigned",
        commandId: "c4",
        at: "2026-08-06T10:03:00.000Z",
        assigneeId: "grace",
      },
      {
        type: "CommentAdded",
        commandId: "c5",
        at: "2026-08-06T10:04:00.000Z",
        commentId: "cm-1",
        authorId: "lin",
        body: "Looks right",
      },
      {
        type: "IssueStatusChanged",
        commandId: "c6",
        at: "2026-08-06T10:05:00.000Z",
        status: "done",
      },
    ]);
    expect(detail).toMatchObject({
      title: "Ship it well",
      priority: "urgent",
      assigneeId: "grace",
      status: "done",
      updatedAt: "2026-08-06T10:05:00.000Z",
    });
    expect(detail?.comments).toHaveLength(1);
  });

  test("creation is idempotent and later events require it", () => {
    const twice = fold([created, created]);
    expect(twice?.createdAt).toBe(created.at);
    expect(() =>
      evolveIssue(undefined, {
        type: "IssueRenamed",
        commandId: "c2",
        at: created.at,
        title: "Too early",
      }),
    ).toThrow(TypeError);
  });

  test("a repeated comment id does not duplicate the comment", () => {
    const comment: IssueEvent = {
      type: "CommentAdded",
      commandId: "c5",
      at: "2026-08-06T10:04:00.000Z",
      commentId: "cm-1",
      authorId: "lin",
      body: "Looks right",
    };
    expect(fold([created, comment, comment])?.comments).toHaveLength(1);
  });

  test("board rows project the fields the board needs", () => {
    const detail = fold([created])!;
    expect(boardRow(detail)).toEqual({
      issueId: "issue-1",
      issueKey: "SHIP-100",
      title: "Ship it",
      status: "backlog",
      priority: "medium",
      assigneeId: null,
      commentCount: 0,
      updatedAt: created.at,
    });
  });

  test("stream names are stable and workspace scoped", () => {
    expect(streamNames.issueEvents("main", "issue-1")).toBe(
      "workspaces/main/issues/issue-1/events",
    );
    expect(streamNames.board("main", "launch")).toBe("workspaces/main/projects/launch/board");
  });

  test("identifiers that would break stream paths are rejected", () => {
    expect(() => assertIdentifier("a/b", "issueId")).toThrow(TypeError);
    expect(() => assertIdentifier("", "issueId")).toThrow(TypeError);
    expect(assertIdentifier("issue-1", "issueId")).toBe("issue-1");
  });
});

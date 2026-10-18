import { expect, test } from "vitest";
import {
  applyEvent,
  countInProgress,
  emptyView,
  findIssue,
  hasProject,
} from "../../server/command-view.ts";
import { mainWorkspaceSeed } from "../../server/decide.ts";
import { WorkspaceEvent } from "../../server/events.ts";
import { fixtureTxid, later, stamp } from "../support.ts";

const seeded = mainWorkspaceSeed(stamp("seed")).reduce(applyEvent, emptyView);

test("the seed folds into the base app's main workspace", () => {
  expect(seeded.projectIds).toEqual(["proj_streamsy", "proj_docs"]);
  expect(Object.keys(seeded.issues)).toHaveLength(5);
  expect(countInProgress(seeded, "proj_streamsy")).toBe(2);
  expect(countInProgress(seeded, "proj_docs")).toBe(0);
});

test("a status change moves an issue in and out of the in-progress count", () => {
  const { IssueStatusChanged } = WorkspaceEvent.cases;
  const txid = fixtureTxid();

  const started = applyEvent(
    seeded,
    IssueStatusChanged.make({ issueId: "issue_names", status: "in_progress", at: later, txid }),
  );

  expect(countInProgress(started, "proj_streamsy")).toBe(3);

  const finished = applyEvent(
    started,
    IssueStatusChanged.make({ issueId: "issue_filters", status: "done", at: later, txid }),
  );

  expect(countInProgress(finished, "proj_streamsy")).toBe(2);
  expect(findIssue(finished, "issue_filters")?.updatedAt).toBe(later);
});

test("a comment changes nothing that a rule reads", () => {
  const { CommentAdded } = WorkspaceEvent.cases;

  const comment = CommentAdded.make({
    commentId: "comment_b",
    issueId: "issue_names",
    author: "Ann",
    body: "Hi",
    at: later,
    txid: fixtureTxid(),
  });

  expect(applyEvent(seeded, comment)).toBe(seeded);
  expect(hasProject(seeded, "proj_docs")).toBe(true);
  expect(hasProject(seeded, "proj_nope")).toBe(false);
});

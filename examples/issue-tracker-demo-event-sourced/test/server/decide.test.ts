import { describe, expect, test } from "bun:test";
import { Effect, Exit } from "effect";
import { applyEvent, emptyView } from "../../server/command-view.ts";
import {
  createComment,
  createIssue,
  createProject,
  mainWorkspaceSeed,
  updateIssue,
} from "../../server/decide.ts";
import { WorkspaceEvent } from "../../server/events.ts";
import { createdAt, later, stamp } from "../support.ts";

const view = mainWorkspaceSeed(stamp("seed")).reduce(applyEvent, emptyView);

const decide = <A, E>(effect: Effect.Effect<A, E>) => Effect.runSync(Effect.exit(effect));

const failure = <A, E>(effect: Effect.Effect<A, E>) => {
  const exit = decide(effect);
  if (Exit.isSuccess(exit)) throw new Error("expected the decision to fail");
  return exit.cause;
};

describe("createProject", () => {
  test("is one ProjectCreated event with server defaults and the stamp's time and txid", () => {
    const at = stamp("proj_x");
    const { events, value } = Effect.runSync(createProject({ name: "  Docs " }, at)());
    expect(events).toEqual([
      expect.objectContaining({
        _tag: "ProjectCreated",
        projectId: "proj_x",
        name: "Docs",
        description: "",
        at: createdAt,
        txid: at.txid,
      }),
    ]);
    expect(value).toEqual({ id: "proj_x", name: "Docs", description: "", createdAt });
  });
});

describe("createIssue", () => {
  test("is one IssueCreated event, open by default, and answers the row", () => {
    const { events, value } = Effect.runSync(
      createIssue(
        { projectId: "proj_docs", title: " Write ", updatedBy: " Ann " },
        stamp("i"),
      )(view),
    );
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ _tag: "IssueCreated", status: "open", updatedBy: "Ann" });
    expect(value).toMatchObject({ id: "i", title: "Write", createdAt, updatedAt: createdAt });
  });

  test("an unknown project fails with UnknownReference", () => {
    expect(String(failure(createIssue({ projectId: "nope" }, stamp("i"))(view)))).toContain(
      "UnknownReference",
    );
  });

  test("a third in-progress issue is allowed and a fourth is refused", () => {
    const third = Effect.runSync(
      createIssue({ projectId: "proj_streamsy", status: "in_progress" }, stamp("i3"))(view),
    );
    const full = third.events.reduce(applyEvent, view);
    expect(
      String(
        failure(
          createIssue({ projectId: "proj_streamsy", status: "in_progress" }, stamp("i4"))(full),
        ),
      ),
    ).toContain("TooManyInProgress");
  });
});

describe("updateIssue", () => {
  test("a title and a status give two events in one decision, rename first", () => {
    const at = stamp("ignored", later);
    const { events, value } = Effect.runSync(
      updateIssue("issue_names", { title: "Names", status: "done", updatedBy: "Bo" }, at)(view),
    );
    expect(events).toEqual([
      expect.objectContaining({ _tag: "IssueRenamed", title: "Names" }),
      expect.objectContaining({ _tag: "IssueStatusChanged", status: "done" }),
    ]);
    expect(events.every((event) => event.txid === at.txid && event.at === later)).toBe(true);
    expect(value).toMatchObject({
      id: "issue_names",
      title: "Names",
      status: "done",
      createdAt,
      updatedAt: later,
      updatedBy: "Bo",
    });
  });

  test("a missing issue fails with IssueNotFound", () => {
    expect(String(failure(updateIssue("nope", { status: "done" }, stamp("x"))(view)))).toContain(
      "IssueNotFound",
    );
  });

  test("a patch with no title and no status fails with NothingToChange", () => {
    expect(
      String(failure(updateIssue("issue_names", { updatedBy: "Ann" }, stamp("x"))(view))),
    ).toContain("NothingToChange");
  });

  test("the WIP rule refuses a fourth issue and counts a moved issue once", () => {
    const third = Effect.runSync(
      updateIssue("issue_names", { status: "in_progress" }, stamp("x"))(view),
    );
    const full = third.events.reduce(applyEvent, view);
    expect(
      String(failure(updateIssue("issue_bootstrap", { status: "in_progress" }, stamp("y"))(full))),
    ).toContain("TooManyInProgress");
    // An issue that is already in progress can be renamed or set to in progress again.
    const again = decide(
      updateIssue("issue_optimistic", { title: "Again", status: "in_progress" }, stamp("z"))(full),
    );
    expect(Exit.isSuccess(again)).toBe(true);
  });

  test("a refused status change appends no rename either", () => {
    const third = Effect.runSync(
      updateIssue("issue_names", { status: "in_progress" }, stamp("x"))(view),
    );
    const full = third.events.reduce(applyEvent, view);
    const exit = decide(
      updateIssue("issue_bootstrap", { title: "Lost", status: "in_progress" }, stamp("y"))(full),
    );
    expect(Exit.isFailure(exit)).toBe(true);
  });
});

describe("createComment", () => {
  test("is one CommentAdded event with the anonymous author for a blank name", () => {
    const { events, value } = Effect.runSync(
      createComment({ issueId: "issue_names", author: " ", body: " Hi " }, stamp("c"))(view),
    );
    expect(events[0]).toMatchObject({ _tag: "CommentAdded", author: "anonymous", body: "Hi" });
    expect(value).toEqual({
      id: "c",
      issueId: "issue_names",
      author: "anonymous",
      body: "Hi",
      createdAt,
    });
  });

  test("an unknown issue fails with UnknownReference", () => {
    expect(String(failure(createComment({ issueId: "nope" }, stamp("c"))(view)))).toContain(
      "UnknownReference",
    );
  });
});

test("the seed is the base app's main workspace as events", () => {
  const events = mainWorkspaceSeed(stamp("seed"));
  const count = (tag: "ProjectCreated" | "IssueCreated" | "CommentAdded") =>
    events.filter(WorkspaceEvent.isAnyOf([tag])).length;
  expect([count("ProjectCreated"), count("IssueCreated"), count("CommentAdded")]).toEqual([
    2, 5, 1,
  ]);
});

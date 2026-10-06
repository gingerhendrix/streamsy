import { Projection } from "@streamsy/projection";
import { it } from "@streamsy/effect-test";
import { expect, test } from "bun:test";
import { Effect } from "effect";
import { mainWorkspaceSeed } from "../../server/decide.ts";
import { WorkspaceEvent } from "../../server/events.ts";
import { project, readModel } from "../../server/read-model.ts";
import { WorkspaceEvents, Workspaces } from "../../server/streams.ts";
import {
  createdAt,
  fixtureTxid,
  later,
  MemoryHost,
  stamp,
  stateChanges,
  withLayer,
} from "../support.ts";

const { IssueCreated, IssueRenamed, IssueStatusChanged } = WorkspaceEvent.cases;

const workspaceId = "rows-test";

const txid = fixtureTxid();

const editTxid = fixtureTxid();

const created = IssueCreated.make({
  issueId: "issue_a",
  projectId: "proj_streamsy",
  title: "First",
  status: "open",
  at: createdAt,
  txid,
});

test("a rename upserts the whole row with the event's time and txid headers", () => {
  const [rows] = project({}, created);

  const [next, changes] = project(
    rows,
    IssueRenamed.make({ issueId: "issue_a", title: "Second", at: later, txid: editTxid }),
  );

  expect(next["issue_a"]?.title).toBe("Second");
  expect(changes).toEqual([
    expect.objectContaining({
      type: "issue",
      value: {
        id: "issue_a",
        projectId: "proj_streamsy",
        title: "Second",
        status: "open",
        createdAt,
        updatedAt: later,
      },
      headers: { timestamp: later, txid: editTxid },
    }),
  ]);
});

test("an edit of an unknown issue writes nothing", () => {
  const moved = IssueStatusChanged.make({ issueId: "nope", status: "done", at: later, txid });
  expect(project({}, moved)).toEqual([{}, []]);
});

it.live("the projection writes the base app's rows to the State stream, once", () =>
  Effect.gen(function* () {
    const { first, again } = yield* Effect.gen(function* () {
      const events = yield* WorkspaceEvents;
      yield* events.create({ workspaceId });
      yield* (yield* Workspaces).create({ workspaceId });
      yield* events.append({ workspaceId }, [...mainWorkspaceSeed(stamp("seed")), created]);
      yield* events.append({ workspaceId }, [
        IssueRenamed.make({ issueId: "issue_a", title: "Second", at: later, txid: editTxid }),
        IssueStatusChanged.make({ issueId: "issue_a", status: "done", at: later, txid: editTxid }),
      ]);
      const rows = readModel.at({ workspaceId });
      yield* Projection.run(rows);
      const once = yield* stateChanges(workspaceId);
      // A second run starts from the checkpoint and writes nothing.
      yield* Projection.run(rows);

      return { first: once, again: yield* stateChanges(workspaceId) };
    }).pipe(withLayer(MemoryHost));

    expect(first.map((change) => change.type)).toEqual([
      "project",
      "project",
      "issue",
      "issue",
      "issue",
      "issue",
      "issue",
      "comment",
      "issue",
      "issue",
      "issue",
    ]);
    expect(first.every((change) => change.headers.operation === "upsert")).toBe(true);
    const edits = first.filter((change) => change.headers.txid === editTxid);
    expect(edits.map((change) => ("value" in change ? change.value : undefined))).toEqual([
      expect.objectContaining({ id: "issue_a", title: "Second", status: "open" }),
      expect.objectContaining({ id: "issue_a", title: "Second", status: "done", updatedAt: later }),
    ]);
    expect(edits.every((change) => change.headers.timestamp === later)).toBe(true);
    expect(again).toHaveLength(first.length);
  }),
);

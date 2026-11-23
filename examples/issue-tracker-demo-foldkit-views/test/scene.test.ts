import {
  Command,
  Subscription,
  change,
  click,
  expect,
  given,
  label,
  role,
  scene,
  selector,
  submit,
  text,
  type,
} from "foldkit/scene";
import { describe, test } from "vitest";
import { GenerateStamp, SendWrite, WaitBeforeNoticeDismissal } from "../src/command.ts";
import { Intent } from "../src/domain/intent.ts";
import { Message } from "../src/message.ts";
import type { Model } from "../src/model.ts";
import { init, update } from "../src/update.ts";
import { view } from "../src/view/view.ts";
import { issue, project, stamp, upsertIssue, upsertProject } from "./support.ts";

// Scene tests drive the page through the view: they click, type and submit,
// resolve the Commands that `update` returns, and check what the page shows.

const loading: Model = init({ workspaceId: "main", displayName: "Ada" }).model;

const seeded = [
  upsertProject(project),
  upsertIssue(issue("issue_a", { title: "Fix the reader" })),
  upsertIssue(issue("issue_b", { title: "Ship it", status: "done" })),
];

/** A page that has read the seeded workspace. */
const live: Model = update(
  loading,
  Message.ReceivedStateBatch({ changes: seeded, isUpToDate: true }),
).model;

const pendingRow = selector(".issue-row.pending");

const wipNotice = "This project already has 3 issues in progress";

describe("the workspace stream", () => {
  test("the page waits for an up-to-date batch, then shows the rows", () => {
    scene(
      { update, view },
      given(loading),
      expect(text("Loading durable stream database…")).toExist(),
      Subscription.emit(Message.ReceivedStateBatch({ changes: seeded, isUpToDate: true })),
      expect(text("Loading durable stream database…")).toBeAbsent(),
      expect(role("heading", { name: "Demo" })).toExist(),
      expect(role("button", { name: "Fix the reader" })).toExist(),
      expect(role("button", { name: "Ship it" })).toExist(),
    );
  });

  test("a lost connection shows the Offline badge until a read succeeds", () => {
    scene(
      { update, view },
      given(live),
      Subscription.emit(Message.ChangedConnection({ isOnline: false })),
      expect(text("Offline")).toExist(),
      Subscription.emit(Message.ChangedConnection({ isOnline: true })),
      expect(text("Offline")).toBeAbsent(),
    );
  });
});

describe("optimistic writes (A4)", () => {
  test("a new issue shows faded at once and turns solid when its txid arrives", () => {
    const intent = Intent.CreateIssue({ projectId: project.id, title: "Write tests" });

    scene(
      { update, view },
      given(live),
      type(label("New issue title"), "Write tests"),
      submit(selector(".new-issue-form")),
      Command.expectExact(GenerateStamp({ intent })),
      Command.resolve(
        GenerateStamp,
        Message.CompletedGenerateStamp({ intent, stamp: stamp("tx-1") }),
      ),
      expect(role("button", { name: "Write tests" })).toExist(),
      expect(pendingRow).toExist(),
      expect(pendingRow).toHaveAttr("aria-busy", "true"),
      Command.resolve(SendWrite, Message.SucceededSendWrite({ txid: "tx-1" })),
      // The server said yes, but only the stream confirms the write.
      expect(pendingRow).toExist(),
      Subscription.emit(
        Message.ReceivedStateBatch({
          changes: [upsertIssue(issue("issue_abcdef12", { title: "Write tests" }), "tx-1")],
          isUpToDate: true,
        }),
      ),
      expect(pendingRow).toBeAbsent(),
      expect(role("button", { name: "Write tests" })).toExist(),
    );
  });

  test("a refused status change rolls back and shows the server's message", () => {
    const intent = Intent.UpdateIssue({ issueId: "issue_a", patch: { status: "in_progress" } });

    scene(
      { update, view },
      given(live),
      change(label("Status for Fix the reader"), "in_progress"),
      Command.resolve(
        GenerateStamp,
        Message.CompletedGenerateStamp({ intent, stamp: stamp("tx-2") }),
      ),
      expect(pendingRow).toExist(),
      expect(selector(".issue-row.pending .status-pill")).toHaveText("in progress"),
      Command.resolve(SendWrite, Message.FailedSendWrite({ txid: "tx-2", error: wipNotice })),
      expect(pendingRow).toBeAbsent(),
      expect(text(wipNotice, { exact: false })).toExist(),
      expect(label("Status for Fix the reader")).toHaveValue("open"),
      Command.expectExact(WaitBeforeNoticeDismissal({ noticeId: 0 })),
      Command.resolve(
        WaitBeforeNoticeDismissal,
        Message.CompletedWaitBeforeNoticeDismissal({ noticeId: 0 }),
      ),
      expect(text(wipNotice, { exact: false })).toBeAbsent(),
    );
  });
});

describe("the status filter (A2)", () => {
  test("the chips count each status, and a filter with no rows says so", () => {
    scene(
      { update, view },
      given(live),
      expect(role("button", { name: /^All\s*2$/ })).toHaveAttr("aria-pressed", "true"),
      expect(role("button", { name: /^Open\s*1$/ })).toExist(),
      expect(role("button", { name: /^In progress\s*0\/3$/ })).toExist(),
      expect(role("button", { name: /^Done\s*1$/ })).toExist(),
      click(role("button", { name: /^Done/ })),
      expect(role("button", { name: "Ship it" })).toExist(),
      expect(role("button", { name: "Fix the reader" })).toBeAbsent(),
      click(role("button", { name: /^In progress/ })),
      expect(text("No issues with this status.", { exact: false })).toExist(),
    );
  });
});

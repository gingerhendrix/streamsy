import { Option, Record } from "effect";
import { Command, given, message, model, story } from "foldkit/story";
import { describe, expect, test } from "vitest";
import {
  FocusTitleDraft,
  GenerateStamp,
  SaveDisplayName,
  SendWrite,
  WaitBeforeNoticeDismissal,
} from "../src/command.ts";
import { Intent } from "../src/domain/intent.ts";
import { Message } from "../src/message.ts";
import type { Model } from "../src/model.ts";
import { init, update } from "../src/update.ts";
import { issue, project, stamp, upsertIssue, upsertProject } from "./support.ts";

const seeded = [upsertProject(project), upsertIssue(issue("issue_a"))];

/** A page that has read the seeded workspace. */
const live: Model = update(
  init({ workspaceId: "main", displayName: "Ada" }).model,
  Message.ReceivedStateBatch({
    changes: seeded,
    isUpToDate: true,
  }),
).model;

describe("the workspace stream", () => {
  test("the page stays loading until a batch is up to date", () => {
    story(
      update,
      given(init({ workspaceId: "main", displayName: "" }).model),
      message(Message.ReceivedStateBatch({ changes: seeded, isUpToDate: false })),
      model((current) => {
        expect(current.sync._tag).toBe("Loading");
        expect(Record.keys(current.rows.issues)).toEqual(["issue_a"]);
      }),
      message(Message.ReceivedStateBatch({ changes: [], isUpToDate: true })),
      model((current) => expect(current.sync._tag).toBe("Live")),
    );
  });

  test("a failed read shows its error", () => {
    story(
      update,
      given(live),
      message(Message.FailedReadWorkspace({ error: "Unable to open the workspace stream" })),
      model((current) =>
        expect(current.sync).toEqual({
          _tag: "Failed",
          error: "Unable to open the workspace stream",
        }),
      ),
    );
  });
});

describe("optimistic writes (A4)", () => {
  test("a new issue is pending until its txid arrives on the stream", () => {
    story(
      update,
      given(live),
      message(Message.UpdatedIssueTitle({ value: "  Write tests  " })),
      message(Message.SubmittedIssue()),
      model((current) => expect(current.issueTitle).toBe("")),
      Command.expectExact(
        GenerateStamp({
          intent: Intent.CreateIssue({ projectId: project.id, title: "Write tests" }),
        }),
      ),
      Command.resolve(
        GenerateStamp,
        Message.CompletedGenerateStamp({
          intent: Intent.CreateIssue({ projectId: project.id, title: "Write tests" }),
          stamp: stamp("tx-1"),
        }),
      ),
      model((current) => {
        expect(current.pending).toHaveLength(1);
        expect(current.pending[0]?.write).toMatchObject({
          issue: { title: "Write tests", updatedBy: "Ada" },
        });
      }),
      Command.expectHas(SendWrite),
      Command.resolve(SendWrite, Message.SucceededSendWrite({ txid: "tx-1" })),
      // The server accepted the write, but the stream has not shown it yet.
      model((current) => expect(current.pending).toHaveLength(1)),
      message(
        Message.ReceivedStateBatch({
          changes: [upsertIssue(issue("issue_abcdef12", { title: "Write tests" }), "tx-1")],
          isUpToDate: true,
        }),
      ),
      model((current) => {
        expect(current.pending).toHaveLength(0);
        expect(current.rows.issues["issue_abcdef12"]?.title).toBe("Write tests");
      }),
    );
  });

  test("a refused write rolls back and shows the server's message for a while", () => {
    const intent = Intent.UpdateIssue({ issueId: "issue_a", patch: { status: "in_progress" } });

    story(
      update,
      given(live),
      message(Message.SelectedIssueStatus({ issueId: "issue_a", value: "in_progress" })),
      Command.resolve(
        GenerateStamp,
        Message.CompletedGenerateStamp({ intent, stamp: stamp("tx-2") }),
      ),
      model((current) => expect(current.pending).toHaveLength(1)),
      Command.resolve(
        SendWrite,
        Message.FailedSendWrite({
          txid: "tx-2",
          error: "This project already has 3 issues in progress",
        }),
      ),
      model((current) => {
        expect(current.pending).toHaveLength(0);
        expect(current.rows.issues["issue_a"]?.status).toBe("open");
        expect(current.notices).toEqual([
          { id: 0, message: "This project already has 3 issues in progress" },
        ]);
      }),
      Command.expectExact(WaitBeforeNoticeDismissal({ noticeId: 0 })),
      Command.resolve(
        WaitBeforeNoticeDismissal,
        Message.CompletedWaitBeforeNoticeDismissal({ noticeId: 0 }),
      ),
      model((current) => expect(current.notices).toEqual([])),
    );
  });

  test("a new project is selected at once", () => {
    const intent = Intent.CreateProject({ name: "New", description: "" });

    story(
      update,
      given(live),
      message(Message.UpdatedProjectName({ value: "New" })),
      message(Message.SubmittedProject()),
      Command.resolve(
        GenerateStamp,
        Message.CompletedGenerateStamp({ intent, stamp: stamp("tx-3") }),
      ),
      model((current) =>
        expect(current.maybeSelectedProjectId).toEqual(Option.some("proj_abcdef12")),
      ),
      Command.resolve(SendWrite, Message.SucceededSendWrite({ txid: "tx-3" })),
    );
  });

  test("empty input and unknown statuses send nothing", () => {
    story(
      update,
      given(live),
      message(Message.UpdatedIssueTitle({ value: "   " })),
      message(Message.SubmittedIssue()),
      message(Message.SubmittedProject()),
      message(Message.SubmittedComment({ issueId: "issue_a" })),
      message(Message.SelectedIssueStatus({ issueId: "issue_a", value: "blocked" })),
      Command.expectNone(),
    );
  });
});

describe("renaming an issue", () => {
  const rename = Intent.UpdateIssue({ issueId: "issue_a", patch: { title: "Renamed" } });

  test("Enter saves once; the blur that follows saves nothing", () => {
    story(
      update,
      given(live),
      message(Message.ClickedIssueTitle({ issueId: "issue_a" })),
      Command.expectExact(FocusTitleDraft({ issueId: "issue_a" })),
      Command.resolve(FocusTitleDraft, Message.CompletedFocusTitleDraft()),
      message(Message.UpdatedTitleDraft({ value: "Renamed" })),
      message(Message.PressedTitleDraftKey({ key: "Enter" })),
      model((current) => expect(current.maybeTitleEdit).toEqual(Option.none())),
      Command.expectExact(GenerateStamp({ intent: rename })),
      Command.resolveAll(
        [GenerateStamp, Message.CompletedGenerateStamp({ intent: rename, stamp: stamp("tx-4") })],
        [SendWrite, Message.SucceededSendWrite({ txid: "tx-4" })],
      ),
      // Closing the field removes it, and the browser sends one more blur.
      message(Message.BlurredTitleDraft()),
      Command.expectNone(),
    );

    story(
      update,
      given(live),
      message(Message.ClickedIssueTitle({ issueId: "issue_a" })),
      Command.resolve(FocusTitleDraft, Message.CompletedFocusTitleDraft()),
      message(Message.PressedTitleDraftKey({ key: "Enter" })),
      message(Message.BlurredTitleDraft()),
      // The title did not change, so there is nothing to send.
      Command.expectNone(),
    );
  });

  test("Escape closes the field and keeps the title", () => {
    story(
      update,
      given(live),
      message(Message.ClickedIssueTitle({ issueId: "issue_a" })),
      Command.resolve(FocusTitleDraft, Message.CompletedFocusTitleDraft()),
      message(Message.UpdatedTitleDraft({ value: "Renamed" })),
      message(Message.PressedTitleDraftKey({ key: "Escape" })),
      message(Message.BlurredTitleDraft()),
      Command.expectNone(),
      model((current) => expect(current.maybeTitleEdit).toEqual(Option.none())),
    );
  });
});

describe("display name", () => {
  test("a new name is kept in the model and saved", () => {
    story(
      update,
      given(live),
      message(Message.UpdatedDisplayName({ value: "Grace" })),
      model((current) => expect(current.displayName).toBe("Grace")),
      Command.expectExact(SaveDisplayName({ name: "Grace" })),
      Command.resolve(SaveDisplayName, Message.CompletedSaveDisplayName()),
    );
  });
});

import { Array, Option, Record, Schema } from "effect";
import type { Update } from "foldkit";
import { modifyFields } from "foldkit/struct";
import { IssueStatus } from "@streamsy/issue-tracker-demo-contract/state-schema";
import { Intent, toPendingWrite } from "./domain/intent.ts";
import {
  applyChanges,
  confirmedTxids,
  emptyRows,
  projectsOldestFirst,
  selectedProject,
  workspaceView,
  Write,
} from "./domain/workspace.ts";
import {
  CopyLink,
  CreateWorkspace,
  FocusTitleDraft,
  GenerateStamp,
  NavigateToWorkspace,
  SaveDisplayName,
  SendWrite,
  WaitBeforeCopiedReset,
  WaitBeforeNoticeDismissal,
} from "./command.ts";
import { Message } from "./message.ts";
import { Sync, type Flags, type Model } from "./model.ts";
import type { Resources } from "./resources.ts";

type UpdateReturn = Update.Return<Model, Message, Resources>;

export const init = (flags: Flags): UpdateReturn => ({
  model: {
    workspaceId: flags.workspaceId,
    displayName: flags.displayName,
    sync: Sync.Loading(),
    isOnline: true,
    rows: emptyRows,
    pending: [],
    maybeSelectedProjectId: Option.none(),
    filter: "all",
    projectName: "",
    projectDescription: "",
    issueTitle: "",
    commentDrafts: {},
    maybeTitleEdit: Option.none(),
    notices: [],
    nextNoticeId: 0,
    isCreatingWorkspace: false,
    isLinkCopied: false,
  },
});

/** The name to write with, when the user gave one. */
const writerOf = (model: Model) =>
  Option.liftPredicate(model.displayName.trim(), (name) => name !== "");

/** The project the issue list shows: the selection, or the first project. */
const currentProject = (model: Model) =>
  selectedProject(
    projectsOldestFirst(workspaceView(model.rows, model.pending)),
    model.maybeSelectedProjectId,
  );

/** Stamp an intent. Its pending row appears when the stamp comes back. */
const stamp = (model: Model, intent: Intent): UpdateReturn => ({
  model,
  commands: [GenerateStamp({ intent })],
});

export const update = (model: Model, message: Message) =>
  Message.match<UpdateReturn>(message, {
    ReceivedStateBatch: ({ changes, isUpToDate }) => {
      const confirmed = confirmedTxids(changes);

      return {
        model: modifyFields(model, {
          rows: (rows) => applyChanges(rows, changes),
          // A change with a write's txid confirms it. The stream row now holds
          // the server's version, so the local one can go.
          pending: Array.filter(({ txid }) => !Array.contains(confirmed, txid)),
          sync: (sync) => (isUpToDate ? Sync.Live() : sync),
        }),
      };
    },

    ChangedConnection: ({ isOnline }) => ({
      model: modifyFields(model, { isOnline: () => isOnline }),
    }),

    FailedReadWorkspace: ({ error }) => ({
      model: modifyFields(model, { sync: () => Sync.Failed({ error }) }),
    }),

    SelectedProject: ({ projectId }) => ({
      model: modifyFields(model, { maybeSelectedProjectId: () => Option.some(projectId) }),
    }),

    UpdatedProjectName: ({ value }) => ({
      model: modifyFields(model, { projectName: () => value }),
    }),

    UpdatedProjectDescription: ({ value }) => ({
      model: modifyFields(model, { projectDescription: () => value }),
    }),

    SubmittedProject: () => {
      const name = model.projectName.trim();

      if (name === "") return { model };

      return stamp(
        modifyFields(model, { projectName: () => "", projectDescription: () => "" }),
        Intent.CreateProject({ name, description: model.projectDescription.trim() }),
      );
    },

    UpdatedIssueTitle: ({ value }) => ({
      model: modifyFields(model, { issueTitle: () => value }),
    }),

    SubmittedIssue: () => {
      const title = model.issueTitle.trim();

      return Option.match(currentProject(model), {
        onNone: () => ({ model }),
        onSome: ({ row }) =>
          title === ""
            ? { model }
            : stamp(
                modifyFields(model, { issueTitle: () => "" }),
                Intent.CreateIssue({ projectId: row.id, title }),
              ),
      });
    },

    SelectedFilter: ({ filter }) => ({
      model: modifyFields(model, { filter: () => filter }),
    }),

    // The select offers only the three statuses; anything else is not a change.
    SelectedIssueStatus: ({ issueId, value }) =>
      Schema.is(IssueStatus)(value)
        ? stamp(model, Intent.UpdateIssue({ issueId, patch: { status: value } }))
        : { model },

    ClickedIssueTitle: ({ issueId }) =>
      Option.match(Record.get(workspaceIssues(model), issueId), {
        onNone: () => ({ model }),
        onSome: ({ title }) => ({
          model: modifyFields(model, {
            maybeTitleEdit: () => Option.some({ issueId, draft: title }),
          }),
          commands: [FocusTitleDraft({ issueId })],
        }),
      }),

    UpdatedTitleDraft: ({ value }) => ({
      model: modifyFields(model, {
        maybeTitleEdit: Option.map((edit) => ({ ...edit, draft: value })),
      }),
    }),

    // Closing the field removes it, and the browser can then send one more
    // blur. The edit is gone by then, so one edit saves at most once.
    BlurredTitleDraft: () => saveTitle(model),

    // Enter saves. Escape closes the field and keeps the old title.
    PressedTitleDraftKey: ({ key }) =>
      key === "Enter"
        ? saveTitle(model)
        : { model: modifyFields(model, { maybeTitleEdit: () => Option.none() }) },

    CompletedFocusTitleDraft: () => ({ model }),

    UpdatedCommentDraft: ({ issueId, value }) => ({
      model: modifyFields(model, { commentDrafts: Record.set(issueId, value) }),
    }),

    SubmittedComment: ({ issueId }) => {
      const body = Option.getOrElse(Record.get(model.commentDrafts, issueId), () => "").trim();

      if (body === "") return { model };

      return stamp(
        modifyFields(model, { commentDrafts: Record.remove(issueId) }),
        Intent.CreateComment({ issueId, body }),
      );
    },

    CompletedGenerateStamp: ({ intent, stamp: generated }) => {
      const pending = toPendingWrite(intent, generated, writerOf(model));

      // A new project is selected at once. If the server refuses it, the
      // selection falls back to the first project.
      const maybeNewProjectId = Write.matchOrElse(
        pending.write,
        { CreateProject: ({ project }) => Option.some(project.id) },
        () => model.maybeSelectedProjectId,
      );

      return {
        model: modifyFields(model, {
          pending: Array.append(pending),
          maybeSelectedProjectId: () => maybeNewProjectId,
        }),
        commands: [SendWrite({ workspaceId: model.workspaceId, pending })],
      };
    },

    // The row stays pending until its txid arrives on the stream.
    SucceededSendWrite: () => ({ model }),

    FailedSendWrite: ({ txid, error }) => {
      const noticeId = model.nextNoticeId;

      return {
        model: modifyFields(model, {
          pending: Array.filter((write) => write.txid !== txid),
          notices: Array.append({ id: noticeId, message: error }),
          nextNoticeId: (id) => id + 1,
        }),
        commands: [WaitBeforeNoticeDismissal({ noticeId })],
      };
    },

    CompletedWaitBeforeNoticeDismissal: ({ noticeId }) => ({
      model: modifyFields(model, {
        notices: Array.filter((notice) => notice.id !== noticeId),
      }),
    }),

    UpdatedDisplayName: ({ value }) => ({
      model: modifyFields(model, { displayName: () => value }),
      commands: [SaveDisplayName({ name: value })],
    }),

    CompletedSaveDisplayName: () => ({ model }),

    ClickedCopyLink: () => ({ model, commands: [CopyLink()] }),

    SucceededCopyLink: () => ({
      model: modifyFields(model, { isLinkCopied: () => true }),
      commands: [WaitBeforeCopiedReset()],
    }),

    // The button keeps its label, so the user sees that nothing was copied.
    FailedCopyLink: () => ({ model }),

    CompletedWaitBeforeCopiedReset: () => ({
      model: modifyFields(model, { isLinkCopied: () => false }),
    }),

    ClickedNewWorkspace: () =>
      model.isCreatingWorkspace
        ? { model }
        : {
            model: modifyFields(model, { isCreatingWorkspace: () => true }),
            commands: [CreateWorkspace()],
          },

    SucceededCreateWorkspace: ({ workspaceId }) => ({
      model,
      commands: [NavigateToWorkspace({ workspaceId })],
    }),

    FailedCreateWorkspace: () => ({
      model: modifyFields(model, { isCreatingWorkspace: () => false }),
    }),

    CompletedNavigateToWorkspace: () => ({ model }),
  });

/** The issues as the page shows them, by id. */
const workspaceIssues = (model: Model) =>
  Record.fromEntries(
    Array.map(workspaceView(model.rows, model.pending).issues, ({ row }) => [row.id, row] as const),
  );

/** Close the title field and rename the issue when the title changed. */
const saveTitle = (model: Model): UpdateReturn =>
  Option.match(model.maybeTitleEdit, {
    onNone: () => ({ model }),
    onSome: ({ issueId, draft }) => {
      const closed = modifyFields(model, { maybeTitleEdit: () => Option.none() });
      const title = draft.trim();

      const isRename = Option.match(Record.get(workspaceIssues(model), issueId), {
        onNone: () => false,
        onSome: (issue) => title !== "" && title !== issue.title,
      });

      return isRename
        ? stamp(closed, Intent.UpdateIssue({ issueId, patch: { title } }))
        : { model: closed };
    },
  });

import { Offset } from "@streamsy/core";
import { Schema } from "effect";
import { defineMessageUnion } from "foldkit/message";
import { Intent, Stamp } from "./domain/intent.ts";
import { Counts } from "./workspace-views.ts";
import { Rows, StatusFilter } from "./domain/workspace.ts";

export const Message = defineMessageUnion({
  ReceivedViews: { rows: Rows, counts: Counts, offset: Offset, caughtUp: Schema.Boolean },
  ChangedConnection: { isOnline: Schema.Boolean },
  FailedReadWorkspace: { error: Schema.String },

  // Projects
  SelectedProject: { projectId: Schema.String },
  UpdatedProjectName: { value: Schema.String },
  UpdatedProjectDescription: { value: Schema.String },
  SubmittedProject: {},

  // Issues
  UpdatedIssueTitle: { value: Schema.String },
  SubmittedIssue: {},
  SelectedFilter: { filter: StatusFilter },
  SelectedIssueStatus: { issueId: Schema.String, value: Schema.String },
  ClickedIssueTitle: { issueId: Schema.String },
  UpdatedTitleDraft: { value: Schema.String },
  BlurredTitleDraft: {},
  PressedTitleDraftKey: { key: Schema.Literals(["Enter", "Escape"]) },
  CompletedFocusTitleDraft: {},

  // Comments
  UpdatedCommentDraft: { issueId: Schema.String, value: Schema.String },
  SubmittedComment: { issueId: Schema.String },

  // Optimistic writes: stamp, show as pending, send, then wait for the append offset.
  CompletedGenerateStamp: { intent: Intent, stamp: Stamp },
  SucceededSendWrite: { txid: Schema.String, awaitOffset: Schema.String },
  FailedSendWrite: { txid: Schema.String, error: Schema.String },
  CompletedWaitBeforeNoticeDismissal: { noticeId: Schema.Int },

  // Display name
  UpdatedDisplayName: { value: Schema.String },
  CompletedSaveDisplayName: {},

  // Share controls
  ClickedCopyLink: {},
  SucceededCopyLink: {},
  FailedCopyLink: { error: Schema.String },
  CompletedWaitBeforeCopiedReset: {},
  ClickedNewWorkspace: {},
  SucceededCreateWorkspace: { workspaceId: Schema.String },
  FailedCreateWorkspace: { error: Schema.String },
  CompletedNavigateToWorkspace: {},
});

export type Message = typeof Message.Type;

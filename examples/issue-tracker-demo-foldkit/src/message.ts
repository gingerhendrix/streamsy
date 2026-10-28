import { Schema } from "effect";
import { defineMessageUnion } from "foldkit/message";
import { StateEvent } from "@streamsy/issue-tracker-demo-contract/state-schema";
import { Intent, Stamp } from "./domain/intent.ts";
import { StatusFilter } from "./domain/workspace.ts";

export const Message = defineMessageUnion({
  // The workspace stream. Each batch the reader gets becomes one Message.
  ReceivedStateBatch: { changes: Schema.Array(StateEvent), isUpToDate: Schema.Boolean },
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

  // Optimistic writes: stamp, show as pending, send, then wait for the txid.
  CompletedGenerateStamp: { intent: Intent, stamp: Stamp },
  SucceededSendWrite: { txid: Schema.String },
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

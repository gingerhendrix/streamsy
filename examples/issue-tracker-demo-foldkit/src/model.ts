import { Schema } from "effect";
import { defineTaggedUnion } from "foldkit/schema";
import { PendingWrite, Rows, StatusFilter } from "./domain/workspace.ts";

/** How far the page has read the workspace stream. */
export const Sync = defineTaggedUnion({
  Loading: {},
  Live: {},
  Failed: { error: Schema.String },
});

export type Sync = typeof Sync.Type;

/** The title field of one issue, while the user renames it. */
export const TitleEdit = Schema.Struct({ issueId: Schema.String, draft: Schema.String });

export type TitleEdit = typeof TitleEdit.Type;

/** A short message about a change that the server refused. */
export const Notice = Schema.Struct({ id: Schema.Int, message: Schema.String });

export type Notice = typeof Notice.Type;

export const Model = Schema.Struct({
  workspaceId: Schema.String,
  /** The name this browser writes with. Empty means anonymous. */
  displayName: Schema.String,
  sync: Sync,
  /** Whether the last stream read reached the server. */
  isOnline: Schema.Boolean,
  /** The rows as the stream has them. */
  rows: Rows,
  /** Local changes that wait for their txid on the stream. */
  pending: Schema.Array(PendingWrite),
  maybeSelectedProjectId: Schema.Option(Schema.String),
  filter: StatusFilter,
  projectName: Schema.String,
  projectDescription: Schema.String,
  issueTitle: Schema.String,
  /** The comment field of each issue, by issue id. */
  commentDrafts: Schema.Record(Schema.String, Schema.String),
  maybeTitleEdit: Schema.Option(TitleEdit),
  notices: Schema.Array(Notice),
  nextNoticeId: Schema.Int,
  isCreatingWorkspace: Schema.Boolean,
  isLinkCopied: Schema.Boolean,
});

export type Model = typeof Model.Type;

/** What the page reads before the first render: the `?w=` workspace and the saved name. */
export const Flags = Schema.Struct({ workspaceId: Schema.String, displayName: Schema.String });

export type Flags = typeof Flags.Type;

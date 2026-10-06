/**
 * Notes and tracker streams for docs code blocks. A block imports them through a
 * hidden `streams.ts` that re-exports this module.
 */
import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";

export const Note = Schema.Struct({ id: Schema.String, text: Schema.String });

export class Notes extends Streamsy.json<Notes>()("Notes/Notes", "notes", {
  params: {},
  schema: Note,
}) {}
export class Drafts extends Streamsy.json<Drafts>()("Notes/Drafts", "drafts/:draftId", {
  params: { draftId: Schema.String },
  schema: Note,
}) {}

const Project = Schema.Struct({ id: Schema.String, name: Schema.String });
const Issue = Schema.Struct({ id: Schema.String, projectId: Schema.String, title: Schema.String });

export class Workspace extends Streamsy.state<Workspace>()(
  "Tracker/Workspace",
  "workspaces/:workspaceId",
  {
    params: { workspaceId: Schema.String },
    collections: { project: { schema: Project, key: "id" }, issue: { schema: Issue, key: "id" } },
  },
) {}

import { Option, Schema } from "effect";
import { defineTaggedUnion } from "foldkit/schema";
import { anonymousAuthor } from "@streamsy/issue-tracker-demo-contract/state-schema";
import { IssuePatch, Write, type PendingWrite } from "./workspace.ts";

/** A change the user asked for. It has no ids or times yet. */
export const Intent = defineTaggedUnion({
  CreateProject: { name: Schema.String, description: Schema.String },
  CreateIssue: { projectId: Schema.String, title: Schema.String },
  UpdateIssue: { issueId: Schema.String, patch: IssuePatch },
  CreateComment: { issueId: Schema.String, body: Schema.String },
});

export type Intent = typeof Intent.Type;

/**
 * The random and time values for one write. A Command makes them, so
 * `update` stays pure. The client picks the row id and the txid, so the
 * pending row and the stream row have the same key.
 */
export const Stamp = Schema.Struct({
  entropy: Schema.String,
  txid: Schema.String,
  at: Schema.String,
});

export type Stamp = typeof Stamp.Type;

const idFor = (prefix: string, stamp: Stamp) => `${prefix}_${stamp.entropy.slice(0, 8)}`;

/** The pending write for an intent, written by `writer` when they gave a name. */
export const toPendingWrite = (
  intent: Intent,
  stamp: Stamp,
  maybeWriter: Option.Option<string>,
): PendingWrite => {
  const updatedBy = Option.getOrUndefined(maybeWriter);

  const named = <A extends object>(row: A) =>
    updatedBy === undefined ? row : { ...row, updatedBy };

  const write = Intent.match(intent, {
    CreateProject: ({ name, description }) =>
      Write.CreateProject({
        project: { id: idFor("proj", stamp), name, description, createdAt: stamp.at },
      }),
    CreateIssue: ({ projectId, title }) =>
      Write.CreateIssue({
        issue: named({
          id: idFor("issue", stamp),
          projectId,
          title,
          status: "open" as const,
          createdAt: stamp.at,
          updatedAt: stamp.at,
        }),
      }),
    UpdateIssue: ({ issueId, patch }) =>
      Write.UpdateIssue(named({ issueId, patch, updatedAt: stamp.at })),
    CreateComment: ({ issueId, body }) =>
      Write.CreateComment({
        comment: {
          id: idFor("comment", stamp),
          issueId,
          author: updatedBy ?? anonymousAuthor,
          body,
          createdAt: stamp.at,
        },
      }),
  });

  return { txid: stamp.txid, write };
};

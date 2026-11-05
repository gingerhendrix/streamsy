import { Data, Option } from "effect";
import { anonymousAuthor } from "@streamsy/issue-tracker-demo-contract/state-schema";
import { Write, type IssuePatch, type PendingWrite } from "./workspace.ts";

/** A change the user asked for. It has no ids or times yet. */
export type Intent = Data.TaggedEnum<{
  CreateProject: { readonly name: string; readonly description: string };
  CreateIssue: { readonly projectId: string; readonly title: string };
  UpdateIssue: { readonly issueId: string; readonly patch: IssuePatch };
  CreateComment: { readonly issueId: string; readonly body: string };
}>;

export const Intent = Data.taggedEnum<Intent>();

/**
 * The random and time values for one write. The client picks the row id and
 * the txid, so the pending row and the stream row have the same key.
 */
export interface Stamp {
  readonly entropy: string;
  readonly txid: string;
  readonly at: string;
}

const idFor = (prefix: string, stamp: Stamp) => `${prefix}_${stamp.entropy.slice(0, 8)}`;

/** The pending write for an intent, written by `writer` when they gave a name. */
export const toPendingWrite = (
  intent: Intent,
  stamp: Stamp,
  maybeWriter: Option.Option<string>,
): PendingWrite => {
  const write = Intent.$match(intent, {
    CreateProject: ({ name, description }) =>
      Write.CreateProject({
        project: { id: idFor("proj", stamp), name, description, createdAt: stamp.at },
      }),
    CreateIssue: ({ projectId, title }) => {
      const issue = {
        id: idFor("issue", stamp),
        projectId,
        title,
        status: "open" as const,
        createdAt: stamp.at,
        updatedAt: stamp.at,
      };

      return Write.CreateIssue({
        issue: Option.match(maybeWriter, {
          onNone: () => issue,
          onSome: (updatedBy) => ({ ...issue, updatedBy }),
        }),
      });
    },
    UpdateIssue: ({ issueId, patch }) =>
      Write.UpdateIssue({ issueId, patch, updatedAt: stamp.at, updatedBy: maybeWriter }),
    CreateComment: ({ issueId, body }) =>
      Write.CreateComment({
        comment: {
          id: idFor("comment", stamp),
          issueId,
          author: Option.getOrElse(maybeWriter, () => anonymousAuthor),
          body,
          createdAt: stamp.at,
        },
      }),
  });

  return { txid: stamp.txid, write };
};

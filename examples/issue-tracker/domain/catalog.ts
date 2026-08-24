/** Typed current-state rows maintained from workspace Durable State sources. */
import { Schema } from "effect";
import { Identifier, Timestamp, Title } from "./issue.ts";

const Description = Schema.String.check(Schema.isMaxLength(2_000));
const LabelColor = Schema.String.check(
  Schema.isPattern(/^#[0-9a-fA-F]{6}$/, { title: "a six-digit hexadecimal colour" }),
);
const HttpUrl = Schema.String.check(
  Schema.isPattern(/^https?:\/\/\S+$/, { title: "an HTTP(S) URL" }),
);

export const ProjectRow = Schema.Struct({
  projectId: Identifier,
  workspaceId: Identifier,
  key: Identifier,
  name: Title,
  description: Schema.optionalKey(Description),
  updatedAt: Timestamp,
});
export type ProjectRow = typeof ProjectRow.Type;

export const UserRow = Schema.Struct({
  userId: Identifier,
  workspaceId: Identifier,
  name: Title,
  avatarUrl: Schema.optionalKey(HttpUrl),
  updatedAt: Timestamp,
});
export type UserRow = typeof UserRow.Type;

export const LabelRow = Schema.Struct({
  labelId: Identifier,
  workspaceId: Identifier,
  name: Title,
  color: LabelColor,
  updatedAt: Timestamp,
});
export type LabelRow = typeof LabelRow.Type;

export const WorkspaceMetadataRow = Schema.Struct({
  workspaceId: Identifier,
  name: Title,
  description: Schema.optionalKey(Description),
  updatedAt: Timestamp,
});
export type WorkspaceMetadataRow = typeof WorkspaceMetadataRow.Type;

export const CatalogCollection = Schema.Literals(["projects", "users", "labels", "metadata"]);
export type CatalogCollection = typeof CatalogCollection.Type;

export type CatalogRow = ProjectRow | UserRow | LabelRow | WorkspaceMetadataRow;

export interface DecodedCatalogRow {
  readonly row: CatalogRow;
  readonly key: string;
  readonly workspaceId: string;
}

export const catalog = {
  projects: { type: "project", schema: ProjectRow, key: (row: ProjectRow) => row.projectId },
  users: { type: "user", schema: UserRow, key: (row: UserRow) => row.userId },
  labels: { type: "label", schema: LabelRow, key: (row: LabelRow) => row.labelId },
  metadata: {
    type: "workspace",
    schema: WorkspaceMetadataRow,
    key: (row: WorkspaceMetadataRow) => row.workspaceId,
  },
} as const;

export function decodeCatalogRow(collection: CatalogCollection, value: unknown): DecodedCatalogRow {
  switch (collection) {
    case "projects": {
      const row = Schema.decodeUnknownSync(ProjectRow)(value);
      return { row, key: row.projectId, workspaceId: row.workspaceId };
    }
    case "users": {
      const row = Schema.decodeUnknownSync(UserRow)(value);
      return { row, key: row.userId, workspaceId: row.workspaceId };
    }
    case "labels": {
      const row = Schema.decodeUnknownSync(LabelRow)(value);
      return { row, key: row.labelId, workspaceId: row.workspaceId };
    }
    case "metadata": {
      const row = Schema.decodeUnknownSync(WorkspaceMetadataRow)(value);
      return { row, key: row.workspaceId, workspaceId: row.workspaceId };
    }
  }
}

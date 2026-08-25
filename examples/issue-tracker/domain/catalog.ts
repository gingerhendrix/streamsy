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

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- The callback is the catalog's named JSON parse boundary and every supplied decoder is built from the adjacent Effect schema.
const codec = <A extends object>(decode: (value: unknown) => A) => ({
  encode: (value: A): A => value,
  decode,
});

export const catalog = {
  projects: {
    type: "project",
    schema: codec(Schema.decodeUnknownSync(ProjectRow)),
    primaryKey: (row: ProjectRow) => row.projectId,
  },
  users: {
    type: "user",
    schema: codec(Schema.decodeUnknownSync(UserRow)),
    primaryKey: (row: UserRow) => row.userId,
  },
  labels: {
    type: "label",
    schema: codec(Schema.decodeUnknownSync(LabelRow)),
    primaryKey: (row: LabelRow) => row.labelId,
  },
  metadata: {
    type: "workspace",
    schema: codec(Schema.decodeUnknownSync(WorkspaceMetadataRow)),
    primaryKey: (row: WorkspaceMetadataRow) => row.workspaceId,
  },
} as const;

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This function is the named schema boundary shared by HTTP ingestion and persisted-row restore; every branch immediately decodes the unknown value through the collection's declared Schema.
export function decodeCatalogRow(collection: CatalogCollection, value: unknown): DecodedCatalogRow {
  switch (collection) {
    case "projects": {
      const row = catalog.projects.schema.decode(value);
      return catalogRow(collection, row);
    }
    case "users": {
      const row = catalog.users.schema.decode(value);
      return catalogRow(collection, row);
    }
    case "labels": {
      const row = catalog.labels.schema.decode(value);
      return catalogRow(collection, row);
    }
    case "metadata": {
      const row = catalog.metadata.schema.decode(value);
      return catalogRow(collection, row);
    }
  }
  collection satisfies never;
  throw new TypeError("unknown catalog collection");
}

/** Recover the identity fields from a row already decoded by the catalog's protocol schema. */
export function catalogRow(collection: CatalogCollection, row: CatalogRow): DecodedCatalogRow {
  switch (collection) {
    case "projects":
      if (!("projectId" in row)) throw new TypeError("expected a project row");
      return {
        row,
        key: catalog.projects.primaryKey(row),
        workspaceId: row.workspaceId,
      };
    case "users":
      if (!("userId" in row)) throw new TypeError("expected a user row");
      return { row, key: catalog.users.primaryKey(row), workspaceId: row.workspaceId };
    case "labels":
      if (!("labelId" in row)) throw new TypeError("expected a label row");
      return { row, key: catalog.labels.primaryKey(row), workspaceId: row.workspaceId };
    case "metadata":
      if ("projectId" in row || "userId" in row || "labelId" in row) {
        throw new TypeError("expected a workspace metadata row");
      }
      return { row, key: catalog.metadata.primaryKey(row), workspaceId: row.workspaceId };
  }
  collection satisfies never;
  throw new TypeError("unknown catalog collection");
}

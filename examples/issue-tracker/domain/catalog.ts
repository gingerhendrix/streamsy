/**
 * The workspace catalog: four Durable State collections and their rows.
 *
 * Each collection is declared once. That single declaration names the wire
 * collection and type, carries the row schema, and names the key field. The
 * plan key expression, the host's schema/type/primary-key table, and the
 * ingestion key check are all derived from it, so a collection cannot key its
 * plan by one field and its wire messages by another.
 */
import { Schema } from "effect";
import { selectors, source } from "@streamsy/views";
import { decodeIdentifier, Identifier, Timestamp, Title } from "./issue.ts";

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

const project = selectors<ProjectRow>();
const user = selectors<UserRow>();
const label = selectors<LabelRow>();
const workspace = selectors<WorkspaceMetadataRow>();

export const projects = source("issue-tracker.projects", {
  schema: ProjectRow,
  schemaRef: { name: "issue-tracker.ProjectRow", version: 1 },
  partitionBy: project.row.workspaceId,
  key: "projectId",
  mode: "state",
  collection: { name: "projects", type: "project" },
});

export const users = source("issue-tracker.users", {
  schema: UserRow,
  schemaRef: { name: "issue-tracker.UserRow", version: 1 },
  partitionBy: user.row.workspaceId,
  key: "userId",
  mode: "state",
  collection: { name: "users", type: "user" },
});

export const labels = source("issue-tracker.labels", {
  schema: LabelRow,
  schemaRef: { name: "issue-tracker.LabelRow", version: 1 },
  partitionBy: label.row.workspaceId,
  key: "labelId",
  mode: "state",
  collection: { name: "labels", type: "label" },
});

export const workspaceMetadata = source("issue-tracker.workspace-metadata", {
  schema: WorkspaceMetadataRow,
  schemaRef: { name: "issue-tracker.WorkspaceMetadataRow", version: 1 },
  partitionBy: workspace.row.workspaceId,
  key: "workspaceId",
  mode: "state",
  collection: { name: "metadata", type: "workspace" },
});

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- The callback is the catalog's named JSON parse boundary and every supplied decoder is built from the adjacent Effect schema.
const codec = <A extends object>(decode: (value: unknown) => A) => ({
  encode: (value: A): A => value,
  decode,
});

/** Lower one declared State source to the schema/type/primary-key entry a host binds. */
const collectionOf = <
  Row extends object,
  const Type extends string,
  const Key extends string,
>(declaration: {
  readonly name: string;
  readonly schema: Schema.Codec<Row>;
  readonly key: Key;
  readonly collection: { readonly name: string; readonly type: Type };
}) => ({
  type: declaration.collection.type,
  schema: codec(Schema.decodeUnknownSync(declaration.schema)),
  primaryKey: declaration.key,
});

/** The one schema/type/primary-key table, derived from the four declarations above. */
export const catalog = {
  [projects.collection.name]: collectionOf(projects),
  [users.collection.name]: collectionOf(users),
  [labels.collection.name]: collectionOf(labels),
  [workspaceMetadata.collection.name]: collectionOf(workspaceMetadata),
} as const;

export const CATALOG_COLLECTIONS = [
  projects.collection.name,
  users.collection.name,
  labels.collection.name,
  workspaceMetadata.collection.name,
] as const;

export const CatalogCollection = Schema.Literals(CATALOG_COLLECTIONS);
export type CatalogCollection = typeof CatalogCollection.Type;

export type CatalogRow = ProjectRow | UserRow | LabelRow | WorkspaceMetadataRow;

export interface DecodedCatalogRow {
  readonly row: CatalogRow;
  readonly key: string;
  readonly workspaceId: string;
}

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This function is the named schema boundary shared by HTTP ingestion and persisted-row restore; every branch immediately decodes the unknown value through the collection's declared Schema.
export function decodeCatalogRow(collection: CatalogCollection, value: unknown): DecodedCatalogRow {
  switch (collection) {
    case "projects":
      return catalogRow(collection, catalog.projects.schema.decode(value));
    case "users":
      return catalogRow(collection, catalog.users.schema.decode(value));
    case "labels":
      return catalogRow(collection, catalog.labels.schema.decode(value));
    case "metadata":
      return catalogRow(collection, catalog.metadata.schema.decode(value));
  }
  collection satisfies never;
  throw new TypeError("unknown catalog collection");
}

/**
 * Recover the identity fields from a row already decoded by the catalog's protocol schema.
 *
 * The key is read from the field the collection declared, so this cannot drift
 * from the plan key expression. Row shape itself is validated by
 * `decodeCatalogRow`, and ingestion additionally checks the wire type before it
 * gets here.
 */
export function catalogRow(collection: CatalogCollection, row: CatalogRow): DecodedCatalogRow {
  const primaryKey = catalog[collection].primaryKey;
  const value: unknown = Object.getOwnPropertyDescriptor(row, primaryKey)?.value;
  return { row, key: decodeIdentifier(value), workspaceId: row.workspaceId };
}

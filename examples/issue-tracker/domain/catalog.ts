import { Schema } from "effect";
import { Identifier, Timestamp, Title } from "./issue.ts";

const Description = Schema.String.check(Schema.isMaxLength(2_000));
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
  avatarUrl: Schema.optionalKey(Schema.String),
  updatedAt: Timestamp,
});
export type UserRow = typeof UserRow.Type;
export const LabelRow = Schema.Struct({
  labelId: Identifier,
  workspaceId: Identifier,
  name: Title,
  color: Schema.String.check(Schema.isPattern(/^#[0-9a-fA-F]{6}$/)),
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

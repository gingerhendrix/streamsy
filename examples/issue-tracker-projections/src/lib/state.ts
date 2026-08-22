/**
 * Pure browser-side State-stream folding.
 *
 * The browser reads durable State streams (project rows, board rows) straight
 * from the Durable Streams HTTP endpoint. It never reconstructs domain state
 * from command responses, so a reload rebuilds everything from these folds.
 *
 * Framework lineage rows share the target stream. They are reserved and are
 * skipped here rather than treated as a fault: the browser is a reader, not a
 * projection owner.
 */
import {
  BoardRowSchema,
  BOARD_ROW_COLLECTION,
  ProjectSchema,
  PROJECT_COLLECTION,
  type BoardRow,
  type Project,
} from "../../shared/model.ts";
import { Schema } from "effect";

/** One State message as it appears in the JSON stream body. */
export interface StateItem {
  readonly type?: unknown;
  readonly key?: unknown;
  readonly value?: unknown;
  readonly headers?: unknown;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isDelete(item: StateItem): boolean {
  return isRecord(item.headers) && item.headers.operation === "delete";
}

/**
 * Decode one durable board row, or return `undefined` when the row is not the
 * shape this reader understands.
 *
 * The server writes these rows through `BoardRowSchema`, so a row that fails
 * here is not something the browser can repair: it is skipped rather than
 * rendered as a partly-typed card.
 */
export function toBoardRow(value: Record<string, unknown>): BoardRow | undefined {
  const decoded = Schema.decodeUnknownOption(BoardRowSchema)(value);
  return decoded._tag === "Some" ? decoded.value : undefined;
}

/** Decode one durable project row, on the same terms as {@link toBoardRow}. */
export function toProject(value: Record<string, unknown>): Project | undefined {
  const decoded = Schema.decodeUnknownOption(ProjectSchema)(value);
  return decoded._tag === "Some" ? decoded.value : undefined;
}

/**
 * Fold State messages of one collection into a keyed map. Unknown collections
 * (including reserved `__streamsy.mesh.*` lineage rows) are ignored, and so is
 * any row `decode` cannot read.
 */
export function foldCollection<T>(
  previous: ReadonlyMap<string, T>,
  items: readonly unknown[],
  collection: string,
  decode: (value: Record<string, unknown>) => T | undefined,
): ReadonlyMap<string, T> {
  let next: Map<string, T> | undefined;
  for (const raw of items) {
    if (!isRecord(raw)) continue;
    const item: StateItem = raw;
    if (item.type !== collection || typeof item.key !== "string") continue;
    next ??= new Map(previous);
    if (isDelete(item)) {
      next.delete(item.key);
      continue;
    }
    if (!isRecord(item.value)) continue;
    const decoded = decode(item.value);
    if (decoded === undefined) continue;
    next.set(item.key, decoded);
  }
  return next ?? previous;
}

export const foldBoardRows = (
  previous: ReadonlyMap<string, BoardRow>,
  items: readonly unknown[],
): ReadonlyMap<string, BoardRow> =>
  foldCollection<BoardRow>(previous, items, BOARD_ROW_COLLECTION, toBoardRow);

export const foldProjects = (
  previous: ReadonlyMap<string, Project>,
  items: readonly unknown[],
): ReadonlyMap<string, Project> =>
  foldCollection<Project>(previous, items, PROJECT_COLLECTION, toProject);

/** Board order: newest activity first inside a column, then by issue key. */
export function sortBoardRows(rows: readonly BoardRow[]): readonly BoardRow[] {
  return rows.toSorted((left, right) => {
    const byTime = right.updatedAt.localeCompare(left.updatedAt);
    return byTime !== 0 ? byTime : left.issueKey.localeCompare(right.issueKey);
  });
}

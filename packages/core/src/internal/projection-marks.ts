import { Context, type Effect, type Option } from "effect";
import type { SqlError } from "effect/sql";
import type { StorageFault } from "../fault.ts";

/** One pending mark: a binding whose input has commits that may not have run. */
export interface PendingMark {
  /** The binding's canonical params, as `canonicalParams` encodes them. */
  readonly params: string;
  /** Raised by every mark. A clear only deletes the row when its `seq` still matches. */
  readonly seq: number;
  /** Failed runs since the last success. A mark keeps it. */
  readonly attempts: number;
  /** Epoch ms. 0 means due now. */
  readonly dueAt: number;
}

/**
 * Durable pending marks on the storage owner, for attached projections.
 *
 * A mark is keyed by projection id and canonical params. Version and generation
 * are left out, so a deploy that raises the version picks up the old marks.
 */
export interface ProjectionMarksApi {
  /**
   * Insert a mark for each binding, or raise its `seq`. A raise keeps
   * `attempts` and `dueAt`, so a write does not reset a binding's backoff.
   * Call it inside a commit hook. It joins the transaction and fails like the
   * transaction's own statements.
   */
  readonly mark: (
    projection: string,
    params: ReadonlyArray<string>,
  ) => Effect.Effect<void, SqlError.SqlError>;
  /** The earliest `dueAt` of the projection's marks. */
  readonly nextDue: (projection: string) => Effect.Effect<Option.Option<number>, StorageFault>;
  /** Up to `limit` marks with `dueAt <= now`, earliest first. */
  readonly due: (
    projection: string,
    now: number,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<PendingMark>, StorageFault>;
  /** The marks of these bindings that exist, keyed by params. */
  readonly read: (
    projection: string,
    params: ReadonlyArray<string>,
  ) => Effect.Effect<ReadonlyMap<string, PendingMark>, StorageFault>;
  /** Delete the mark if its `seq` still equals `seq`. A later mark keeps the row. */
  readonly clear: (
    projection: string,
    params: string,
    seq: number,
  ) => Effect.Effect<void, StorageFault>;
  /** Delete the mark whatever its `seq`. */
  readonly remove: (projection: string, params: string) => Effect.Effect<void, StorageFault>;
  /** Set `attempts` and `dueAt`, and insert the mark if it is absent. */
  readonly defer: (
    projection: string,
    params: string,
    attempts: number,
    dueAt: number,
  ) => Effect.Effect<void, StorageFault>;
  /** The number of the projection's marks with at least one failed attempt. */
  readonly deferred: (projection: string) => Effect.Effect<number, StorageFault>;
}

export class ProjectionMarks extends Context.Service<ProjectionMarks, ProjectionMarksApi>()(
  "@streamsy/core/internal/ProjectionMarks",
) {}

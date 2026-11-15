import { Context, type Effect, type Option } from "effect";
import type { SqlError } from "effect/sql";
import type { StorageFault } from "../fault.ts";

/** Last failed run, retained through marks, retries and resumes. Messages hold at most 1,000 characters. */
export interface MarkFailure {
  readonly at: number;
  readonly reason: Option.Option<string>;
  readonly message: string;
}

/** One pending mark: a binding whose input has commits that may not have run. */
export interface PendingMark {
  /** The binding's canonical params, as `canonicalParams` encodes them. */
  readonly params: string;
  /** Raised by every mark. A clear only deletes the row when its `seq` still matches. */
  readonly seq: number;
  /** Failed runs since the last success. A mark keeps it. */
  readonly attempts: number;
  /** Epoch ms. 0 means due now; ignored while stopped. */
  readonly dueAt: number;
  /** Waits until retry or resume. A mark preserves the stop. */
  readonly stopped: boolean;
  readonly failure: Option.Option<MarkFailure>;
}

/** Disjoint counts of one projection's marks at the supplied time. */
export interface ProjectionMarkSummary {
  readonly fresh: number;
  readonly retryDue: number;
  readonly backingOff: number;
  readonly stopped: number;
  /** Earliest due time across non-stopped marks, including fresh marks. */
  readonly nextDueAt: Option.Option<number>;
}

/**
 * Pending marks for attached projections, on a storage owner or in private memory.
 *
 * A mark is keyed by projection id and canonical params. Version and generation
 * are left out, so a deploy that raises the version picks up the old marks.
 */
export interface ProjectionMarksApi {
  /**
   * Insert a mark for each binding, or raise its `seq`. A raise keeps
   * `attempts`, `dueAt`, `stopped` and `failure`, so a write never resets backoff or a stop.
   * Call it inside a commit hook. It joins the transaction and fails like the
   * transaction's own statements.
   */
  readonly mark: (
    projection: string,
    params: ReadonlyArray<string>,
  ) => Effect.Effect<void, SqlError.SqlError>;
  /** The earliest `dueAt` of the projection's non-stopped marks. */
  readonly nextDue: (projection: string) => Effect.Effect<Option.Option<number>, StorageFault>;
  /** Four disjoint counts: fresh, retry due, backing off and stopped. */
  readonly summary: (
    projection: string,
    now: number,
  ) => Effect.Effect<ProjectionMarkSummary, StorageFault>;
  /** Up to `limit` non-stopped marks with `dueAt <= now`, earliest first. */
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
  /** Delete on a matching `seq`; otherwise keep the row and reset its failure, attempts, dueAt and stop. */
  readonly clear: (
    projection: string,
    params: string,
    seq: number,
  ) => Effect.Effect<void, StorageFault>;
  /** Delete the mark whatever its `seq`. */
  readonly remove: (projection: string, params: string) => Effect.Effect<void, StorageFault>;
  /** Insert or update a failure, preserving seq. Some(dueAt) backs off; None stops. */
  readonly fail: (
    projection: string,
    params: string,
    options: {
      readonly attempts: number;
      readonly failure: MarkFailure;
      readonly dueAt: Option.Option<number>;
    },
  ) => Effect.Effect<void, StorageFault>;
  /** Set failed or stopped marks due now and clear their stop, preserving attempts, seq and failure.
   * Optionally select one binding. Returns the number changed; already due, non-stopped marks are unchanged.
   */
  readonly retry: (projection: string, params?: string) => Effect.Effect<number, StorageFault>;
  /** Make stopped marks due now, preserving attempts, seq and failure. Returns the number changed. */
  readonly resume: (projection: string) => Effect.Effect<number, StorageFault>;
  /** Up to limit marks with attempts > 0, stopped first, then by dueAt and params. */
  readonly failed: (
    projection: string,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<PendingMark>, StorageFault>;
}

export class ProjectionMarks extends Context.Service<ProjectionMarks, ProjectionMarksApi>()(
  "@streamsy/core/internal/ProjectionMarks",
) {}

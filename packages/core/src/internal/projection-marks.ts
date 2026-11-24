import { Context, type Effect, type Option } from "effect";
import type { SqlError } from "effect/sql";
import type { StorageFault } from "../fault.ts";

/** Last failed run, retained through marks, retries and resumes. Messages hold at most 1,000 characters. */
export interface MarkFailure {
  readonly at: number;
  readonly reason: Option.Option<string>;
  readonly message: string;
}

/** One pending mark: a binding with input work, a retry, or a recorded wake. */
export interface PendingMark {
  /** The binding's canonical params, as `canonicalParams` encodes them. */
  readonly params: string;
  /** Raised by every mark. A clear only deletes the row when its `seq` still matches. */
  readonly seq: number;
  /** Failed runs since the last success. A mark keeps it. */
  readonly attempts: number;
  /** Epoch ms. 0 means due now; ignored while stopped. */
  readonly dueAt: number;
  /** Recorded wake; when set, equals dueAt and the row has no failure or attempts. */
  readonly wakeAt: Option.Option<number>;
  /** Waits until retry or resume. A mark preserves the stop. */
  readonly stopped: boolean;
  readonly failure: Option.Option<MarkFailure>;
}

/** Disjoint counts of one projection's marks at the supplied time. */
export interface ProjectionMarkSummary {
  readonly fresh: number;
  readonly sleeping: number;
  readonly retryDue: number;
  readonly backingOff: number;
  readonly stopped: number;
  /** Earliest due time across non-stopped marks, including fresh and sleeping marks. */
  readonly nextDueAt: Option.Option<number>;
}

/**
 * Pending marks for attached projections, on a storage owner or in private memory.
 *
 * A mark is keyed by projection id and canonical params. A deploy that raises the
 * generation picks up the existing marks.
 */
export interface ProjectionMarksApi {
  /** Maximum projection id length in Unicode code points, when bounded by storage. */
  readonly maxProjectionIdLength?: number;
  /**
   * Insert a mark for each binding, or raise its `seq`. A raise keeps
   * `attempts`, `stopped` and `failure`, so a write never resets backoff or a stop.
   * A sleeping row becomes due now and loses its wake index.
   * Call it inside a commit hook. It joins the transaction and fails like the
   * transaction's own statements.
   */
  readonly mark: (
    projection: string,
    params: ReadonlyArray<string>,
  ) => Effect.Effect<void, SqlError.SqlError>;
  /** The earliest `dueAt` of the projection's non-stopped marks. */
  readonly nextDue: (projection: string) => Effect.Effect<Option.Option<number>, StorageFault>;
  /** Five disjoint counts: fresh, sleeping, retry due, backing off and stopped. */
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
  /** Delete on a matching `seq`; otherwise keep the row and reset its failure, attempts, dueAt, wakeAt and stop. */
  readonly clear: (
    projection: string,
    params: string,
    seq: number,
  ) => Effect.Effect<void, StorageFault>;
  /** Keep a matching row asleep at wakeAt and reset failure state. A seq mismatch
   * becomes fresh and due now. An absent row is inserted asleep with seq 1. */
  readonly sleep: (
    projection: string,
    params: string,
    seq: number,
    wakeAt: number,
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

/**
 * Maintained state, as a service.
 *
 * The store owns four durable things for one workspace:
 *
 * - `view_rows`     — the maintained output of `issue-tracker.issues`;
 * - `reducer_state` — the fold state of `issue-tracker.issue-lifecycle`;
 * - `progress`      — the consumed source checkpoint and the published position;
 * - `receipts`      — one row per accepted `commandId`.
 *
 * Row and state are equal values today, because this reducer's state *is* the
 * published row. They are still stored apart: a reducer whose state carries
 * bookkeeping the sink must not publish is the normal case, and collapsing the
 * two now would hide that seam behind a coincidence.
 *
 * Both values are stored as JSON text and decoded through the declared Schema
 * on the way out. A durable value that no longer decodes becomes a typed
 * {@link StoreRestorePoison} rather than a row the board would serve — the same
 * law `issue-tracker-projections` established for its State restores.
 */
import { Context, Effect, Layer } from "effect";
import { decodeIssueRow, type IssueRow } from "../domain/issue.ts";
import { StoreRestorePoison, StoreUnavailable } from "./errors.ts";

/** What the view has consumed, and what the sink has published. */
export interface ViewProgress {
  /** After-exclusive source cursor already folded into `view_rows`. */
  readonly checkpoint: string | undefined;
  /** Source cursor whose changes are durably on the sink's State stream. */
  readonly published: string | undefined;
}

export interface CommandReceipt {
  readonly commandId: string;
  readonly workspaceId: string;
  readonly issueId: string;
  /** The offset the *original* append received. */
  readonly offset: string;
  readonly eventId: string;
  readonly sequence: number;
}

/** One atomic advance of the maintained view. */
export interface CommitInput {
  readonly checkpoint: string;
  /** Final reducer state per touched key; also the maintained row. */
  readonly rows: ReadonlyMap<string, IssueRow>;
  /**
   * One past the highest `sequence` this batch folded.
   *
   * The next command reads it to number its own event, so numbering survives a
   * restart by being derived from durable facts rather than from a counter the
   * process happened to hold.
   */
  readonly nextSequence: number;
}

export interface IssueStoreService {
  readonly progress: (workspaceId: string) => Effect.Effect<ViewProgress, StoreUnavailable>;
  /** Prior reducer state for exactly the keys a batch touches. */
  readonly reducerStates: (
    workspaceId: string,
    keys: readonly string[],
  ) => Effect.Effect<ReadonlyMap<string, IssueRow>, StoreUnavailable | StoreRestorePoison>;
  readonly rows: (
    workspaceId: string,
  ) => Effect.Effect<readonly IssueRow[], StoreUnavailable | StoreRestorePoison>;
  /** Commit rows, reducer state and the source checkpoint together or not at all. */
  readonly commit: (
    workspaceId: string,
    input: CommitInput,
  ) => Effect.Effect<void, StoreUnavailable>;
  readonly markPublished: (
    workspaceId: string,
    position: string,
  ) => Effect.Effect<void, StoreUnavailable>;
  readonly receipt: (
    commandId: string,
  ) => Effect.Effect<CommandReceipt | undefined, StoreUnavailable>;
  readonly recordReceipt: (receipt: CommandReceipt) => Effect.Effect<void, StoreUnavailable>;
  /** Next source sequence for a workspace: one past the highest folded event. */
  readonly nextSequence: (workspaceId: string) => Effect.Effect<number, StoreUnavailable>;
}

export class IssueStore extends Context.Service<IssueStore, IssueStoreService>()(
  "issue-tracker/IssueStore",
) {}

/** Decode one durable JSON value into a maintained row, or fail with typed poison. */
export const restoreRow = (
  table: string,
  key: string,
  json: string,
): Effect.Effect<IssueRow, StoreRestorePoison> =>
  Effect.try({
    try: () => decodeIssueRow(JSON.parse(json)),
    catch: (cause) =>
      new StoreRestorePoison({
        table,
        key,
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

interface WorkspaceMemory {
  rows: Map<string, string>;
  state: Map<string, string>;
  checkpoint?: string;
  published?: string;
  nextSequence: number;
}

export interface MemoryStoreOptions {
  /**
   * Durable values to start from, as raw JSON text.
   *
   * Tests use this to plant a malformed row and prove the restore path fails
   * typed instead of serving it.
   */
  readonly preload?: Readonly<Record<string, Readonly<Record<string, string>>>>;
}

/**
 * The in-memory store.
 *
 * It is not a stub: it implements the same commit ordering and the same typed
 * restore path as SQLite, so the declaration runs unchanged on both.
 */
export const memoryLayer = (options: MemoryStoreOptions = {}): Layer.Layer<IssueStore> =>
  Layer.sync(IssueStore, () => {
    const workspaces = new Map<string, WorkspaceMemory>();
    const receipts = new Map<string, CommandReceipt>();

    const workspace = (workspaceId: string): WorkspaceMemory => {
      const existing = workspaces.get(workspaceId);
      if (existing !== undefined) return existing;
      const created: WorkspaceMemory = { rows: new Map(), state: new Map(), nextSequence: 0 };
      for (const [key, json] of Object.entries(options.preload?.[workspaceId] ?? {})) {
        created.rows.set(key, json);
        created.state.set(key, json);
      }
      workspaces.set(workspaceId, created);
      return created;
    };

    return IssueStore.of({
      progress: Effect.fn("IssueStore.progress")((workspaceId: string) =>
        Effect.sync(() => {
          const memory = workspace(workspaceId);
          return { checkpoint: memory.checkpoint, published: memory.published };
        }),
      ),
      reducerStates: Effect.fn("IssueStore.reducerStates")(function* (
        workspaceId: string,
        keys: readonly string[],
      ) {
        const memory = workspace(workspaceId);
        const restored = new Map<string, IssueRow>();
        for (const key of keys) {
          const json = memory.state.get(key);
          if (json === undefined) continue;
          restored.set(key, yield* restoreRow("reducer_state", key, json));
        }
        return restored;
      }),
      rows: Effect.fn("IssueStore.rows")(function* (workspaceId: string) {
        const memory = workspace(workspaceId);
        const restored: IssueRow[] = [];
        for (const [key, json] of [...memory.rows].sort(([left], [right]) =>
          left.localeCompare(right),
        )) {
          restored.push(yield* restoreRow("view_rows", key, json));
        }
        return restored;
      }),
      commit: Effect.fn("IssueStore.commit")((workspaceId: string, input: CommitInput) =>
        Effect.sync(() => {
          const memory = workspace(workspaceId);
          for (const [key, row] of input.rows) {
            const json = JSON.stringify(row);
            memory.rows.set(key, json);
            memory.state.set(key, json);
          }
          memory.checkpoint = input.checkpoint;
          memory.nextSequence = Math.max(memory.nextSequence, input.nextSequence);
        }),
      ),
      markPublished: Effect.fn("IssueStore.markPublished")(
        (workspaceId: string, position: string) =>
          Effect.sync(() => {
            workspace(workspaceId).published = position;
          }),
      ),
      receipt: Effect.fn("IssueStore.receipt")((commandId: string) =>
        Effect.sync(() => receipts.get(commandId)),
      ),
      recordReceipt: Effect.fn("IssueStore.recordReceipt")((receipt: CommandReceipt) =>
        Effect.sync(() => {
          if (!receipts.has(receipt.commandId)) receipts.set(receipt.commandId, receipt);
        }),
      ),
      nextSequence: Effect.fn("IssueStore.nextSequence")((workspaceId: string) =>
        Effect.sync(() => workspace(workspaceId).nextSequence),
      ),
    });
  });

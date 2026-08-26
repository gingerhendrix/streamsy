/**
 * The second generated checked-sink binding, adapted to the UI lifecycle.
 *
 * It is a separate connection to a separate sink rather than a second
 * collection on the board's: `boardIssues` and `boardLabelCounts` are two
 * contracts with two fingerprints, published from two State streams, and a
 * client that resumed both on one session could not tell which of them a reset
 * applied to.
 */
import {
  memoryResumeStore,
  type ResumeStore,
  type StateSinkConnection,
  type StateSinkStatus,
} from "@streamsy/tanstack-db";
import {
  boardLabelCountsBinding,
  type BoardLabelCountsParams,
  type BoardLabelCountsRow,
} from "../generated/label-counts.ts";

export type LabelCountsDb = StateSinkConnection<
  typeof boardLabelCountsBinding.descriptor.state
>["db"];
export type LabelCountsStatus = StateSinkStatus;
export type { BoardLabelCountsRow };

export interface LabelCountsConnection {
  readonly db: LabelCountsDb;
  readonly preload: () => Promise<void>;
  readonly close: () => void;
}

export interface LabelCountsConnectionOptions {
  readonly workspaceId: BoardLabelCountsParams["workspaceId"];
  readonly origin: string;
  readonly onStatus: (status: LabelCountsStatus) => void;
  readonly resumeStore?: ResumeStore;
  readonly fetch?: typeof globalThis.fetch;
}

export function createLabelCountsConnection(
  options: LabelCountsConnectionOptions,
): LabelCountsConnection {
  const resumeStore = options.resumeStore ?? memoryResumeStore();
  const transport = boardLabelCountsBinding.createTransport({
    params: { workspaceId: options.workspaceId },
    origin: options.origin,
    resumeStore,
    onStatus: options.onStatus,
    fetch: options.fetch,
  });
  const connection = boardLabelCountsBinding.connect({ transport, resumeStore });
  return {
    db: connection.db,
    preload: connection.preload,
    close: connection.dispose,
  };
}

/** Labels in a stable order: most-used first, then by name. */
export function sortLabelCounts(
  rows: readonly BoardLabelCountsRow[],
): readonly BoardLabelCountsRow[] {
  return [...rows].sort(
    (left, right) =>
      right.issueCount - left.issueCount || left.labelName.localeCompare(right.labelName),
  );
}

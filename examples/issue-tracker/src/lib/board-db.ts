/** Generated checked-sink binding adapted to the issue-tracker UI lifecycle. */
import {
  memoryResumeStore,
  type ResumeStore,
  type StateSinkConnection,
  type StateSinkStatus,
} from "@streamsy/tanstack-db";
import {
  boardIssuesBinding,
  type BoardIssuesParams,
  type BoardIssuesRow,
} from "../generated/board-issues.ts";

export type BoardDb = StateSinkConnection<typeof boardIssuesBinding.descriptor.state>["db"];
export type SinkStatus = StateSinkStatus;

export interface BoardConnection {
  readonly db: BoardDb;
  readonly preload: () => Promise<void>;
  readonly close: () => void;
}

export interface BoardConnectionOptions {
  readonly workspaceId: BoardIssuesParams["workspaceId"];
  readonly origin: string;
  readonly onStatus: (status: SinkStatus) => void;
  readonly resumeStore?: ResumeStore;
  readonly fetch?: typeof globalThis.fetch;
}

export function createBoardConnection(options: BoardConnectionOptions): BoardConnection {
  const resumeStore = options.resumeStore ?? memoryResumeStore();
  const transport = boardIssuesBinding.createTransport({
    params: { workspaceId: options.workspaceId },
    origin: options.origin,
    resumeStore,
    onStatus: options.onStatus,
    fetch: options.fetch,
  });
  const connection = boardIssuesBinding.connect({ transport, resumeStore });
  return {
    db: connection.db,
    preload: connection.preload,
    close: connection.dispose,
  };
}

export function sortRows(rows: readonly BoardIssuesRow[]): readonly BoardIssuesRow[] {
  return rows.toSorted(
    (left, right) =>
      right.updatedAt.localeCompare(left.updatedAt) || left.issueId.localeCompare(right.issueId),
  );
}

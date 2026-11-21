export { CurrentOwner, isForeignOwner, attachOwner, ownerOf } from "./owner.ts";

/** Internal composition contract between storage hosts and the projection checkpoint store. */
export {
  OWNER_RECORDS_LIST_UNSUPPORTED,
  OwnerRecords,
  type OwnerRecordsApi,
} from "./owner-records.ts";

/** The producer id limit of storage, shared with projection producer ids. */
export { MAX_PRODUCER_ID_LENGTH } from "./limits.ts";

/** Hashes a key over a storage limit. Projection producer ids and SQL owner keys use it. */
export { fitKey } from "./fit-key.ts";

/** Internal contract between storage hosts and durable projection marks. */
export {
  CommitHooks,
  commitHookRegistry,
  type CommitHook,
  type CommitHooksApi,
} from "./commit-hooks.ts";

export {
  ProjectionMarks,
  type MarkFailure,
  type PendingMark,
  type ProjectionMarkSummary,
  type ProjectionMarksApi,
} from "./projection-marks.ts";

/** The parameter names from the route grammar, shared with projection binding. */
export { compileSegments } from "../toolkit/route-internal.ts";

export { memoryProjectionMarks } from "../storage/memory/marks.ts";

/** Storage message offsets consumed by projection reads. */
export { attachOffsets, offsetsOf } from "./read-offsets.ts";

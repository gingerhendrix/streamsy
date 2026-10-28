export { CurrentOwner, isForeignOwner, attachOwner, ownerOf } from "./owner.ts";

/** Internal composition contract between storage hosts and the projection checkpoint store. */
export { OwnerRecords, type OwnerRecordsApi } from "./owner-records.ts";

/** The producer id limit of storage, shared with projection producer ids. */
export { MAX_PRODUCER_ID_LENGTH } from "./limits.ts";

/** The parameter names from the route grammar, shared with projection binding. */
export { compileSegments } from "../toolkit/route-internal.ts";

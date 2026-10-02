export { CurrentOwner, isForeignOwner, attachOwner, ownerOf } from "./owner.ts";

/** Internal composition contract between storage hosts and the projection checkpoint store. */
export { OwnerRecords, type OwnerRecordsApi } from "./owner-records.ts";

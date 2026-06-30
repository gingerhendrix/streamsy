/** @streamsy/storage-fs */
export { createFsStorageAdapter } from "./adapter.ts";
export type { FsStorageAdapter, FsStorageAdapterOptions } from "./adapter.ts";
export { FsStreamState } from "./state.ts";
export type { FsStreamStateOptions, FsExpiryHandler } from "./state.ts";
export type { LockOptions } from "./lock.ts";
export {
  decodeEnvelope,
  encodeEnvelope,
  encodeStreamId,
  isJsonContentType,
  JSON_CONTENT_TYPE,
} from "./codec.ts";

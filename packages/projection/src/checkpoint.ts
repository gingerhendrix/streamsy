/** Storage adapter author seam. Kernel restoration and range helpers stay private. */
export {
  CheckpointRecord,
  CheckpointStore,
  fromStore,
  recordKey,
  type CheckpointStoreApi,
  type EncodedStore,
  type Loaded,
  type ProjectionKey,
} from "./checkpoint-store.ts";

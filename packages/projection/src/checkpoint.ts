/** Storage adapter author seam. Kernel restoration and range helpers stay private. */
export {
  CheckpointRecord,
  CheckpointStore,
  fromStore,
  recordKey,
  type CheckpointStoreApi,
  type EncodedStore,
  type ListedBinding,
  type Loaded,
  type ProjectionKey,
  type RouteRecords,
  type RoutedStreamRecord,
} from "./checkpoint-store.ts";

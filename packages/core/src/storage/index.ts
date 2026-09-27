// The accepted storage contract explicitly names StorageShape.
/* oxlint-disable anti-slop/no-shape-in-symbol-names */
export { Storage, type StorageShape } from "./storage.ts";
export { StorageCapabilities } from "./capabilities.ts";
export {
  ProducerPrecondition,
  Operation,
  Mutation,
  OperationResult,
  MutationApplied,
  MutationRejected,
  Operations,
  OperationResults,
  Mutations,
} from "./mutation.ts";
export * as Memory from "./memory/streams.ts";

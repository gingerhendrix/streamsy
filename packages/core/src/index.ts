/** @streamsy/core */
export { createStreamProtocol, StreamProtocol, ZERO_OFFSET } from "./protocol.ts";
export { createMemoryStorageAdapter } from "./storage/memory/adapter.ts";
export type { MemoryStorageAdapterOptions } from "./storage/memory/adapter.ts";
export { bindStream } from "./protocol/helpers/bind-stream.ts";
export type { BoundStream } from "./protocol/helpers/bind-stream.ts";
export { createHttpHandler, HttpHandler } from "./http.ts";
export { createReadOnlyHttpHandler, ReadOnlyHttpHandler } from "./read-only-http.ts";

export {
  isNotSupported,
  isNotSupportedError,
  notSupported,
  notSupportedFromError,
  NotSupportedError,
  unsupported,
} from "./types/storage-adapter.ts";
// Storage-seam building blocks for adapter authors: `runAwaitChangeLoop` is the
// contract-faithful level-triggered `awaitChange` loop (an adapter supplies
// `readRecord` + `waitForWake`); `buildChangeSnapshot` / `changeSnapshotDiffers`
// are the shared primitives it is built from; `compareOffsets` implements the
// seam's lexicographic offset-order guarantee.
export { buildChangeSnapshot, changeSnapshotDiffers } from "./protocol/helpers/change-snapshot.ts";
export { runAwaitChangeLoop } from "./protocol/helpers/await-change-loop.ts";
export type { AwaitChangeLoopDeps } from "./protocol/helpers/await-change-loop.ts";
export {
  compareOffsets,
  defaultOffsetGenerator,
  InvalidGeneratedOffsetError,
} from "./protocol/helpers/offset-generator.ts";
export type { OffsetGenerator } from "./protocol/helpers/offset-generator.ts";
export { maybeNotSupportedResponse, notSupportedResponse } from "./http/not-supported.ts";

// Reusable storage-adapter conformance kit (testing utility for adapter authors).
export { runStorageAdapterContract } from "./testing/storage-adapter-contract.ts";
export type {
  MakeStorageAdapter,
  StorageAdapterContractHarness,
} from "./testing/storage-adapter-contract.ts";

export type { StreamProtocolDeps, StreamProtocolOptions } from "./protocol.ts";
export type {
  HttpHandlerInterface,
  HttpHandlerOptions,
  ReadOnlyHttpHandlerOptions,
} from "./http/types.ts";

export type {
  StreamProtocolFactory,
  CommitEvent,
  AfterCommitHook,
  CommitObservable,
  ProtocolStream,
  ProtocolGetResult,
  CreateOptions,
  CreateResult,
  CreateConflictReason,
  AppendOptions,
  AppendResult,
  AppendConflictReason,
  ProducerOptions,
  ReadOptions,
  ReadResult,
  ReadLiveOptions,
  ReadLiveResult,
  MetadataResult,
  DeleteResult,
} from "./types/protocol.ts";

export type {
  StreamId,
  Offset,
  StreamConfig,
  StreamLifecycleState,
  StreamRecord,
  StreamRecordPatch,
  StoredMessage,
  ProducerState,
  ListMessagesOptions,
  StreamChangeSnapshot,
  AwaitChangeOptions,
  AwaitChangeResult,
  Clock,
} from "./types/storage.ts";

export type {
  StorageAdapter,
  StreamReader,
  StreamAppender,
  StreamLiveWaiter,
  StreamExpiryScheduler,
  AppendPlan,
  CreatePlan,
  ForkPlan,
  DeletePlan,
  StorageAppendResult,
  StorageCreateResult,
  StorageForkResult,
  StorageDeleteResult,
  NotSupportedResult,
} from "./types/storage-adapter.ts";

export {
  cascadeReclaim,
  copyOnForkReclaim,
  plainPurge,
  refCountLineage,
  reverseIndexLineage,
  ttlOnlyReclaim,
} from "./storage/strategies/index.ts";
export type { DependentsQuery, LineagePolicy, LineageStore } from "./storage/strategies/index.ts";

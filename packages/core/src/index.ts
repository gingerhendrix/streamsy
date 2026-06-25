/** @streamsy/core */
export { createStreamProtocol, StreamProtocol, ZERO_OFFSET } from "./protocol.ts";
export { createMemoryStreamFactory } from "./storage/memory/factory.ts";
export type { MemoryStreamFactoryOptions } from "./storage/memory/factory.ts";
export { createHttpHandler, HttpHandler } from "./http.ts";
export { createReadOnlyHttpHandler, ReadOnlyHttpHandler } from "./read-only-http.ts";

export {
  isNotSupported,
  isNotSupportedError,
  notSupported,
  notSupportedFromError,
  NotSupportedError,
  unsupported,
} from "./types/factory.ts";
export { maybeNotSupportedResponse, notSupportedResponse } from "./http/not-supported.ts";

export type { StreamProtocolDeps, StreamProtocolOptions } from "./protocol.ts";
export type {
  HttpHandlerInterface,
  HttpHandlerOptions,
  ReadOnlyHttpHandlerOptions,
} from "./http/types.ts";

export type {
  StreamProtocolFactory,
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
  StreamEventType,
  WaitForEventOptions,
  WaitForEventResult,
  Clock,
} from "./types/storage.ts";

export type {
  Stream,
  StreamFactory,
  StreamReader,
  StreamMutator,
  StreamEventHub,
  StreamExpiryScheduler,
  AfterCommitEffects,
  MutationPlan,
  CommitResult,
  CreatePlan,
  CreateCommit,
  ForkPlan,
  ForkCommit,
  DeletePlan,
  DeleteCommit,
  NotSupportedResult,
} from "./types/factory.ts";

export {
  cascadeReclaim,
  copyOnForkReclaim,
  plainPurge,
  refCountLineage,
  reverseIndexLineage,
  ttlOnlyReclaim,
} from "./storage/strategies/index.ts";
export type { DependentsQuery, LineagePolicy, LineageStore } from "./storage/strategies/index.ts";

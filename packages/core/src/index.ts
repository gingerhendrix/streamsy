/** @streamsy/core */
export { createStreamProtocol, StreamProtocol, ZERO_OFFSET } from "./protocol.ts";
export { createHttpHandler, HttpHandler } from "./http.ts";

export { composeStream } from "./factory/compose-stream.ts";
export {
  requireEventHub,
  requireExpiryScheduler,
  requireMutationCoordinator,
  requireProducerStore,
  requireReferenceTracker,
} from "./factory/require-deps.ts";
export { isNotSupported, notSupported } from "./types/factory.ts";
export { maybeNotSupportedResponse, notSupportedResponse } from "./http/not-supported.ts";

export type { StreamProtocolDeps, StreamProtocolOptions } from "./protocol.ts";
export type { HttpHandlerInterface, HttpHandlerOptions } from "./http/types.ts";

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
  CreateStreamRecordResult,
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
  StreamRecordStore,
  StreamMessageStore,
  StreamProducerStore,
  StreamReferenceTracker,
  StreamMutationCoordinator,
  StreamEventHub,
  StreamExpiryScheduler,
  ComposedStreamDeps,
  NotSupportedResult,
} from "./types/factory.ts";

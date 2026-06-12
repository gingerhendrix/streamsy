/** @streamsy/core */
export { createStreamProtocol, StreamProtocol, ZERO_OFFSET } from "./protocol.ts";
export { createMemoryStreamFactory } from "./storage/memory/factory.ts";
export type { MemoryStreamFactoryOptions } from "./storage/memory/factory.ts";
export { createHttpHandler, HttpHandler } from "./http.ts";
export { createReadOnlyHttpHandler, ReadOnlyHttpHandler } from "./read-only-http.ts";
export {
  createJsonProtocol,
  JsonProtocol,
  JsonStream,
  JSON_CONTENT_TYPE,
  JsonValidationError,
} from "./json.ts";
export {
  createDurableStateProtocol,
  DurableStateProtocol,
  DurableStateStream,
} from "./durable-state.ts";

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
  JsonAppendOptions,
  JsonCodec,
  JsonCreateOptions,
  JsonCreateResult,
  JsonGetResult,
  JsonReadLiveResult,
  JsonReadResult,
  JsonSchema,
  JsonStoredMessage,
} from "./json.ts";

export type {
  ChangeMessage,
  CollectionValue,
  ControlMessage,
  DeleteMessage,
  DurableState,
  DurableStateChangeHeaders,
  DurableStateCollectionDef,
  DurableStateControl,
  DurableStateControlHeaders,
  DurableStateCreateOptions,
  DurableStateCreateResult,
  DurableStateGetResult,
  DurableStateMessage,
  DurableStateOperation,
  DurableStateOperationWithExtensions,
  DurableStateSchemaMap,
  DurableStateUserHeaders,
  InsertMessage,
  UpdateMessage,
  ValuesByWireType,
} from "./durable-state.ts";

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
  NotSupportedResult,
} from "./types/factory.ts";

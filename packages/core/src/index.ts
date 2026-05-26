/** @streamsy/core */
export { createStreamProtocol, StreamProtocol, ZERO_OFFSET } from "./protocol.ts";
export { createHttpHandler, HttpHandler } from "./http.ts";

export type { StreamProtocolOptions } from "./protocol.ts";
export type { HttpHandlerInterface, HttpHandlerOptions } from "./http/types.ts";

export type {
  StreamProtocolInterface,
  StreamStoreFactory,
  CreateOptions,
  CreateResult,
  CreateConflictReason,
  AppendOptions,
  AppendResult,
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
  StreamStoreAdapter,
  StoredMessage,
  ProducerState,
  ListMessagesOptions,
  StreamEventType,
  WaitForEventOptions,
  WaitForEventResult,
  Clock,
} from "./types/storage.ts";

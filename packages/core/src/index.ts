/** @streamsy/core */
export { StreamProtocol, ZERO_OFFSET } from "./protocol.ts";
export { HttpHandler } from "./http.ts";

export type {
  StreamProtocolInterface,
  StreamStoreFactory,
  CreateOptions,
  CreateResult,
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
  StreamStoreAdapter,
  StoredMessage,
  ProducerState,
  ListMessagesOptions,
  StreamEventType,
  WaitForEventOptions,
  WaitForEventResult,
  Clock,
} from "./types/storage.ts";

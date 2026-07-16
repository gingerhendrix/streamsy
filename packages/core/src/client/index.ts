/** Transport-neutral client seam: types, the shared read session, and the direct adapter. */

export { directProtocolClient, hasStreamsyProtocol } from "./direct/client.ts";
/** The single read-session implementation, shared by every adapter (incl. `@streamsy/client`). */
export { ClientReadSession } from "./read-session.ts";

export type {
  AppendStreamOptions,
  ByteStreamBatch,
  ClientAppendResult,
  ClientCloseResult,
  ClientCreateResult,
  ClientErrorCode,
  ClientFailure,
  ClientHeadResult,
  ClientLiveMode,
  ClientReadResult,
  ClientRequestOptions,
  CloseStreamOptions,
  CreateStreamOptions,
  JsonPrimitive,
  JsonStreamBatch,
  JsonValue,
  ReadEndResult,
  ReadStreamOptions,
  StreamBatch,
  StreamBatchMeta,
  StreamOffset,
  StreamProtocolClient,
  StreamProtocolHandle,
  StreamReadSession,
  StreamsyProtocolClient,
  TextStreamBatch,
} from "./types.ts";

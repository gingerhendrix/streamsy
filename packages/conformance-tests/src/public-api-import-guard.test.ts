import { describe, expect, test } from "vitest";
import { createHttpHandler, createStreamProtocol, ZERO_OFFSET } from "@streamsy/core";
import type {
  AppendOptions,
  AppendResult,
  Clock,
  CreateConflictReason,
  CreateOptions,
  CreateResult,
  CreateStreamRecordResult,
  DeleteResult,
  HttpHandlerInterface,
  HttpHandlerOptions,
  ListMessagesOptions,
  MetadataResult,
  Offset,
  ProducerOptions,
  ProducerState,
  ReadLiveOptions,
  ReadLiveResult,
  ReadOptions,
  ReadResult,
  StoredMessage,
  StreamConfig,
  StreamEventType,
  StreamId,
  StreamLifecycleState,
  StreamProtocolInterface,
  StreamProtocolOptions,
  StreamRecord,
  StreamRecordPatch,
  StreamStoreAdapter,
  StreamStoreFactory,
  WaitForEventOptions,
  WaitForEventResult,
} from "@streamsy/core";
import {
  createMemoryStreamStore,
  MemoryStreamStore,
  type ProducerState as MemoryPackageProducerState,
  type StoredMessage as MemoryPackageStoredMessage,
  type StreamRecord as MemoryPackageStreamRecord,
  type StreamStoreAdapter as MemoryPackageStreamStoreAdapter,
} from "@streamsy/storage-memory";
import type {
  DurableObjectStreamStorage,
  DurableObjectStreamStoreAdapter,
  DurableObjectStreamStoreEnv,
  ProducerState as DurableObjectPackageProducerState,
  StoredMessage as DurableObjectPackageStoredMessage,
  StreamRecord as DurableObjectPackageStreamRecord,
  StreamStoreAdapter as DurableObjectPackageStreamStoreAdapter,
} from "@streamsy/storage-durable-object";

export type PublicCoreProtocolImportGuard = {
  protocol: StreamProtocolInterface;
  protocolOptions: StreamProtocolOptions;
  createOptions: CreateOptions;
  createResult: CreateResult;
  createConflictReason: CreateConflictReason;
  appendOptions: AppendOptions;
  appendResult: AppendResult;
  producerOptions: ProducerOptions;
  readOptions: ReadOptions;
  readResult: ReadResult;
  readLiveOptions: ReadLiveOptions;
  readLiveResult: ReadLiveResult;
  metadataResult: MetadataResult;
  deleteResult: DeleteResult;
};

export type PublicCoreHttpImportGuard = {
  handler: HttpHandlerInterface;
  options: HttpHandlerOptions;
};

export type PublicCoreStorageImportGuard = {
  streamId: StreamId;
  offset: Offset;
  config: StreamConfig;
  lifecycle: StreamLifecycleState;
  record: StreamRecord;
  patch: StreamRecordPatch;
  createRecordResult: CreateStreamRecordResult;
  adapter: StreamStoreAdapter;
  factory: StreamStoreFactory;
  message: StoredMessage;
  producerState: ProducerState;
  listOptions: ListMessagesOptions;
  eventType: StreamEventType;
  waitOptions: WaitForEventOptions;
  waitResult: WaitForEventResult;
  clock: Clock;
};

export type PublicStoragePackageImportGuard = {
  memoryAdapter: MemoryPackageStreamStoreAdapter;
  memoryRecord: MemoryPackageStreamRecord;
  memoryMessage: MemoryPackageStoredMessage;
  memoryProducerState: MemoryPackageProducerState;
  durableObjectStorage: DurableObjectStreamStorage;
  durableObjectAdapter: DurableObjectStreamStoreAdapter;
  durableObjectEnv: DurableObjectStreamStoreEnv;
  durableObjectPackageAdapter: DurableObjectPackageStreamStoreAdapter;
  durableObjectRecord: DurableObjectPackageStreamRecord;
  durableObjectMessage: DurableObjectPackageStoredMessage;
  durableObjectProducerState: DurableObjectPackageProducerState;
};

describe("public API import guard", () => {
  test("wires core factories through package entrypoints", () => {
    const memoryStore = createMemoryStreamStore();
    const directMemoryStore: StreamStoreAdapter = new MemoryStreamStore();
    const clock: Clock = {
      now: () => 0,
      date: (value?: number | string) => new Date(value ?? 0),
    };
    const protocolOptions: StreamProtocolOptions = {
      clock,
      longPollTimeoutMs: 10,
    };

    const protocol: StreamProtocolInterface = createStreamProtocol(memoryStore, protocolOptions);
    const httpOptions: HttpHandlerOptions = {
      protocol,
      pathPrefix: "/ds",
      maxMessageSize: 1024,
    };
    const http: HttpHandlerInterface = createHttpHandler(httpOptions);

    expect(http.fetch).toBeTypeOf("function");
    expect(directMemoryStore).toBeInstanceOf(MemoryStreamStore);
    expect(ZERO_OFFSET).toBeTypeOf("string");
  });
});

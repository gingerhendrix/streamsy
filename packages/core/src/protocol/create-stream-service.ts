/** Create-side orchestration for one storage-bound stream. */

import type { CreateOptions, CreateOutcome } from "../types/protocol.ts";
import type { CreateStreamRecordResult, StreamId, StreamRecord } from "../types/storage.ts";
import { configMatches } from "./helpers/create-config-matcher.ts";
import { frameMessages } from "./helpers/message-framer.ts";

/** Narrow record-store view the create service depends on. */
export interface CreateStreamStore {
  getRecord(): Promise<StreamRecord | null>;
  createRecord(record: StreamRecord): Promise<CreateStreamRecordResult>;
}

export interface CreateStreamMutators {
  newRecord(streamId: StreamId, contentType: string, options: CreateOptions): StreamRecord;
  scheduleExpiry(record: StreamRecord): Promise<void>;
  appendMessages(streamId: StreamId, record: StreamRecord, data: Uint8Array[]): Promise<string>;
  closeRecord(streamId: StreamId, record: StreamRecord, data: Uint8Array[]): Promise<string>;
  createFork(streamId: StreamId, options: CreateOptions): Promise<CreateOutcome>;
}

export interface CreateStreamServiceDeps {
  store: CreateStreamStore;
  mutators: CreateStreamMutators;
}

export class CreateStreamService {
  constructor(private deps: CreateStreamServiceDeps) {}

  async execute(
    streamId: StreamId,
    record: StreamRecord | null,
    options: CreateOptions,
  ): Promise<CreateOutcome> {
    if (record) return this.resultForExisting(record, options);
    if (options.forkedFrom) return this.deps.mutators.createFork(streamId, options);

    const contentType = options.contentType ?? "application/octet-stream";
    const initialMessages = options.initialData
      ? frameMessages(options.initialData, contentType)
      : [];
    const wantClosed = options.closed === true;

    const newRecord = this.deps.mutators.newRecord(streamId, contentType, options);
    const createResult = await this.deps.store.createRecord(newRecord);
    if (createResult.status === "exists")
      return this.resultForExisting(createResult.record, options);
    await this.deps.mutators.scheduleExpiry(newRecord);

    let final = newRecord.currentOffset;
    if (initialMessages.length > 0) {
      final = await this.deps.mutators.appendMessages(streamId, newRecord, initialMessages);
    }
    if (wantClosed) {
      const latest = (await this.deps.store.getRecord()) ?? newRecord;
      final = await this.deps.mutators.closeRecord(streamId, latest, []);
    }

    return {
      status: "created",
      nextOffset: final,
      contentType,
      closed: wantClosed,
    };
  }

  private resultForExisting(existing: StreamRecord, options: CreateOptions): CreateOutcome {
    if (existing.lifecycle.softDeleted) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "soft-deleted",
      };
    }
    if (!configMatches(existing, options)) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "config-mismatch",
      };
    }
    return {
      status: "exists",
      nextOffset: existing.currentOffset,
      contentType: existing.config.contentType,
      closed: existing.lifecycle.closed === true,
    };
  }
}

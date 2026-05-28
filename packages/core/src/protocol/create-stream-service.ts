/** Create-side orchestration for one storage-bound stream. */

import type { CreateOptions, CreateResult } from "../types/protocol.ts";
import type { Stream } from "../types/factory.ts";
import type { StreamId, StreamRecord } from "../types/storage.ts";
import { configMatches } from "./helpers/create-config-matcher.ts";
import { frameMessages } from "./helpers/message-framer.ts";

export interface CreateStreamMutators {
  newRecord(streamId: StreamId, contentType: string, options: CreateOptions): StreamRecord;
  scheduleExpiry(record: StreamRecord): Promise<void>;
  appendMessages(streamId: StreamId, record: StreamRecord, data: Uint8Array[]): Promise<string>;
  closeRecord(streamId: StreamId, record: StreamRecord, data: Uint8Array[]): Promise<string>;
  createFork(streamId: StreamId, options: CreateOptions): Promise<CreateResult>;
}

export class CreateStreamService {
  constructor(
    private stream: Stream,
    private mutators: CreateStreamMutators,
  ) {}

  async execute(streamId: StreamId, options: CreateOptions): Promise<CreateResult> {
    const existing = await this.stream.getRecord();

    if (existing) return this.resultForExisting(existing, options);
    if (options.forkedFrom) return this.mutators.createFork(streamId, options);

    const contentType = options.contentType ?? "application/octet-stream";
    const initialMessages = options.initialData
      ? frameMessages(options.initialData, contentType)
      : [];
    const wantClosed = options.closed === true;

    const record = this.mutators.newRecord(streamId, contentType, options);
    const createResult = await this.stream.createRecord(record);
    if (createResult.status === "exists")
      return this.resultForExisting(createResult.record, options);
    await this.mutators.scheduleExpiry(record);

    let final = record.currentOffset;
    if (initialMessages.length > 0) {
      final = await this.mutators.appendMessages(streamId, record, initialMessages);
    }
    if (wantClosed) {
      const latest = (await this.stream.getRecord()) ?? record;
      final = await this.mutators.closeRecord(streamId, latest, []);
    }

    return {
      status: "created",
      stream: undefined as never,
      nextOffset: final,
      contentType,
      closed: wantClosed,
    };
  }

  private resultForExisting(existing: StreamRecord, options: CreateOptions): CreateResult {
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
      stream: undefined as never,
      nextOffset: existing.currentOffset,
      contentType: existing.config.contentType,
      closed: existing.lifecycle.closed === true,
    };
  }
}

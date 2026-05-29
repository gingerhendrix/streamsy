/** Fork-side orchestration using explicit storage-stream resolution. */

import type { CreateOptions, CreateOutcome } from "../../types/protocol.ts";
import type { Stream } from "../../types/factory.ts";
import type { StreamId, StreamRecord } from "../../types/storage.ts";
import { ZERO_OFFSET, compareOffsets, isValidOffset } from "./offset-generator.ts";
import { contentTypeMatches } from "./content-type-matcher.ts";
import { frameMessages } from "./message-framer.ts";

export interface ForkDescriptor {
  forkedFrom: StreamId;
  forkOffset: string;
}

export interface ForkServiceMutators {
  expireIfNeeded(stream: Stream): Promise<void>;
  newRecord(
    stream: Stream,
    contentType: string,
    options: CreateOptions,
    fork: ForkDescriptor,
  ): StreamRecord;
  scheduleExpiry(record: StreamRecord): Promise<void>;
  appendMessages(record: StreamRecord, data: Uint8Array[]): Promise<string>;
}

export type ResolveStorageStream = (streamId: StreamId) => Promise<Stream> | Stream;

export function resolveForkExpiry(
  opts: CreateOptions,
  source: StreamRecord,
): { ttlSeconds?: number; expiresAt?: string } {
  if (opts.ttlSeconds !== undefined) return { ttlSeconds: opts.ttlSeconds };
  if (opts.expiresAt) return { expiresAt: opts.expiresAt };
  if (source.config.ttlSeconds !== undefined) return { ttlSeconds: source.config.ttlSeconds };
  if (source.config.expiresAt) return { expiresAt: source.config.expiresAt };
  return {};
}

export interface ForkServiceDeps {
  resolve: ResolveStorageStream;
  mutators: ForkServiceMutators;
}

export class ForkService {
  constructor(private deps: ForkServiceDeps) {}

  async execute(targetStream: Stream, options: CreateOptions): Promise<CreateOutcome> {
    const sourcePath = options.forkedFrom!;
    const sourceStream = await this.deps.resolve(sourcePath);
    await this.deps.mutators.expireIfNeeded(sourceStream);
    const source = await sourceStream.getRecord();
    if (!source)
      return {
        status: "not-found",
        nextOffset: "",
        contentType: "",
        errorMessage: `Source stream not found: ${sourcePath}`,
      };
    if (source.lifecycle.softDeleted) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "fork-source-soft-deleted",
        errorMessage: `Source stream is soft-deleted: ${sourcePath}`,
      };
    }

    const forkOffset = options.forkOffset ?? source.currentOffset;
    if (!isValidOffset(forkOffset)) {
      return {
        status: "bad-request",
        nextOffset: "",
        contentType: "",
        errorMessage: "Invalid Stream-Fork-Offset format",
      };
    }
    if (
      compareOffsets(forkOffset, ZERO_OFFSET) < 0 ||
      compareOffsets(forkOffset, source.currentOffset) > 0
    ) {
      return {
        status: "bad-request",
        nextOffset: "",
        contentType: "",
        errorMessage: "Stream-Fork-Offset exceeds source tail",
      };
    }

    let contentType = options.contentType;
    if (!contentType || contentType.trim() === "") contentType = source.config.contentType;
    else if (!contentTypeMatches(contentType, source.config.contentType)) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "fork-content-type",
        errorMessage: "Fork Content-Type does not match source",
      };
    }

    const expiry = resolveForkExpiry(options, source);
    const record = this.deps.mutators.newRecord(
      targetStream,
      contentType,
      { ...options, ...expiry },
      {
        forkedFrom: sourcePath,
        forkOffset,
      },
    );
    const initialMessages = options.initialData
      ? frameMessages(options.initialData, contentType)
      : [];

    const createResult = await targetStream.createRecord(record);
    if (createResult.status === "created") await sourceStream.references?.incrementChildRefCount();
    if (createResult.status === "exists") {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "config-mismatch",
        errorMessage: `Stream already exists: ${targetStream.id}`,
      };
    }
    await this.deps.mutators.scheduleExpiry(record);
    let final = record.currentOffset;
    if (initialMessages.length > 0)
      final = await this.deps.mutators.appendMessages(record, initialMessages);
    return { status: "created", nextOffset: final, contentType };
  }
}

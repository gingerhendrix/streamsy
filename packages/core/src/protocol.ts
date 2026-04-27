/**
 * Protocol Layer Implementation
 *
 * Handles validation, JSON mode processing, cursor generation,
 * and orchestration between HTTP and storage layers.
 */

import type {
  StreamProtocolInterface,
  StorageFactory,
  CreateOptions,
  CreateResult,
  AppendOptions,
  AppendResult,
  ReadOptions,
  ReadResult,
  ReadLiveOptions,
  ReadLiveResult,
  MetadataResult,
  DeleteResult,
} from "./types/protocol.ts";
import type {
  ProducerState,
  StreamMetadata,
  StreamStorage,
} from "./types/storage.ts";

type ProducerValidation =
  | { kind: "accepted"; proposedState: ProducerState }
  | { kind: "duplicate"; lastSeq: number; epoch: number }
  | { kind: "stale-epoch"; currentEpoch: number }
  | { kind: "gap"; expectedSeq: number; receivedSeq: number }
  | { kind: "invalid-epoch-seq" };

const CURSOR_EPOCH = new Date("2024-10-09T00:00:00.000Z").getTime();
const CURSOR_INTERVAL_MS = 20_000;

export class StreamProtocol implements StreamProtocolInterface {
  constructor(private getStorage: StorageFactory) {}

  async create(
    streamId: string,
    options: CreateOptions
  ): Promise<CreateResult> {
    const storage = this.getStorage(streamId);
    const existing = await storage.getMetadata();

    if (existing) {
      if (!this.configMatches(existing, options)) {
        return { status: "conflict", nextOffset: "", contentType: "" };
      }
      const offset = await storage.getCurrentOffset();
      return {
        status: "exists",
        nextOffset: offset,
        contentType: existing.contentType,
      };
    }

    const contentType = options.contentType ?? "application/octet-stream";

    const nextOffset = await storage.createStream({
      contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      initialData: options.initialData
        ? this.processData(options.initialData, contentType)
        : undefined,
    });

    return { status: "created", nextOffset, contentType };
  }

  async append(
    streamId: string,
    options: AppendOptions
  ): Promise<AppendResult> {
    const storage = this.getStorage(streamId);

    if (options.producer) {
      const release = await storage.acquireProducerLock(
        options.producer.producerId
      );
      try {
        return await this.appendInner(storage, options);
      } finally {
        release();
      }
    }

    return this.appendInner(storage, options);
  }

  private async appendInner(
    storage: StreamStorage,
    options: AppendOptions
  ): Promise<AppendResult> {
    const metadata = await storage.getMetadata();

    if (!metadata) {
      return { status: "not-found" };
    }

    if (!this.contentTypeMatches(metadata.contentType, options.contentType)) {
      return { status: "conflict", conflictReason: "content-type" };
    }

    // Producer validation runs BEFORE Stream-Seq check so retries with both
    // producer headers AND Stream-Seq can return 204 (duplicate) instead of
    // failing the Stream-Seq conflict check.
    let producerValidation: ProducerValidation | undefined;
    if (options.producer) {
      const state = await storage.getProducerState(options.producer.producerId);
      producerValidation = this.validateProducer(
        state,
        options.producer.producerEpoch,
        options.producer.producerSeq
      );

      switch (producerValidation.kind) {
        case "duplicate":
          return {
            status: "duplicate",
            nextOffset: await storage.getCurrentOffset(),
            producerEpoch: producerValidation.epoch,
            producerSeq: producerValidation.lastSeq,
          };
        case "stale-epoch":
          return {
            status: "stale-epoch",
            currentEpoch: producerValidation.currentEpoch,
          };
        case "gap":
          return {
            status: "producer-gap",
            expectedSeq: producerValidation.expectedSeq,
            receivedSeq: producerValidation.receivedSeq,
          };
        case "invalid-epoch-seq":
          return { status: "invalid-epoch-seq" };
      }
    }

    if (options.seq && metadata.lastSeq && options.seq <= metadata.lastSeq) {
      return { status: "conflict", conflictReason: "sequence" };
    }

    const processed = this.processData(options.data, metadata.contentType);

    const nextOffset = await storage.append(processed, options.seq);

    if (producerValidation && producerValidation.kind === "accepted") {
      await storage.setProducerState(
        options.producer!.producerId,
        producerValidation.proposedState
      );
      return {
        status: "appended",
        nextOffset,
        producerEpoch: producerValidation.proposedState.epoch,
        producerSeq: producerValidation.proposedState.lastSeq,
      };
    }

    return { status: "appended", nextOffset };
  }

  private validateProducer(
    state: ProducerState | undefined,
    epoch: number,
    seq: number
  ): ProducerValidation {
    if (!state) {
      // First time we see this producer. Treat as a fresh epoch session.
      if (seq !== 0) {
        return { kind: "gap", expectedSeq: 0, receivedSeq: seq };
      }
      return { kind: "accepted", proposedState: { epoch, lastSeq: 0 } };
    }

    if (epoch < state.epoch) {
      return { kind: "stale-epoch", currentEpoch: state.epoch };
    }

    if (epoch > state.epoch) {
      if (seq !== 0) {
        return { kind: "invalid-epoch-seq" };
      }
      return { kind: "accepted", proposedState: { epoch, lastSeq: 0 } };
    }

    // Same epoch
    if (seq <= state.lastSeq) {
      return { kind: "duplicate", lastSeq: state.lastSeq, epoch: state.epoch };
    }
    if (seq === state.lastSeq + 1) {
      return {
        kind: "accepted",
        proposedState: { epoch, lastSeq: seq },
      };
    }
    return {
      kind: "gap",
      expectedSeq: state.lastSeq + 1,
      receivedSeq: seq,
    };
  }

  async read(
    streamId: string,
    options: ReadOptions
  ): Promise<ReadResult> {
    const storage = this.getStorage(streamId);
    const metadata = await storage.getMetadata();

    if (!metadata) {
      return {
        status: "not-found",
        messages: [],
        nextOffset: "",
        upToDate: false,
      };
    }

    const offset = this.normalizeOffset(options.offset);
    const { messages, nextOffset, upToDate } = await storage.read(offset);

    return { status: "ok", messages, nextOffset, upToDate };
  }

  async readLive(
    streamId: string,
    options: ReadLiveOptions
  ): Promise<ReadLiveResult> {
    const storage = this.getStorage(streamId);
    const metadata = await storage.getMetadata();

    if (!metadata) {
      return {
        status: "not-found",
        messages: [],
        nextOffset: "",
        upToDate: false,
        cursor: "",
      };
    }

    const { messages, nextOffset, timedOut } = await storage.readLive(
      options.offset,
      options.signal
    );

    return {
      status: timedOut ? "timeout" : "ok",
      messages,
      nextOffset,
      upToDate: true,
      cursor: this.generateCursor(options.cursor),
    };
  }

  async metadata(streamId: string): Promise<MetadataResult> {
    const storage = this.getStorage(streamId);
    const meta = await storage.getMetadata();
    if (!meta) return { status: "not-found" };

    return {
      status: "ok",
      contentType: meta.contentType,
      nextOffset: await storage.getCurrentOffset(),
      ttlSeconds: meta.ttlSeconds,
      expiresAt: meta.expiresAt,
    };
  }

  async delete(streamId: string): Promise<DeleteResult> {
    const storage = this.getStorage(streamId);
    const exists = await storage.getMetadata();
    if (!exists) return { status: "not-found" };

    await storage.deleteAll();
    return { status: "ok" };
  }

  // === Private helpers ===

  private processData(data: Uint8Array, contentType: string): Uint8Array[] {
    if (!contentType.toLowerCase().startsWith("application/json")) {
      return [data];
    }

    const text = new TextDecoder().decode(data);
    const parsed = JSON.parse(text);

    if (Array.isArray(parsed)) {
      if (parsed.length === 0) {
        return [];  // Return empty array instead of throwing
      }
      // Flatten one level
      return parsed.map((item) =>
        new TextEncoder().encode(JSON.stringify(item))
      );
    }

    return [new TextEncoder().encode(JSON.stringify(parsed))];
  }

  private contentTypeMatches(expected: string, actual: string): boolean {
    const normalize = (ct: string) => ct.toLowerCase().split(";")[0]!.trim();
    return normalize(expected) === normalize(actual);
  }

  private configMatches(
    existing: StreamMetadata,
    options: CreateOptions
  ): boolean {
    const contentType = options.contentType ?? "application/octet-stream";
    if (!this.contentTypeMatches(existing.contentType, contentType)) {
      return false;
    }

    if (existing.ttlSeconds !== options.ttlSeconds) {
      return false;
    }

    if (existing.expiresAt !== options.expiresAt) {
      return false;
    }

    return true;
  }

  private normalizeOffset(offset?: string): string | undefined {
    if (!offset || offset === "-1") return undefined;
    return offset;
  }

  private generateCursor(previous?: string): string {
    const now = Date.now();
    const currentInterval = Math.floor(
      (now - CURSOR_EPOCH) / CURSOR_INTERVAL_MS
    );

    if (!previous) {
      return String(currentInterval);
    }

    const previousInterval = parseInt(previous, 10);
    if (previousInterval < currentInterval) {
      return String(currentInterval);
    }

    // Add jitter: 1-180 intervals (20s each = 1-3600 seconds)
    const jitterIntervals = Math.max(1, Math.floor(Math.random() * 180));
    return String(previousInterval + jitterIntervals);
  }
}

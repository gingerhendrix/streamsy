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
import type { StreamMetadata } from "./types/storage.ts";

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
        closed: existing.closed === true,
      };
    }

    const contentType = options.contentType ?? "application/octet-stream";
    const wantClosed = options.closed === true;

    const initialMessages = options.initialData
      ? this.processData(options.initialData, contentType)
      : undefined;

    const nextOffset = await storage.createStream({
      contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      initialData: wantClosed ? undefined : initialMessages,
      closed: wantClosed,
    });

    let finalOffset = nextOffset;
    if (wantClosed) {
      finalOffset = await storage.close(initialMessages);
    }

    return {
      status: "created",
      nextOffset: finalOffset,
      contentType,
      closed: wantClosed,
    };
  }

  async append(
    streamId: string,
    options: AppendOptions
  ): Promise<AppendResult> {
    const storage = this.getStorage(streamId);
    const metadata = await storage.getMetadata();

    if (!metadata) {
      return { status: "not-found" };
    }

    const isClosed = metadata.closed === true;
    const wantClose = options.close === true;
    const hasBody = options.data.byteLength > 0;

    // Close-only request: empty body + Stream-Closed: true
    if (wantClose && !hasBody) {
      // Idempotent: closing already-closed stream returns 204 with Stream-Closed
      if (isClosed) {
        return {
          status: "ok",
          nextOffset: await storage.getCurrentOffset(),
          closed: true,
        };
      }
      // Close without appending data; ignore content-type for empty body
      const offset = await storage.close(undefined, options.seq);
      return { status: "ok", nextOffset: offset, closed: true };
    }

    // Stream is already closed and we have a body — error precedence:
    // closed wins over content-type and sequence.
    if (isClosed) {
      return {
        status: "conflict",
        conflictReason: "closed",
        closed: true,
        nextOffset: await storage.getCurrentOffset(),
      };
    }

    if (!this.contentTypeMatches(metadata.contentType, options.contentType)) {
      return { status: "conflict", conflictReason: "content-type" };
    }

    if (options.seq && metadata.lastSeq && options.seq <= metadata.lastSeq) {
      return { status: "conflict", conflictReason: "sequence" };
    }

    const processed = this.processData(options.data, metadata.contentType);

    if (wantClose) {
      const offset = await storage.close(processed, options.seq);
      return { status: "ok", nextOffset: offset, closed: true };
    }

    const nextOffset = await storage.append(processed, options.seq);
    return { status: "ok", nextOffset };
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
    const isClosed = metadata.closed === true;

    return {
      status: "ok",
      messages,
      nextOffset,
      upToDate,
      // Only signal closed when caller has reached the tail. Partial reads
      // (more data exists between response and final offset) MUST NOT carry
      // Stream-Closed.
      closed: isClosed && upToDate,
    };
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

    const isClosed = metadata.closed === true;

    // Closed stream: short-circuit to avoid waiting. Either return any
    // remaining data (which makes us up-to-date afterward) or signal EOF
    // immediately.
    if (isClosed) {
      const { messages, nextOffset, upToDate } = await storage.read(
        options.offset
      );
      return {
        status: messages.length > 0 ? "ok" : "timeout",
        messages,
        nextOffset,
        upToDate: true,
        cursor: this.generateCursor(options.cursor),
        closed: upToDate,
      };
    }

    const { messages, nextOffset, timedOut } = await storage.readLive(
      options.offset,
      options.signal
    );

    // The stream may have closed while we were waiting.
    const finalMeta = await storage.getMetadata();
    const finalClosed = finalMeta?.closed === true;
    const reachedTail = nextOffset === (await storage.getCurrentOffset());

    return {
      status: timedOut ? "timeout" : "ok",
      messages,
      nextOffset,
      upToDate: true,
      cursor: this.generateCursor(options.cursor),
      closed: finalClosed && reachedTail,
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
      closed: meta.closed === true,
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

    // Closure status is part of the configuration: PUT must match.
    const existingClosed = existing.closed === true;
    const wantClosed = options.closed === true;
    if (existingClosed !== wantClosed) {
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

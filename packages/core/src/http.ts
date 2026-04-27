/**
 * HTTP Handler
 *
 * Handles HTTP requests and routes them to the appropriate protocol methods.
 */

import type {
  ProducerOptions,
  StreamProtocolInterface,
} from "./types/protocol.ts";

const MAX_SAFE_PRODUCER_INT = Number.MAX_SAFE_INTEGER;
const NON_NEGATIVE_INT_RE = /^(0|[1-9]\d*)$/;

type ProducerHeaderResult =
  | { kind: "absent" }
  | { kind: "ok"; producer: ProducerOptions }
  | { kind: "invalid" };

function parseProducerHeaders(request: Request): ProducerHeaderResult {
  const id = request.headers.get("producer-id");
  const epoch = request.headers.get("producer-epoch");
  const seq = request.headers.get("producer-seq");

  const present = [id, epoch, seq].filter((v) => v !== null).length;
  if (present === 0) return { kind: "absent" };
  if (present !== 3) return { kind: "invalid" };

  if (id!.length === 0) return { kind: "invalid" };

  const parsedEpoch = parseProducerInt(epoch!);
  const parsedSeq = parseProducerInt(seq!);
  if (parsedEpoch === null || parsedSeq === null) return { kind: "invalid" };

  return {
    kind: "ok",
    producer: {
      producerId: id!,
      producerEpoch: parsedEpoch,
      producerSeq: parsedSeq,
    },
  };
}

function parseProducerInt(raw: string): number | null {
  if (!NON_NEGATIVE_INT_RE.test(raw)) return null;
  const n = Number(raw);
  if (!Number.isFinite(n) || n > MAX_SAFE_PRODUCER_INT) return null;
  return n;
}

interface HttpHandlerOptions {
  protocol: StreamProtocolInterface;
  pathPrefix?: string; // default: "/"
  maxMessageSize?: number; // default: 1MB (1024 * 1024)
}

function withSecurityHeaders(response: Response): Response {
  const headers = new Headers(response.headers);
  if (!headers.has("x-content-type-options")) {
    headers.set("x-content-type-options", "nosniff");
  }
  if (!headers.has("cross-origin-resource-policy")) {
    headers.set("cross-origin-resource-policy", "cross-origin");
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function bytesToBase64(bytes: Uint8Array): string {
  const BufferRef = (globalThis as { Buffer?: { from: (...args: unknown[]) => { toString(enc: string): string } } }).Buffer;
  if (BufferRef) {
    return BufferRef.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("base64");
  }
  // Fallback: build a binary string in chunks (avoids stack overflow on large inputs).
  let binary = "";
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}

export class HttpHandler {
  private protocol: StreamProtocolInterface;
  private pathPrefix: string;
  private maxMessageSize: number;

  constructor(options: HttpHandlerOptions) {
    this.protocol = options.protocol;
    this.pathPrefix = options.pathPrefix ?? "/";
    this.maxMessageSize = options.maxMessageSize ?? 1024 * 1024;
  }

  async fetch(request: Request): Promise<Response> {
    return withSecurityHeaders(await this.dispatch(request));
  }

  private async dispatch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // Extract stream path - everything after the path prefix
    const prefix = this.pathPrefix.endsWith("/")
      ? this.pathPrefix
      : this.pathPrefix + "/";
    const regex = new RegExp(
      `^${prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`,
    );
    const streamPath = url.pathname.replace(regex, "");

    if (!streamPath || streamPath === url.pathname) {
      return new Response(`Stream path required: ${prefix}{path}`, {
        status: 400,
      });
    }

    const method = request.method;

    try {
      switch (method) {
        case "PUT":
          return await this.handleCreate(request, streamPath);
        case "POST":
          return await this.handleAppend(request, streamPath);
        case "GET":
          return await this.handleRead(request, url, streamPath);
        case "HEAD":
          return await this.handleMetadata(streamPath);
        case "DELETE":
          return await this.handleDelete(streamPath);
        default:
          return new Response("Method not allowed", { status: 405 });
      }
    } catch (error) {
      console.error("Error handling request:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }

  private async handleCreate(
    request: Request,
    streamId: string,
  ): Promise<Response> {
    const contentType =
      request.headers.get("content-type") ?? "application/octet-stream";
    const ttlHeader = request.headers.get("stream-ttl");
    const expiresAtHeader = request.headers.get("stream-expires-at");

    // Validate TTL format
    if (ttlHeader && !/^(0|[1-9]\d*)$/.test(ttlHeader)) {
      return new Response("Invalid Stream-TTL format", { status: 400 });
    }

    // Cannot specify both TTL and expires-at
    if (ttlHeader && expiresAtHeader) {
      return new Response(
        "Cannot specify both Stream-TTL and Stream-Expires-At",
        {
          status: 400,
        },
      );
    }

    // Validate Expires-At format (must be valid ISO 8601 timestamp)
    if (expiresAtHeader) {
      const parsed = new Date(expiresAtHeader);
      if (isNaN(parsed.getTime())) {
        return new Response("Invalid Stream-Expires-At format", {
          status: 400,
        });
      }
    }

    const ttlSeconds = ttlHeader ? parseInt(ttlHeader, 10) : undefined;

    let initialData: ArrayBuffer;
    try {
      initialData = await request.arrayBuffer();
    } catch (error) {
      console.error("Error reading request body:", error);
      return new Response("Payload too large", { status: 413 });
    }

    // Validate max message size at HTTP layer
    if (initialData.byteLength > this.maxMessageSize) {
      return new Response("Payload too large", { status: 413 });
    }

    // Check for empty arrays in JSON content-type before calling protocol
    let effectiveInitialData: Uint8Array | undefined =
      initialData.byteLength > 0 ? new Uint8Array(initialData) : undefined;

    if (
      effectiveInitialData &&
      contentType.toLowerCase().startsWith("application/json")
    ) {
      try {
        const text = new TextDecoder().decode(effectiveInitialData);
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed) && parsed.length === 0) {
          // Empty array in create - treat as no initial data
          effectiveInitialData = undefined;
        }
      } catch (error) {
        if (error instanceof SyntaxError) {
          return new Response("Invalid JSON", { status: 400 });
        }
        throw error;
      }
    }

    const result = await this.protocol.create(streamId, {
      contentType,
      ttlSeconds,
      expiresAt: expiresAtHeader ?? undefined,
      initialData: effectiveInitialData,
    });

    if (result.status === "conflict") {
      return new Response("Stream exists with different configuration", {
        status: 409,
      });
    }

    const status = result.status === "created" ? 201 : 200;
    return new Response(null, {
      status,
      headers: {
        "content-type": result.contentType,
        "stream-next-offset": result.nextOffset,
        ...(status === 201 ? { location: request.url } : {}),
      },
    });
  }

  private async handleAppend(
    request: Request,
    streamId: string,
  ): Promise<Response> {
    const contentType = request.headers.get("content-type");
    if (!contentType) {
      return new Response("Content-Type required", { status: 400 });
    }

    const seq = request.headers.get("stream-seq") ?? undefined;

    const producerHeaders = parseProducerHeaders(request);
    if (producerHeaders.kind === "invalid") {
      return new Response("Invalid producer headers", { status: 400 });
    }

    let data: ArrayBuffer;
    try {
      data = await request.arrayBuffer();
    } catch (error) {
      console.error("Error reading request body:", error);
      return new Response("Payload too large", { status: 413 });
    }

    if (data.byteLength === 0) {
      return new Response("Empty body not allowed", { status: 400 });
    }

    // Validate max message size at HTTP layer
    if (data.byteLength > this.maxMessageSize) {
      return new Response("Payload too large", { status: 413 });
    }

    // Check for empty arrays in JSON content-type before calling protocol
    if (contentType.toLowerCase().startsWith("application/json")) {
      try {
        const text = new TextDecoder().decode(data);
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed) && parsed.length === 0) {
          return new Response("Empty arrays not allowed", { status: 400 });
        }
      } catch (error) {
        if (error instanceof SyntaxError) {
          return new Response("Invalid JSON", { status: 400 });
        }
        throw error;
      }
    }

    const result = await this.protocol.append(streamId, {
      data: new Uint8Array(data),
      contentType,
      seq,
      producer:
        producerHeaders.kind === "ok" ? producerHeaders.producer : undefined,
    });

    switch (result.status) {
      case "not-found":
        return new Response("Stream not found", { status: 404 });

      case "conflict": {
        const message =
          result.conflictReason === "content-type"
            ? "Content-Type mismatch"
            : "Sequence conflict";
        return new Response(message, { status: 409 });
      }

      case "stale-epoch":
        return new Response("Stale producer epoch", {
          status: 403,
          headers: { "producer-epoch": String(result.currentEpoch) },
        });

      case "producer-gap":
        return new Response("Producer sequence gap", {
          status: 409,
          headers: {
            "producer-expected-seq": String(result.expectedSeq),
            "producer-received-seq": String(result.receivedSeq),
          },
        });

      case "invalid-epoch-seq":
        return new Response("New epoch must start at seq=0", { status: 400 });

      case "duplicate":
        return new Response(null, {
          status: 204,
          headers: {
            "stream-next-offset": result.nextOffset,
            "producer-epoch": String(result.producerEpoch),
            "producer-seq": String(result.producerSeq),
          },
        });

      case "appended": {
        const headers: Record<string, string> = {
          "stream-next-offset": result.nextOffset,
        };
        if (result.producerEpoch !== undefined) {
          headers["producer-epoch"] = String(result.producerEpoch);
        }
        if (result.producerSeq !== undefined) {
          headers["producer-seq"] = String(result.producerSeq);
        }
        const status = producerHeaders.kind === "ok" ? 200 : 204;
        return new Response(null, { status, headers });
      }
    }
  }

  private async handleRead(
    request: Request,
    url: URL,
    streamId: string,
  ): Promise<Response> {
    const offset = url.searchParams.get("offset") ?? undefined;
    const live = url.searchParams.get("live");
    const cursor = url.searchParams.get("cursor") ?? undefined;

    const isLive = live === "long-poll" || live === "sse";

    // Validate offset format: -1, now, or digits_digits.
    if (
      offset !== undefined &&
      offset !== "-1" &&
      offset !== "now" &&
      !/^\d+_\d+$/.test(offset)
    ) {
      return new Response("Invalid offset format", { status: 400 });
    }

    // Resolve offset=now to the current tail before delegating, so the
    // protocol/storage layers never see the literal sentinel.
    let effectiveOffset = offset;
    if (offset === "now") {
      const meta = await this.protocol.metadata(streamId);
      if (meta.status === "not-found") {
        return new Response("Stream not found", { status: 404 });
      }
      effectiveOffset = meta.nextOffset;

      // Catch-up mode (no live param): return an empty body at the tail.
      if (live !== "long-poll" && live !== "sse") {
        const contentType = meta.contentType!;
        const isJson = contentType.toLowerCase().startsWith("application/json");
        return new Response(isJson ? "[]" : "", {
          headers: {
            "content-type": contentType,
            "stream-next-offset": effectiveOffset!,
            "stream-up-to-date": "true",
            "cache-control": "no-store",
          },
        });
      }
    }

    // Handle live modes
    if (isLive) {
      if (!effectiveOffset) {
        return new Response("offset required for live modes", { status: 400 });
      }

      if (live === "sse") {
        return await this.handleSSE(streamId, effectiveOffset, cursor);
      }

      return await this.handleLongPoll(streamId, effectiveOffset, cursor);
    }

    // Regular catch-up read
    const result = await this.protocol.read(streamId, {
      offset: effectiveOffset,
    });

    if (result.status === "not-found") {
      return new Response("Stream not found", { status: 404 });
    }

    // Generate ETag for cache validation
    const startOffset = offset ?? "-1";
    const etag = `"${btoa(url.pathname)}:${startOffset}:${result.nextOffset}"`;

    // Check If-None-Match header for conditional request
    const ifNoneMatch = request.headers.get("if-none-match");
    if (ifNoneMatch === etag) {
      return new Response(null, {
        status: 304,
        headers: {
          etag,
          "cache-control": "public, max-age=60, stale-while-revalidate=300",
        },
      });
    }

    // Get metadata to determine content type
    const metadata = await this.protocol.metadata(streamId);
    const contentTypeLower = metadata?.contentType?.toLowerCase() ?? "";
    const isJson = contentTypeLower.startsWith("application/json");
    const isText = contentTypeLower.startsWith("text/");

    let body: BodyInit;
    if (isJson) {
      const items = result.messages.map((msg) =>
        new TextDecoder().decode(msg.data),
      );
      body = `[${items.join(",")}]`;
    } else if (isText) {
      body = result.messages
        .map((msg) => new TextDecoder().decode(msg.data))
        .join("");
    } else {
      const totalLength = result.messages.reduce(
        (acc, msg) => acc + msg.data.length,
        0,
      );
      const combined = new Uint8Array(totalLength);
      let pos = 0;
      for (const msg of result.messages) {
        combined.set(msg.data, pos);
        pos += msg.data.length;
      }
      body = combined.buffer as ArrayBuffer;
    }

    return new Response(body, {
      headers: {
        "content-type": metadata.contentType!,
        "stream-next-offset": result.nextOffset,
        ...(result.upToDate ? { "stream-up-to-date": "true" } : {}),
        etag,
        "cache-control": "public, max-age=60, stale-while-revalidate=300",
      },
    });
  }

  private async handleLongPoll(
    streamId: string,
    offset: string,
    cursor?: string,
  ): Promise<Response> {
    const result = await this.protocol.readLive(streamId, {
      offset,
      mode: "long-poll",
      cursor,
    });

    if (result.status === "not-found") {
      return new Response("Stream not found", { status: 404 });
    }

    if (result.status === "timeout" || result.messages.length === 0) {
      return new Response(null, {
        status: 204,
        headers: {
          "stream-next-offset": result.nextOffset,
          "stream-up-to-date": "true",
          "stream-cursor": result.cursor,
        },
      });
    }

    const metadata = await this.protocol.metadata(streamId);
    const contentTypeLower = metadata?.contentType?.toLowerCase() ?? "";
    const isJson = contentTypeLower.startsWith("application/json");
    const isText = contentTypeLower.startsWith("text/");

    let body: BodyInit;
    if (isJson && result.messages.length > 0) {
      const items = result.messages.map((msg) =>
        new TextDecoder().decode(msg.data),
      );
      body = `[${items.join(",")}]`;
    } else if (isText) {
      body = result.messages
        .map((msg) => new TextDecoder().decode(msg.data))
        .join("");
    } else {
      const totalLength = result.messages.reduce(
        (acc, msg) => acc + msg.data.length,
        0,
      );
      const combined = new Uint8Array(totalLength);
      let pos = 0;
      for (const msg of result.messages) {
        combined.set(msg.data, pos);
        pos += msg.data.length;
      }
      body = combined.buffer as ArrayBuffer;
    }

    return new Response(body, {
      headers: {
        "content-type": metadata.contentType!,
        "stream-next-offset": result.nextOffset,
        "stream-up-to-date": "true",
        "stream-cursor": result.cursor,
      },
    });
  }

  private async handleSSE(
    streamId: string,
    offset: string,
    cursor?: string,
  ): Promise<Response> {
    const metadata = await this.protocol.metadata(streamId);

    if (metadata.status === "not-found") {
      return new Response("Stream not found", { status: 404 });
    }

    const contentTypeLower = metadata.contentType!.toLowerCase();
    const isText = contentTypeLower.startsWith("text/");
    const isJson = contentTypeLower.startsWith("application/json");
    const useBase64 = !isText && !isJson;

    const encoder = new TextEncoder();
    const decoder = new TextDecoder();
    let currentOffset = offset;
    let currentCursor = cursor;
    const connectionStartTime = Date.now();
    const CONNECTION_TIMEOUT_MS = 60_000;

    const generateCursor = (previous?: string): string => {
      const CURSOR_EPOCH = new Date("2024-10-09T00:00:00.000Z").getTime();
      const CURSOR_INTERVAL_MS = 20_000;
      const now = Date.now();
      const currentInterval = Math.floor(
        (now - CURSOR_EPOCH) / CURSOR_INTERVAL_MS,
      );

      if (!previous) {
        return String(currentInterval);
      }

      const previousInterval = parseInt(previous, 10);
      if (previousInterval < currentInterval) {
        return String(currentInterval);
      }

      const jitterIntervals = Math.max(1, Math.floor(Math.random() * 180));
      return String(previousInterval + jitterIntervals);
    };

    const concatBytes = (messages: { data: Uint8Array }[]): Uint8Array => {
      const totalLength = messages.reduce(
        (acc, msg) => acc + msg.data.length,
        0,
      );
      const combined = new Uint8Array(totalLength);
      let pos = 0;
      for (const msg of messages) {
        combined.set(msg.data, pos);
        pos += msg.data.length;
      }
      return combined;
    };

    const writeDataEvent = (
      controller: ReadableStreamDefaultController<Uint8Array>,
      messages: { data: Uint8Array }[],
    ) => {
      controller.enqueue(encoder.encode("event: data\n"));
      if (useBase64) {
        const combined = concatBytes(messages);
        const base64 = bytesToBase64(combined);
        controller.enqueue(encoder.encode(`data:${base64}\n`));
      } else if (isJson) {
        const items = messages.map((msg) => decoder.decode(msg.data));
        controller.enqueue(encoder.encode("data:[\n"));
        for (let i = 0; i < items.length; i++) {
          const suffix = i < items.length - 1 ? "," : "";
          controller.enqueue(encoder.encode(`data:${items[i]}${suffix}\n`));
        }
        controller.enqueue(encoder.encode("data:]\n"));
      } else {
        // text/*: split on any line terminator (CR, LF, CRLF) so payload bytes
        // can never inject SSE event boundaries; emit one data: line per logical
        // line per the SSE spec.
        const text = messages.map((msg) => decoder.decode(msg.data)).join("");
        const lines = text.split(/\r\n|\r|\n/);
        for (const line of lines) {
          controller.enqueue(encoder.encode(`data:${line}\n`));
        }
      }
      controller.enqueue(encoder.encode("\n"));
    };

    const writeControlEvent = (
      controller: ReadableStreamDefaultController<Uint8Array>,
      data: Record<string, unknown>,
    ) => {
      controller.enqueue(encoder.encode("event: control\n"));
      controller.enqueue(encoder.encode(`data:${JSON.stringify(data)}\n\n`));
    };

    const protocol = this.protocol;

    const stream = new ReadableStream<Uint8Array>({
      start: async (controller) => {
        try {
          // For offset=now, skip historical data and start at the current tail.
          // Otherwise do a non-blocking read for catch-up data.
          let initialNextOffset: string;
          let initialUpToDate: boolean;
          if (currentOffset === "now") {
            initialNextOffset = metadata.nextOffset!;
            initialUpToDate = true;
          } else {
            const initialResult = await protocol.read(streamId, {
              offset: currentOffset === "-1" ? undefined : currentOffset,
            });

            if (initialResult.status === "not-found") {
              controller.close();
              return;
            }

            if (initialResult.messages.length > 0) {
              writeDataEvent(controller, initialResult.messages);
            }

            initialNextOffset = initialResult.nextOffset;
            initialUpToDate = initialResult.upToDate;
          }

          // Send initial control event
          currentCursor = generateCursor(currentCursor);
          const initialControlData: {
            streamNextOffset: string;
            streamCursor: string;
            upToDate?: boolean;
          } = {
            streamNextOffset: initialNextOffset,
            streamCursor: currentCursor,
          };
          if (initialUpToDate) {
            initialControlData.upToDate = true;
          }
          writeControlEvent(controller, initialControlData);

          currentOffset = initialNextOffset;

          // Live polling loop
          while (true) {
            if (Date.now() - connectionStartTime >= CONNECTION_TIMEOUT_MS) {
              controller.close();
              return;
            }

            const result = await protocol.readLive(streamId, {
              offset: currentOffset,
              mode: "sse",
              cursor: currentCursor,
            });

            if (result.status === "not-found") {
              controller.close();
              return;
            }

            if (result.messages.length > 0) {
              writeDataEvent(controller, result.messages);
            }

            const controlData: {
              streamNextOffset: string;
              streamCursor: string;
              upToDate?: boolean;
            } = {
              streamNextOffset: result.nextOffset,
              streamCursor: result.cursor,
            };
            if (result.upToDate) {
              controlData.upToDate = true;
            }
            writeControlEvent(controller, controlData);

            currentOffset = result.nextOffset;
            currentCursor = result.cursor;

            if (result.status === "timeout") {
              continue;
            }
          }
        } catch (error) {
          console.error("SSE stream error:", error);
          controller.close();
        }
      },
    });

    const headers: Record<string, string> = {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    };
    if (useBase64) {
      headers["stream-sse-data-encoding"] = "base64";
    }

    return new Response(stream, { headers });
  }

  private async handleMetadata(streamId: string): Promise<Response> {
    const result = await this.protocol.metadata(streamId);

    if (result.status === "not-found") {
      return new Response("Stream not found", { status: 404 });
    }

    return new Response(null, {
      headers: {
        "content-type": result.contentType!,
        "stream-next-offset": result.nextOffset!,
        ...(result.ttlSeconds
          ? { "stream-ttl": String(result.ttlSeconds) }
          : {}),
        ...(result.expiresAt ? { "stream-expires-at": result.expiresAt } : {}),
        "cache-control": "no-store",
      },
    });
  }

  private async handleDelete(streamId: string): Promise<Response> {
    const result = await this.protocol.delete(streamId);

    if (result.status === "not-found") {
      return new Response("Stream not found", { status: 404 });
    }

    return new Response(null, { status: 204 });
  }
}

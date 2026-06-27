import { describe, expect, test } from "vitest";

const STREAM_CLOSED_HEADER = "Stream-Closed";
const STREAM_OFFSET_HEADER = "Stream-Next-Offset";
const decoder = new TextDecoder();

interface TraceEntry {
  t: number;
  event: string;
  details?: unknown;
}

interface SseEvent {
  type: string;
  data: string;
}

function baseUrl(): string {
  return (process.env.SERVER_BASE_URL || "http://localhost:1337").replace(/\/$/, "");
}

function now(startedAt: number): number {
  return Date.now() - startedAt;
}

function pushTrace(trace: TraceEntry[], startedAt: number, event: string, details?: unknown): void {
  const entry = { t: now(startedAt), event, ...(details === undefined ? {} : { details }) };
  trace.push(entry);
  console.log(`[sse-final-append-diagnostic] ${JSON.stringify(entry)}`);
}

function parseSseEvents(input: string): SseEvent[] {
  const events: SseEvent[] = [];
  for (const block of input.split(/\n\n/)) {
    const lines = block.split(/\n/).filter(Boolean);
    if (lines.length === 0) continue;
    let type = "message";
    const data: string[] = [];
    for (const line of lines) {
      if (line.startsWith("event:")) type = line.slice("event:".length).trim();
      if (line.startsWith("data:")) data.push(line.slice("data:".length).trimStart());
    }
    if (data.length > 0) events.push({ type, data: data.join("\n") });
  }
  return events;
}

async function readUntil(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  options: {
    timeoutMs: number;
    predicate: (received: string) => boolean;
    trace: TraceEntry[];
    startedAt: number;
  },
): Promise<string> {
  let received = "";
  const deadline = Date.now() + options.timeoutMs;
  while (Date.now() < deadline) {
    const remainingMs = deadline - Date.now();
    const read = reader.read();
    const timeout = new Promise<"timeout">((resolve) =>
      setTimeout(() => resolve("timeout"), Math.min(remainingMs, 250)),
    );
    const result = await Promise.race([read, timeout]);
    if (result === "timeout") continue;
    if (result.done) {
      pushTrace(options.trace, options.startedAt, "sse-reader-done", {
        receivedLength: received.length,
      });
      break;
    }
    const chunk = decoder.decode(result.value);
    received += chunk;
    pushTrace(options.trace, options.startedAt, "sse-chunk", {
      bytes: result.value.byteLength,
      chunk,
      receivedLength: received.length,
    });
    if (options.predicate(received)) return received;
  }
  return received;
}

function formatTrace(trace: TraceEntry[], received: string): string {
  return `${trace.map((entry) => JSON.stringify(entry)).join("\n")}\n\nreceived:\n${received}`;
}

describe("Durable Object SSE final-append diagnostic", () => {
  test("handshaked tail SSE receives final append+close on the same connection", async () => {
    const startedAt = Date.now();
    const trace: TraceEntry[] = [];
    const streamPath = `/v1/stream/sse-final-append-diagnostic-${startedAt}-${Math.random().toString(36).slice(2)}`;

    pushTrace(trace, startedAt, "create-start", { streamPath });
    const createResponse = await fetch(`${baseUrl()}${streamPath}`, {
      method: "PUT",
      headers: { "Content-Type": "text/plain" },
      body: "initial",
    });
    const tailOffset = createResponse.headers.get(STREAM_OFFSET_HEADER);
    pushTrace(trace, startedAt, "create-response", {
      status: createResponse.status,
      tailOffset,
      closed: createResponse.headers.get(STREAM_CLOSED_HEADER),
    });
    expect(createResponse.status).toBe(201);
    expect(tailOffset).toBeTruthy();

    const abort = new AbortController();
    const sseUrl = `${baseUrl()}${streamPath}?offset=${tailOffset}&live=sse`;
    pushTrace(trace, startedAt, "sse-fetch-start", { sseUrl });
    const sseResponse = await fetch(sseUrl, { signal: abort.signal });
    pushTrace(trace, startedAt, "sse-response", {
      status: sseResponse.status,
      contentType: sseResponse.headers.get("content-type"),
    });
    expect(sseResponse.status).toBe(200);
    expect(sseResponse.body).toBeTruthy();

    const reader = sseResponse.body!.getReader();
    let received = await readUntil(reader, {
      timeoutMs: 2_000,
      trace,
      startedAt,
      predicate: (body) => body.includes("upToDate"),
    });
    pushTrace(trace, startedAt, "initial-handshake-complete", { receivedLength: received.length });
    expect(received, formatTrace(trace, received)).toContain("upToDate");

    pushTrace(trace, startedAt, "append-close-start");
    const appendResponse = await fetch(`${baseUrl()}${streamPath}`, {
      method: "POST",
      headers: {
        "Content-Type": "text/plain",
        [STREAM_CLOSED_HEADER]: "true",
      },
      body: "sse-data",
    });
    pushTrace(trace, startedAt, "append-close-response", {
      status: appendResponse.status,
      nextOffset: appendResponse.headers.get(STREAM_OFFSET_HEADER),
      closed: appendResponse.headers.get(STREAM_CLOSED_HEADER),
    });
    expect(appendResponse.status).toBe(204);

    const afterAppend = await readUntil(reader, {
      timeoutMs: 5_000,
      trace,
      startedAt,
      predicate: (body) => body.includes("streamClosed"),
    });
    received += afterAppend;
    const containsSseData = received.includes("sse-data");
    const containsStreamClosed = received.includes("streamClosed");
    pushTrace(trace, startedAt, "post-append-read-complete", {
      totalReceivedLength: received.length,
      containsSseData,
      containsStreamClosed,
    });

    if (!containsSseData || !containsStreamClosed) {
      pushTrace(trace, startedAt, "catchup-read-after-sse-miss-start");
      const catchupResponse = await fetch(`${baseUrl()}${streamPath}?offset=${tailOffset}`);
      const catchupBody = await catchupResponse.text();
      pushTrace(trace, startedAt, "catchup-read-after-sse-miss-response", {
        status: catchupResponse.status,
        nextOffset: catchupResponse.headers.get(STREAM_OFFSET_HEADER),
        closed: catchupResponse.headers.get(STREAM_CLOSED_HEADER),
        body: catchupBody,
      });
    }
    abort.abort();

    const events = parseSseEvents(received);
    const data = events
      .filter((event) => event.type === "data")
      .map((event) => event.data)
      .join("");
    expect(data, formatTrace(trace, received)).toContain("sse-data");
    expect(received, formatTrace(trace, received)).toContain("streamClosed");
  }, 10_000);
});

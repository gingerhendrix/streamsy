/**
 * On-disk (de)serialization for the JSONL filesystem adapter.
 *
 * Two concerns live here:
 *
 *  1. **Stream id → directory name.** A `streamId` is an arbitrary string and may
 *     contain `/`, `..`, or other path-unsafe characters. {@link encodeStreamId}
 *     percent-encodes every character outside `[A-Za-z0-9_-]`, so the result can
 *     never be `.`, `..`, or contain a separator — path traversal is impossible.
 *     The encoding is reversible (`decodeURIComponent`) and the raw id is also
 *     stored inside `record.json`, so directories stay debuggable.
 *
 *  2. **Message envelope.** Each line of `messages.jsonl` is one JSON envelope.
 *     The encoding is chosen from the stream's `contentType`:
 *       - JSON streams (`application/json`) store the parsed value inline under
 *         `json` — human-readable, semantically faithful (byte-lossy: whitespace
 *         / key order may differ, but `@streamsy/json` re-parses so semantics are
 *         what matter). A line whose bytes are not valid JSON falls back to the
 *         base64 form so a mislabeled byte stream still round-trips.
 *       - Non-JSON streams store the exact bytes under `b64` for a lossless
 *         round-trip.
 *     Envelopes are discriminated by the presence of `b64` vs `json`.
 */
import { Buffer } from "node:buffer";
import path from "node:path";
import type { Offset, StoredMessage, StreamRecord } from "@streamsy/core";

export const JSON_CONTENT_TYPE = "application/json";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/** Match `@streamsy/json`: the media type (ignoring parameters), case-insensitive. */
export function isJsonContentType(contentType: string): boolean {
  return contentType.toLowerCase().split(";", 1)[0]?.trim() === JSON_CONTENT_TYPE;
}

const UNSAFE_ID_CHAR = /[^A-Za-z0-9_-]/g;

/** Encode a stream id to a path-safe directory name (reversible, traversal-proof). */
export function encodeStreamId(id: string): string {
  if (id.length === 0) throw new Error("storage-fs: stream id must not be empty");
  return id.replace(UNSAFE_ID_CHAR, (ch) => {
    let out = "";
    for (const byte of textEncoder.encode(ch)) {
      out += "%" + byte.toString(16).toUpperCase().padStart(2, "0");
    }
    return out;
  });
}

/** Resolve a stream's directory under `root`, rejecting any escape outside it. */
export function streamDir(root: string, id: string): string {
  const dir = path.join(root, encodeStreamId(id));
  const rel = path.relative(root, dir);
  if (rel.length === 0 || rel.startsWith("..") || path.isAbsolute(rel)) {
    throw new Error(`storage-fs: refusing path-escaping stream id ${JSON.stringify(id)}`);
  }
  return dir;
}

export function serializeRecord(record: StreamRecord): string {
  return JSON.stringify(record);
}

export function parseRecord(text: string): StreamRecord {
  return JSON.parse(text) as StreamRecord;
}

interface MessageEnvelope {
  offset: Offset;
  timestamp: number;
  json?: unknown;
  b64?: string;
}

/** Encode one stored message to a single JSONL line (no trailing newline). */
export function encodeEnvelope(message: StoredMessage, contentType: string): string {
  if (isJsonContentType(contentType)) {
    try {
      const json = JSON.parse(textDecoder.decode(message.data)) as unknown;
      const envelope: MessageEnvelope = {
        offset: message.offset,
        timestamp: message.timestamp,
        json,
      };
      return JSON.stringify(envelope);
    } catch {
      // Bytes are not valid JSON on a nominally-JSON stream: fall back to base64
      // so the line still round-trips losslessly instead of throwing.
    }
  }
  const envelope: MessageEnvelope = {
    offset: message.offset,
    timestamp: message.timestamp,
    b64: Buffer.from(message.data).toString("base64"),
  };
  return JSON.stringify(envelope);
}

/** Decode one JSONL line back to a stored message, or `null` for a malformed line. */
export function decodeEnvelope(line: string): StoredMessage | null {
  const trimmed = line.trim();
  if (trimmed.length === 0) return null;
  let envelope: MessageEnvelope;
  try {
    envelope = JSON.parse(trimmed) as MessageEnvelope;
  } catch {
    return null;
  }
  if (typeof envelope.offset !== "string" || typeof envelope.timestamp !== "number") return null;

  let data: Uint8Array;
  if (typeof envelope.b64 === "string") {
    data = new Uint8Array(Buffer.from(envelope.b64, "base64"));
  } else if ("json" in envelope) {
    data = textEncoder.encode(JSON.stringify(envelope.json));
  } else {
    return null;
  }
  return { offset: envelope.offset, timestamp: envelope.timestamp, data };
}

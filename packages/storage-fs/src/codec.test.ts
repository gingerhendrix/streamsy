/**
 * Envelope encoding: JSON-inline for `application/json`, base64 for everything
 * else, with a base64 fallback when a nominally-JSON line is not valid JSON.
 */
import { describe, expect, it } from "bun:test";
import {
  decodeEnvelope,
  encodeEnvelope,
  encodeStreamId,
  isJsonContentType,
  streamDir,
} from "./codec.ts";
import type { StoredMessage } from "@streamsy/core";

const enc = (s: string): Uint8Array => new TextEncoder().encode(s);
const dec = (u: Uint8Array): string => new TextDecoder().decode(u);

function message(offset: string, data: Uint8Array): StoredMessage {
  return { offset, timestamp: 1730000000000, data };
}

describe("isJsonContentType", () => {
  it("matches the media type ignoring case and parameters", () => {
    expect(isJsonContentType("application/json")).toBe(true);
    expect(isJsonContentType("Application/JSON; charset=utf-8")).toBe(true);
    expect(isJsonContentType("text/plain")).toBe(false);
    expect(isJsonContentType("application/octet-stream")).toBe(false);
  });
});

describe("encodeEnvelope / decodeEnvelope", () => {
  it("stores JSON inline (no base64) for a JSON stream and round-trips semantically", () => {
    const value = { hello: "world", n: 42, nested: [1, 2, { a: true }] };
    const original = message("0000000000000001_0000000000000000", enc(JSON.stringify(value)));

    const line = encodeEnvelope(original, "application/json");
    expect(line.includes('"json"')).toBe(true);
    expect(line.includes('"b64"')).toBe(false);
    // The parsed value is stored inline (human-readable).
    expect(JSON.parse(line).json).toEqual(value);

    const decoded = decodeEnvelope(line)!;
    expect(decoded.offset).toBe(original.offset);
    expect(decoded.timestamp).toBe(original.timestamp);
    expect(JSON.parse(dec(decoded.data))).toEqual(value);
  });

  it("round-trips inline JSON null and primitives", () => {
    for (const raw of ["null", "true", "123", '"a string"']) {
      const line = encodeEnvelope(
        message("0000000000000001_0000000000000000", enc(raw)),
        "application/json",
      );
      expect(line.includes('"b64"')).toBe(false);
      const decoded = decodeEnvelope(line)!;
      expect(JSON.parse(dec(decoded.data))).toEqual(JSON.parse(raw));
    }
  });

  it("uses base64 for a non-JSON stream and round-trips bytes exactly", () => {
    const bytes = new Uint8Array([0, 1, 2, 250, 251, 255, 10, 13]);
    const line = encodeEnvelope(message("0000000000000002_0000000000000000", bytes), "text/plain");
    expect(line.includes('"b64"')).toBe(true);
    expect(line.includes('"json"')).toBe(false);

    const decoded = decodeEnvelope(line)!;
    expect([...decoded.data]).toEqual([...bytes]);
  });

  it("falls back to base64 for invalid JSON bytes on a JSON stream", () => {
    const bytes = new Uint8Array([0xff, 0x00, 0x01]); // not valid UTF-8/JSON
    const line = encodeEnvelope(
      message("0000000000000003_0000000000000000", bytes),
      "application/json",
    );
    expect(line.includes('"b64"')).toBe(true);

    const decoded = decodeEnvelope(line)!;
    expect([...decoded.data]).toEqual([...bytes]);
  });

  it("ignores blank and malformed lines on decode", () => {
    expect(decodeEnvelope("")).toBeNull();
    expect(decodeEnvelope("   ")).toBeNull();
    expect(decodeEnvelope("{not json")).toBeNull();
    expect(decodeEnvelope('{"offset":1}')).toBeNull();
  });
});

describe("encodeStreamId / streamDir", () => {
  it("encodes path-unsafe characters reversibly", () => {
    expect(encodeStreamId("plain")).toBe("plain");
    expect(encodeStreamId("a/b")).toBe("a%2Fb");
    expect(encodeStreamId("..")).toBe("%2E%2E");
    expect(decodeURIComponent(encodeStreamId("a/b/../c"))).toBe("a/b/../c");
  });

  it("rejects an empty stream id", () => {
    expect(() => encodeStreamId("")).toThrow();
  });

  it("never escapes the root directory", () => {
    const root = "/tmp/streamsy-root";
    for (const id of ["../escape", "a/../../b", "..", "/etc/passwd"]) {
      const dir = streamDir(root, id);
      expect(dir.startsWith(root + "/")).toBe(true);
      expect(dir.includes("/../")).toBe(false);
    }
  });
});

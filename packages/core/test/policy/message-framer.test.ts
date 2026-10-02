/**
 * Unit coverage for the message-framer helper extracted from
 * StreamProtocol.processData. Pins non-JSON passthrough, JSON object
 * canonicalization, JSON array splitting, and empty-array behavior.
 */

import { describe, it, expect } from "bun:test";
import { frameMessages } from "../../src/policy/message-framer.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

describe("frameMessages — non-JSON passthrough", () => {
  it("returns the original bytes as a single message for octet-stream and text/plain", () => {
    for (const [data, contentType] of [
      [new Uint8Array([0x00, 0x01, 0x02, 0xff]), "application/octet-stream"],
      [enc.encode("hello world"), "text/plain"],
    ] as const) {
      const result = frameMessages(data, contentType);
      expect(result).toHaveLength(1);
      expect(result[0]).toBe(data);
    }
  });

  it("does not parse content types that merely contain 'json' but do not start with application/json", () => {
    const data = enc.encode("[1,2,3]");
    const result = frameMessages(data, "text/json");
    expect(result).toHaveLength(1);
    expect(result[0]).toBe(data);
  });
});

describe("frameMessages — JSON object input", () => {
  it("re-encodes a JSON object as a single canonical message", () => {
    const data = enc.encode('{ "a": 1, "b": 2 }');
    const result = frameMessages(data, "application/json");
    expect(result).toHaveLength(1);
    expect(dec.decode(result[0])).toBe('{"a":1,"b":2}');
  });

  it("re-encodes a JSON string or number as a single message", () => {
    for (const text of ['"hello"', "42"]) {
      const result = frameMessages(enc.encode(text), "application/json");
      expect(result).toHaveLength(1);
      expect(dec.decode(result[0])).toBe(text);
    }
  });
});

describe("frameMessages — JSON array input", () => {
  it("produces one encoded message per array item", () => {
    const data = enc.encode('[{"x":1},{"y":2},"z"]');
    const result = frameMessages(data, "application/json");
    expect(result).toHaveLength(3);
    expect(dec.decode(result[0])).toBe('{"x":1}');
    expect(dec.decode(result[1])).toBe('{"y":2}');
    expect(dec.decode(result[2])).toBe('"z"');
  });

  it("returns no messages for an empty JSON array", () => {
    const data = enc.encode("[]");
    const result = frameMessages(data, "application/json");
    expect(result).toEqual([]);
  });
});

describe("frameMessages — content-type matching semantics", () => {
  it("matches case-insensitively on the application/json prefix", () => {
    const data = enc.encode('{"a":1}');
    expect(dec.decode(frameMessages(data, "Application/JSON")[0])).toBe('{"a":1}');
    expect(dec.decode(frameMessages(data, "APPLICATION/JSON")[0])).toBe('{"a":1}');
  });

  it("treats application/json with parameters as JSON via prefix match", () => {
    const data = enc.encode("[1,2]");
    const result = frameMessages(data, "application/json; charset=utf-8");
    expect(result).toHaveLength(2);
    expect(dec.decode(result[0])).toBe("1");
    expect(dec.decode(result[1])).toBe("2");
  });
});

describe("frameMessages — invalid JSON", () => {
  it("propagates JSON.parse errors for invalid bodies under application/json", () => {
    const data = enc.encode("{not json");
    expect(() => frameMessages(data, "application/json")).toThrow();
  });
});

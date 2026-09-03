import { describe, expect, it } from "vitest";
import {
  compareStreamPositions,
  coverage,
  sourceAck,
  sourceWatermark,
  streamPosition,
} from "./causal.ts";
import {
  decodeStreamIdentity,
  encodeStreamIdentity,
  streamIdentity,
  streamIdentityEquals,
} from "./identity.ts";

describe("stream identity", () => {
  it("round-trips a canonical, versioned durable encoding", () => {
    const identity = streamIdentity("orders/Europe % café");
    const encoded = encodeStreamIdentity(identity);

    expect(encoded).toBe("streamsy.identity.v1:orders%2FEurope%20%25%20caf%C3%A9");
    expect(decodeStreamIdentity(encoded)).toEqual(identity);
  });

  it("makes the encoding version part of the durable key", () => {
    const encoded = encodeStreamIdentity(streamIdentity("orders"));
    expect(encoded).toMatch(/^streamsy\.identity\.v1:/);
    expect(() => decodeStreamIdentity(encoded.replace(".v1:", ".v2:"))).toThrow(
      "Unsupported stream identity encoding version",
    );
  });

  it("normalizes names and compares only within the mesh identity domain", () => {
    const composed = streamIdentity("caf\u00e9");
    const decomposed = streamIdentity("cafe\u0301");
    expect(streamIdentityEquals(composed, decomposed)).toBe(true);
    expect(streamIdentityEquals(composed, streamIdentity("other"))).toBe(false);
  });

  it("rejects invalid and non-canonical encodings", () => {
    for (const name of ["", "   ", "bad\u0000name", "x".repeat(257)]) {
      expect(() => streamIdentity(name)).toThrow(TypeError);
    }
    expect(() => decodeStreamIdentity("streamsy.identity.v1:orders%2feu")).toThrow(
      "Non-canonical stream identity encoding",
    );
  });
});

describe("causal coverage", () => {
  const identity = streamIdentity("orders");

  it("orders real positions lexicographically", () => {
    expect(compareStreamPositions(streamPosition("01"), streamPosition("02"))).toBe(-1);
    expect(compareStreamPositions(streamPosition("B"), streamPosition("A"))).toBe(1);
    expect(compareStreamPositions(streamPosition("same"), streamPosition("same"))).toBe(0);
  });

  it("rejects protocol read sentinels and unsafe tokens", () => {
    for (const invalid of ["", "-1", "now", "bad/value", "x".repeat(256)]) {
      expect(() => streamPosition(invalid)).toThrow();
      expect(() => compareStreamPositions(invalid, "01")).toThrow();
      expect(() => compareStreamPositions("01", invalid)).toThrow();
      expect(() => sourceAck(identity, invalid)).toThrow();
      expect(() => sourceWatermark(identity, invalid)).toThrow();
    }
  });

  it("is incomparable across identities even when positions match", () => {
    expect(
      coverage(
        sourceWatermark(streamIdentity("target-a"), "02"),
        sourceAck(streamIdentity("target-b"), "02"),
      ),
    ).toEqual({ status: "incomparable" });
  });

  it("is reflexive at every sampled position", () => {
    for (const position of ["0", "01", "01HZX9Q9M3Z7ZJ8W4G5A6B7C8D", "zz"] as const) {
      expect(coverage(sourceWatermark(identity, position), sourceAck(identity, position))).toEqual({
        status: "proven",
      });
    }
  });

  it("is monotonic as a watermark advances", () => {
    const ack = sourceAck(identity, "03");
    const statuses = ["01", "02", "03", "04", "05"].map(
      (position) => coverage(sourceWatermark(identity, position), ack).status,
    );
    expect(statuses).toEqual(["not-yet", "not-yet", "proven", "proven", "proven"]);
  });
});

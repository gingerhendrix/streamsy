import { expect, test } from "bun:test";
import { Offset } from "@streamsy/core";
import { decodeFrames, encodeFrames } from "./fork-frames.ts";

const message = (offset: string, timestamp: number, data: Uint8Array) => ({
  offset: Offset.make(offset),
  timestamp,
  data,
});

test("frames round-trip empty data, large data, and many small messages", () => {
  const messages = [
    message("0000000000000000_0000000000000001", 1.5, new Uint8Array()),
    message("0000000000000001_0000000000000000", 2.5, new Uint8Array(1_048_576)),
    ...Array.from({ length: 100 }, (_, index) =>
      message(
        `0000000000000002_${String(index).padStart(16, "0")}`,
        index,
        new Uint8Array([index]),
      ),
    ),
  ];

  expect(decodeFrames(encodeFrames(messages))).toEqual(messages);
});

test("empty frame bodies decode to no messages", () => {
  expect(decodeFrames(new Uint8Array())).toEqual([]);
});

test("decoding rejects a truncated final frame", () => {
  const encoded = encodeFrames([
    message("0000000000000000_0000000000000000", 1, new Uint8Array([1, 2])),
  ]);
  expect(() => decodeFrames(encoded.subarray(0, encoded.byteLength - 1))).toThrow(
    "Truncated frame",
  );
});

test("decoding rejects malformed offsets", () => {
  const encoded = encodeFrames([message("0000000000000000_0000000000000000", 1, new Uint8Array())]);
  encoded[0] = 120;
  expect(() => decodeFrames(encoded)).toThrow("Invalid frame offset");
});

test("decoding rejects non-finite timestamps", () => {
  const encoded = encodeFrames([message("0000000000000000_0000000000000000", 1, new Uint8Array())]);
  new DataView(encoded.buffer).setFloat64(33, Number.NaN, false);
  expect(() => decodeFrames(encoded)).toThrow("Invalid frame timestamp");
});

test("decoding rejects frame data that overruns the buffer", () => {
  const encoded = encodeFrames([message("0000000000000000_0000000000000000", 1, new Uint8Array())]);
  new DataView(encoded.buffer).setUint32(41, 1, false);
  expect(() => decodeFrames(encoded)).toThrow("Truncated frame data");
});

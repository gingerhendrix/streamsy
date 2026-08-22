import { describe, expect, it } from "vitest";
import { nodeBufferBase64, toArrayBuffer } from "./bytes.ts";
import { MessageBodyCodec } from "./message-body-codec.ts";

describe("toArrayBuffer", () => {
  it("returns the backing buffer itself when the view spans all of it", () => {
    const view = new Uint8Array([1, 2, 3]);
    expect(toArrayBuffer(view)).toBe(view.buffer);
  });

  it("copies exactly the view's bytes when the view covers part of its buffer", () => {
    const backing = new Uint8Array([9, 1, 2, 3, 9]);
    const view = backing.subarray(1, 4);

    const result = toArrayBuffer(view);

    expect(result).toBeInstanceOf(ArrayBuffer);
    expect(result.byteLength).toBe(3);
    expect(Array.from(new Uint8Array(result))).toEqual([1, 2, 3]);
    expect(result).not.toBe(backing.buffer);
  });

  it("does not alias the source when it copies", () => {
    const backing = new Uint8Array([9, 1, 2, 3, 9]);
    const result = toArrayBuffer(backing.subarray(1, 4));

    backing.fill(0);

    expect(Array.from(new Uint8Array(result))).toEqual([1, 2, 3]);
  });
});

describe("nodeBufferBase64", () => {
  it("encodes only the bytes the view covers", () => {
    const backing = new Uint8Array([255, 1, 2, 3, 255]);
    const encoded = nodeBufferBase64(backing.subarray(1, 4));

    // The fallback path is the reference encoding for the same three bytes.
    expect(encoded).toBe(btoa(String.fromCharCode(1, 2, 3)));
  });

  it("returns undefined when the host has no Buffer global", () => {
    const original = Reflect.get(globalThis, "Buffer");
    Reflect.deleteProperty(globalThis, "Buffer");
    try {
      expect(nodeBufferBase64(new Uint8Array([1, 2, 3]))).toBeUndefined();
    } finally {
      Reflect.set(globalThis, "Buffer", original);
    }
  });

  it("returns undefined when a foreign Buffer global cannot produce a string", () => {
    const original = Reflect.get(globalThis, "Buffer");
    Reflect.set(
      globalThis,
      "Buffer",
      Object.assign(function foreign() {}, { from: () => ({ toString: () => 42 }) }),
    );
    try {
      expect(nodeBufferBase64(new Uint8Array([1, 2, 3]))).toBeUndefined();
    } finally {
      Reflect.set(globalThis, "Buffer", original);
    }
  });
});

describe("MessageBodyCodec base64 fallback", () => {
  it("encodes a partial view identically with and without the Buffer global", () => {
    const codec = new MessageBodyCodec();
    const backing = new Uint8Array([255, 1, 2, 3, 255]);
    const view = backing.subarray(1, 4);

    const withBuffer = codec.bytesToBase64(view);

    const original = Reflect.get(globalThis, "Buffer");
    Reflect.deleteProperty(globalThis, "Buffer");
    let withoutBuffer: string;
    try {
      withoutBuffer = codec.bytesToBase64(view);
    } finally {
      Reflect.set(globalThis, "Buffer", original);
    }

    expect(withBuffer).toBe(withoutBuffer);
    expect(withBuffer).toBe(btoa(String.fromCharCode(1, 2, 3)));
  });
});

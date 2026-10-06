import { describe, expect, it } from "bun:test";
import { toArrayBuffer } from "../../src/http/bytes.ts";
import * as MessageBody from "../../src/http/message-body-codec.ts";

describe("toArrayBuffer", () => {
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

function restoreBuffer(descriptor: PropertyDescriptor | undefined): void {
  if (descriptor === undefined) Reflect.deleteProperty(globalThis, "Buffer");
  else Object.defineProperty(globalThis, "Buffer", descriptor);
}

/** Installs a stand-in `Buffer` global for one call, then restores the original. */
function withBufferGlobal<Result, Replacement>(
  replacement: Replacement,
  run: (installed: Replacement) => Result,
): Result {
  const original = Object.getOwnPropertyDescriptor(globalThis, "Buffer");
  Object.defineProperty(globalThis, "Buffer", {
    configurable: true,
    writable: true,
    value: replacement,
  });

  try {
    return run(replacement);
  } finally {
    restoreBuffer(original);
  }
}

/** A `Buffer`-shaped global whose `from` returns whatever the test supplies. */
function foreignBuffer<Result>(from: () => Result) {
  return Object.assign(function foreign() {}, { from });
}

describe("MessageBodyCodec base64 fallback", () => {
  it("encodes a partial view identically with and without the Buffer global", () => {
    const codec = MessageBody;
    const backing = new Uint8Array([255, 1, 2, 3, 255]);
    const view = backing.subarray(1, 4);

    const withBuffer = codec.bytesToBase64(view);

    const original = Object.getOwnPropertyDescriptor(globalThis, "Buffer");
    Reflect.deleteProperty(globalThis, "Buffer");
    let withoutBuffer: string;

    try {
      withoutBuffer = codec.bytesToBase64(view);
    } finally {
      restoreBuffer(original);
    }

    expect(withBuffer).toBe(withoutBuffer);
    expect(withBuffer).toBe(btoa(String.fromCharCode(1, 2, 3)));
  });

  it("falls back to btoa when a foreign Buffer global returns an unusable value", () => {
    const codec = MessageBody;

    const encoded = withBufferGlobal(
      foreignBuffer(() => null),
      () => codec.bytesToBase64(new Uint8Array([1, 2, 3])),
    );

    expect(encoded).toBe(btoa(String.fromCharCode(1, 2, 3)));
  });
});

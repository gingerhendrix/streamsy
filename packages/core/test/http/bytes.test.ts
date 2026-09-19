import { describe, expect, it } from "bun:test";
import { nodeBufferBase64, toArrayBuffer } from "../../src/http/bytes.ts";
import * as MessageBody from "../../src/http/message-body-codec.ts";

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

type HostValue = null | undefined | boolean | number | bigint | string | symbol | object;

interface BufferReceiver {}

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

describe("nodeBufferBase64", () => {
  it("encodes only the bytes the view covers", () => {
    const backing = new Uint8Array([255, 1, 2, 3, 255]);
    const encoded = nodeBufferBase64(backing.subarray(1, 4));

    // The fallback path is the reference encoding for the same three bytes.
    expect(encoded).toBe(btoa(String.fromCharCode(1, 2, 3)));
  });

  it("returns undefined when the host has no Buffer global", () => {
    const original = Object.getOwnPropertyDescriptor(globalThis, "Buffer");
    Reflect.deleteProperty(globalThis, "Buffer");
    try {
      expect(nodeBufferBase64(new Uint8Array([1, 2, 3]))).toBeUndefined();
    } finally {
      restoreBuffer(original);
    }
  });

  // Each case is a value a foreign `Buffer` global can return that the encoder
  // must reject by returning `undefined` rather than by throwing. `toBeUndefined`
  // fails on a thrown error, so these pin both halves of that contract.
  const unusable: [string, () => HostValue][] = [
    ["null", () => null],
    ["undefined", () => undefined],
    ["a primitive", () => 7],
    ["an object with no toString", () => Object.create(null)],
    ["an object with a non-callable toString", () => ({ toString: "not-callable" })],
    ["an object whose toString returns a non-string", () => ({ toString: () => 42 })],
  ];

  for (const [description, from] of unusable) {
    it(`returns undefined when a foreign Buffer.from returns ${description}`, () => {
      expect(
        withBufferGlobal(foreignBuffer(from), () => nodeBufferBase64(new Uint8Array([1, 2, 3]))),
      ).toBeUndefined();
    });
  }

  it("calls from and toString with their own receivers", () => {
    const receivers: unknown[] = [];
    const produced = {
      toString(this: BufferReceiver) {
        receivers.push(this);
        return "AQID";
      },
    };
    const global = foreignBuffer(function from(this: BufferReceiver) {
      receivers.push(this);
      return produced;
    });

    const encoded = withBufferGlobal(global, () => nodeBufferBase64(new Uint8Array([1, 2, 3])));

    expect(encoded).toBe("AQID");
    expect(receivers).toEqual([global, produced]);
  });
});

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

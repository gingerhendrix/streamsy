/**
 * Cross-runtime byte normalization for HTTP bodies.
 *
 * `Uint8Array.prototype.buffer` is typed `ArrayBufferLike`, which also admits a
 * `SharedArrayBuffer`. `BodyInit` accepts only a real `ArrayBuffer`, and a view
 * may cover just part of its backing buffer, so neither the buffer identity nor
 * its element type can be assumed. These helpers establish both with runtime
 * checks instead of an assertion.
 */

/**
 * Returns an `ArrayBuffer` holding exactly the bytes of `view`.
 *
 * When the view already owns its whole backing `ArrayBuffer` the buffer is
 * returned as-is (no copy), which is the common case for freshly concatenated
 * bodies. Otherwise — a partial view, or a `SharedArrayBuffer` backing — the
 * bytes are copied into a fresh, exactly sized `ArrayBuffer`.
 */
export function toArrayBuffer(view: Uint8Array): ArrayBuffer {
  const { buffer, byteOffset, byteLength } = view;
  if (buffer instanceof ArrayBuffer && byteOffset === 0 && byteLength === buffer.byteLength) {
    return buffer;
  }
  const copy = new ArrayBuffer(byteLength);
  new Uint8Array(copy).set(view);
  return copy;
}

/**
 * Base64-encodes `bytes` through a Node-style `Buffer` when the host provides
 * one, or returns `undefined` so the caller can fall back to `btoa`.
 *
 * `Buffer` is an optional host global: absent in browsers and workers, present
 * in Node and Bun. Every step is checked at runtime before it is used — the
 * global exists and is callable, its `from` is callable, the value `from`
 * returns can carry a method, that method is callable, and the encoded result
 * is a primitive string. A `Buffer` global that fails any of these returns `undefined`
 * rather than throwing, so the caller always reaches its `btoa` fallback.
 */
type HostValue = null | undefined | boolean | number | bigint | string | symbol | object;

interface NodeBufferGlobal {
  from(buffer: ArrayBufferLike, byteOffset: number, byteLength: number): HostValue;
}

interface Base64BufferView {
  toString(encoding?: "base64"): HostValue;
}

function isReferenceValue<Value>(candidate: Value): candidate is Value & object {
  return candidate !== null && Object(candidate) === candidate;
}

function isNodeBufferGlobal<Value>(candidate: Value): candidate is Value & NodeBufferGlobal {
  return (
    isReferenceValue(candidate) &&
    candidate instanceof Function &&
    "from" in candidate &&
    candidate.from instanceof Function
  );
}

function isBase64BufferView<Value>(candidate: Value): candidate is Value & Base64BufferView {
  return (
    isReferenceValue(candidate) && "toString" in candidate && candidate.toString instanceof Function
  );
}

function primitiveString<Value>(candidate: Value): string | undefined {
  if (
    candidate === null ||
    candidate === undefined ||
    isReferenceValue(candidate) ||
    Object.getPrototypeOf(Object(candidate)) !== String.prototype
  ) {
    return undefined;
  }
  return String(candidate);
}

export function nodeBufferBase64(bytes: Uint8Array): string | undefined {
  if (!("Buffer" in globalThis)) return undefined;
  const bufferGlobal: HostValue = globalThis.Buffer;
  if (!isReferenceValue(bufferGlobal) || !isNodeBufferGlobal(bufferGlobal)) return undefined;

  // `Buffer` stays the receiver: `Buffer.from` is a static method and a host
  // implementation may rely on `this`.
  const view = bufferGlobal.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (!isBase64BufferView(view)) return undefined;

  // The produced value stays the receiver, so `toString` sees its own bytes.
  return primitiveString(view.toString("base64"));
}

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
 * is a string. A `Buffer` global that fails any of these returns `undefined`
 * rather than throwing, so the caller always reaches its `btoa` fallback.
 */
export function nodeBufferBase64(bytes: Uint8Array): string | undefined {
  if (!("Buffer" in globalThis)) return undefined;
  const bufferGlobal: unknown = globalThis.Buffer;
  if (typeof bufferGlobal !== "function" || !("from" in bufferGlobal)) return undefined;
  const from: unknown = bufferGlobal.from;
  if (typeof from !== "function") return undefined;

  // `Buffer` stays the receiver: `Buffer.from` is a static method and a host
  // implementation may rely on `this`.
  const view: unknown = Reflect.apply(from, bufferGlobal, [
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength,
  ]);
  if (view === null || (typeof view !== "object" && typeof view !== "function")) return undefined;

  // `Reflect.get` rather than `view.toString`: reading the method off the value
  // would be an unbound method reference, and a missing key yields `undefined`
  // here instead of needing a separate `in` check.
  const encode: unknown = Reflect.get(view, "toString");
  if (typeof encode !== "function") return undefined;

  // The produced value stays the receiver, so `toString` sees its own bytes.
  const encoded: unknown = Reflect.apply(encode, view, ["base64"]);
  return typeof encoded === "string" ? encoded : undefined;
}

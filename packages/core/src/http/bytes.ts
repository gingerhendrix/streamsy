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
 * in Node and Bun. Every step is checked at runtime — the global exists, it is
 * callable, its `from` is callable, and the encoded result is a string — so a
 * foreign `Buffer` global cannot produce a non-string body.
 */
export function nodeBufferBase64(bytes: Uint8Array): string | undefined {
  if (!("Buffer" in globalThis)) return undefined;
  const bufferGlobal: unknown = globalThis.Buffer;
  if (typeof bufferGlobal !== "function" || !("from" in bufferGlobal)) return undefined;
  const from: unknown = bufferGlobal.from;
  if (typeof from !== "function") return undefined;
  const encoded: unknown = from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString(
    "base64",
  );
  return typeof encoded === "string" ? encoded : undefined;
}

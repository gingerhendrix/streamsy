/**
 * Cross-runtime byte normalization for request bodies.
 *
 * `Uint8Array.prototype.buffer` is typed `ArrayBufferLike`, which also admits a
 * `SharedArrayBuffer`, and a view may cover only part of its backing buffer.
 * `BodyInit` accepts just a real `ArrayBuffer`, so the conversion is done by
 * construction here rather than by asserting the difference away.
 *
 * `@streamsy/core` publishes only a root entry point, so its equivalent helper
 * cannot be imported without adding a new public export; this module is the
 * client-side counterpart.
 */

/**
 * Copies the bytes of `view` into a fresh, exactly sized `ArrayBuffer`.
 *
 * The copy is deliberate: the result is handed to `fetch` as a request body,
 * and callers retain ownership of `view` and may reuse or mutate it as soon as
 * the append call returns.
 */
export function copyToArrayBuffer(view: Uint8Array): ArrayBuffer {
  const copy = new ArrayBuffer(view.byteLength);
  new Uint8Array(copy).set(view);
  return copy;
}

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

interface NodeBufferResult {
  value: HostValue;
}

interface HostObject {}

interface Base64Encoder {
  (encoding: "base64"): HostValue;
}

function isReferenceValue<Value>(candidate: Value): candidate is Value & object {
  return candidate !== null && Object(candidate) === candidate;
}

function isCallable<Value>(candidate: Value): candidate is Value & Function {
  if (!isReferenceValue(candidate)) return false;
  try {
    Function.prototype.toString.call(candidate);
    return true;
  } catch {
    return false;
  }
}

function isNodeBufferGlobal<Value>(candidate: Value): candidate is Value & NodeBufferGlobal {
  return (
    isReferenceValue(candidate) &&
    isCallable(candidate) &&
    "from" in candidate &&
    isCallable(candidate.from)
  );
}

function readHostProperty(value: HostObject, key: PropertyKey): HostValue {
  let owner: HostObject | null = value;
  while (owner !== null) {
    const descriptor = Object.getOwnPropertyDescriptor(owner, key);
    if (descriptor !== undefined) {
      return "value" in descriptor ? descriptor.value : descriptor.get?.call(value);
    }
    owner = Object.getPrototypeOf(owner);
  }
  return undefined;
}

function parseBase64Encoder(result: NodeBufferResult): Base64Encoder | undefined {
  const { value } = result;
  if (!isReferenceValue(value)) return undefined;
  let owner: object | null = value;
  while (owner !== null) {
    const descriptor = Object.getOwnPropertyDescriptor(owner, "toString");
    if (descriptor !== undefined) {
      const method: HostValue =
        "value" in descriptor ? descriptor.value : descriptor.get?.call(value);
      return isCallable(method) ? (encoding) => method.call(value, encoding) : undefined;
    }
    owner = Object.getPrototypeOf(owner);
  }
  return undefined;
}

export function nodeBufferBase64(bytes: Uint8Array): string | undefined {
  if (!("Buffer" in globalThis)) return undefined;
  const bufferGlobal = readHostProperty(globalThis, "Buffer");
  if (!isReferenceValue(bufferGlobal) || !isNodeBufferGlobal(bufferGlobal)) return undefined;

  // `Buffer` stays the receiver: `Buffer.from` is a static method and a host
  // implementation may rely on `this`.
  const view = bufferGlobal.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const encode = parseBase64Encoder({ value: view });
  if (encode === undefined) return undefined;

  // The produced value stays the receiver, so `toString` sees its own bytes.
  const encoded = encode("base64");
  if (
    encoded === null ||
    encoded === undefined ||
    isReferenceValue(encoded) ||
    Object.getPrototypeOf(Object(encoded)) !== String.prototype
  ) {
    return undefined;
  }
  return String.prototype.valueOf.call(encoded);
}

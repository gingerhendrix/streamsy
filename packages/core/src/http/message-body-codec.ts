import { Array } from "effect";
import { nodeBufferBase64, toArrayBuffer } from "./bytes.ts";

export interface MessageWithData {
  data: Uint8Array;
}

const decoder = new TextDecoder();

export function encodeHttpBody(
  messages: readonly MessageWithData[],
  contentType: string,
): string | ArrayBuffer {
  const lower = contentType.toLowerCase();

  if (lower.startsWith("application/json")) {
    const items = Array.map(messages, (msg) => decoder.decode(msg.data));

    return `[${Array.join(items, ",")}]`;
  }

  if (lower.startsWith("text/")) {
    return Array.join(
      Array.map(messages, (msg) => decoder.decode(msg.data)),
      "",
    );
  }

  return toArrayBuffer(concatBytes(messages));
}

export function emptyBodyForContentType(contentType: string): string | ArrayBuffer {
  return contentType.toLowerCase().startsWith("application/json") ? "[]" : "";
}

export function concatBytes(messages: readonly MessageWithData[]): Uint8Array {
  const totalLength = Array.reduce(messages, 0, (acc, msg) => acc + msg.data.length);
  const combined = new Uint8Array(totalLength);
  let pos = 0;

  for (const msg of messages) {
    combined.set(msg.data, pos);
    pos += msg.data.length;
  }

  return combined;
}

export function bytesToBase64(bytes: Uint8Array): string {
  const viaBuffer = nodeBufferBase64(bytes);

  if (viaBuffer !== undefined) return viaBuffer;
  let binary = "";
  const chunk = 0x8000;

  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }

  return btoa(binary);
}

import * as MessageBody from "./message-body-codec.ts";
import type { MessageWithData } from "./message-body-codec.ts";

export interface SseEncodingOptions {
  isJson: boolean;
  isText: boolean;
  useBase64: boolean;
}

export interface SseControlData {
  streamNextOffset: string;
  streamCursor?: string;
  streamClosed?: true;
  upToDate?: true;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function dataEvent(
  messages: readonly MessageWithData[],
  options: SseEncodingOptions,
): Uint8Array[] {
  const chunks: Uint8Array[] = [encode("event: data\n")];
  if (options.useBase64) {
    const combined = MessageBody.concatBytes(messages);
    chunks.push(encode(`data:${MessageBody.bytesToBase64(combined)}\n`));
  } else if (options.isJson) {
    const items = messages.map((msg) => decoder.decode(msg.data));
    chunks.push(encode("data:[\n"));
    for (let i = 0; i < items.length; i++) {
      const suffix = i < items.length - 1 ? "," : "";
      chunks.push(encode(`data:${items[i]}${suffix}\n`));
    }
    chunks.push(encode("data:]\n"));
  } else {
    const text = messages.map((msg) => decoder.decode(msg.data)).join("");
    for (const line of text.split(/\r\n|\r|\n/)) {
      chunks.push(encode(`data:${line}\n`));
    }
  }
  chunks.push(encode("\n"));
  return chunks;
}

export function controlEvent(data: SseControlData): Uint8Array {
  return encode(`event: control\ndata:${JSON.stringify(data)}\n\n`);
}

function encode(value: string): Uint8Array {
  return encoder.encode(value);
}

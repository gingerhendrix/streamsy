import { MessageBodyCodec, type MessageWithData } from "./message-body-codec.ts";

export interface SseEncodingOptions {
  isJson: boolean;
  isText: boolean;
  useBase64: boolean;
}

export class SseEventEncoder {
  private encoder = new TextEncoder();
  private decoder = new TextDecoder();

  constructor(private bodyCodec: MessageBodyCodec) {}

  dataEvent(messages: MessageWithData[], options: SseEncodingOptions): Uint8Array[] {
    const chunks: Uint8Array[] = [this.encode("event: data\n")];
    if (options.useBase64) {
      const combined = this.bodyCodec.concatBytes(messages);
      chunks.push(this.encode(`data:${this.bodyCodec.bytesToBase64(combined)}\n`));
    } else if (options.isJson) {
      const items = messages.map((msg) => this.decoder.decode(msg.data));
      chunks.push(this.encode("data:[\n"));
      for (let i = 0; i < items.length; i++) {
        const suffix = i < items.length - 1 ? "," : "";
        chunks.push(this.encode(`data:${items[i]}${suffix}\n`));
      }
      chunks.push(this.encode("data:]\n"));
    } else {
      const text = messages.map((msg) => this.decoder.decode(msg.data)).join("");
      for (const line of text.split(/\r\n|\r|\n/)) {
        chunks.push(this.encode(`data:${line}\n`));
      }
    }
    chunks.push(this.encode("\n"));
    return chunks;
  }

  controlEvent(data: Record<string, unknown>): Uint8Array {
    return this.encode(`event: control\ndata:${JSON.stringify(data)}\n\n`);
  }

  private encode(value: string): Uint8Array {
    return this.encoder.encode(value);
  }
}

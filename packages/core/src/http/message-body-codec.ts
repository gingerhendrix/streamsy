export interface MessageWithData {
  data: Uint8Array;
}

export class MessageBodyCodec {
  private decoder = new TextDecoder();

  encodeHttpBody(messages: MessageWithData[], contentType: string): BodyInit {
    const lower = contentType.toLowerCase();
    if (lower.startsWith("application/json")) {
      const items = messages.map((msg) => this.decoder.decode(msg.data));
      return `[${items.join(",")}]`;
    }
    if (lower.startsWith("text/")) {
      return messages.map((msg) => this.decoder.decode(msg.data)).join("");
    }
    return this.concatBytes(messages).buffer as ArrayBuffer;
  }

  emptyBodyForContentType(contentType: string): BodyInit {
    return contentType.toLowerCase().startsWith("application/json") ? "[]" : "";
  }

  concatBytes(messages: MessageWithData[]): Uint8Array {
    const totalLength = messages.reduce((acc, msg) => acc + msg.data.length, 0);
    const combined = new Uint8Array(totalLength);
    let pos = 0;
    for (const msg of messages) {
      combined.set(msg.data, pos);
      pos += msg.data.length;
    }
    return combined;
  }

  bytesToBase64(bytes: Uint8Array): string {
    const BufferRef = (
      globalThis as { Buffer?: { from: (...args: unknown[]) => { toString(enc: string): string } } }
    ).Buffer;
    if (BufferRef) {
      return BufferRef.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("base64");
    }
    let binary = "";
    const chunk = 0x8000;
    for (let i = 0; i < bytes.length; i += chunk) {
      binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
    }
    return btoa(binary);
  }
}

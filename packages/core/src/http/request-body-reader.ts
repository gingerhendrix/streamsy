import { HttpResponseFactory } from "./responses.ts";

export type BodyReadResult =
  | { ok: true; data: Uint8Array; byteLength: number }
  | { ok: false; response: Response };

export class RequestBodyReader {
  constructor(
    private maxMessageSize: number,
    private responses: HttpResponseFactory,
  ) {}

  async read(request: Request): Promise<BodyReadResult> {
    let data: ArrayBuffer;
    try {
      data = await request.arrayBuffer();
    } catch (error) {
      console.error("Error reading request body:", error);
      return { ok: false, response: this.responses.payloadTooLarge() };
    }
    if (data.byteLength > this.maxMessageSize) {
      return { ok: false, response: this.responses.payloadTooLarge() };
    }
    return { ok: true, data: new Uint8Array(data), byteLength: data.byteLength };
  }
}

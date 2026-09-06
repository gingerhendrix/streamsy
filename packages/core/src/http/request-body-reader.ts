import { Effect } from "effect";
import type { HttpServerRequest } from "effect/unstable/http";
import { HttpResponseFactory } from "./responses.ts";

export type BodyReadResult =
  | { ok: true; data: Uint8Array; byteLength: number }
  | { ok: false; response: Response };

export class RequestBodyReader {
  constructor(
    private maxMessageSize: number,
    private responses: HttpResponseFactory,
  ) {}

  readonly read = Effect.fn("Http.readBody")(function* (
    this: RequestBodyReader,
    request: HttpServerRequest.HttpServerRequest,
  ): Effect.fn.Return<BodyReadResult> {
    return yield* request.arrayBuffer.pipe(
      Effect.map(
        (data): BodyReadResult =>
          data.byteLength > this.maxMessageSize
            ? { ok: false, response: this.responses.payloadTooLarge() }
            : { ok: true, data: new Uint8Array(data), byteLength: data.byteLength },
      ),
      Effect.orElseSucceed(() => ({
        ok: false as const,
        response: this.responses.payloadTooLarge(),
      })),
    );
  });
}

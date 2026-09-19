import { Effect } from "effect";
import type { HttpServerRequest } from "effect/unstable/http";
import * as Responses from "./responses.ts";

export type BodyReadResult =
  | { ok: true; data: Uint8Array; byteLength: number }
  | { ok: false; response: Response };

export const requestBodyReader = (maxMessageSize: number) => ({
  read: Effect.fn("Http.readBody")(function* (
    request: HttpServerRequest.HttpServerRequest,
  ): Effect.fn.Return<BodyReadResult> {
    return yield* request.arrayBuffer.pipe(
      Effect.map(
        (data): BodyReadResult =>
          data.byteLength > maxMessageSize
            ? { ok: false, response: Responses.payloadTooLarge() }
            : { ok: true, data: new Uint8Array(data), byteLength: data.byteLength },
      ),
      Effect.orElseSucceed(() => ({
        ok: false as const,
        response: Responses.payloadTooLarge(),
      })),
    );
  }),
});

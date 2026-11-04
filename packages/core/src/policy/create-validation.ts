import { DateTime, Effect, Option, Result } from "effect";
import { InvalidCreateRequest } from "../protocol/errors.ts";
import type { StreamId } from "../schema/index.ts";
import type { CreateOptions } from "./options.ts";
import { frameMessages } from "./message-framer.ts";

/** Validate expiry before either transport accesses a stream. */
export const validateCreateOptions = Effect.fn("Create.validateOptions")(function* (
  id: StreamId,
  options: CreateOptions = {},
) {
  if (options.ttlSeconds !== undefined && options.expiresAt !== undefined)
    return yield* new InvalidCreateRequest({
      id,
      message: "Cannot specify both Stream-TTL and Stream-Expires-At",
    });

  if (options.expiresAt !== undefined && Option.isNone(DateTime.make(options.expiresAt)))
    return yield* new InvalidCreateRequest({ id, message: "Invalid Stream-Expires-At format" });

  return undefined;
});

/** Create bodies use typed JSON failures without changing append framing. */
export function frameCreateMessages(
  id: StreamId,
  data: Uint8Array | undefined,
  contentType: string,
): Result.Result<Uint8Array[], InvalidCreateRequest> {
  return Result.try({
    try: () => (data === undefined ? [] : frameMessages(data, contentType)),
    catch: (error) => {
      if (error instanceof SyntaxError)
        return new InvalidCreateRequest({ id, message: "Invalid JSON" });
      throw error;
    },
  });
}

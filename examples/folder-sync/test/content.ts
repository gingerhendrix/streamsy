import { Effect, Stream } from "effect";
import { BlobStore } from "../server/blob-store.ts";
import { prepareContent } from "../server/domain.ts";

/** Put `bytes` in the blob store and give the hash and size that a commit names. */
export const storedContent = Effect.fn("Test.storedContent")(function* (bytes: Uint8Array) {
  const content = yield* prepareContent(bytes);
  yield* (yield* BlobStore).put(content.hash, Stream.succeed(bytes));

  return content;
});

/** `storedContent` for UTF-8 text. */
export const storedText = (text: string) => storedContent(new TextEncoder().encode(text));

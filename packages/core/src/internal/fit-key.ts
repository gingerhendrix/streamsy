import { Array, Effect } from "effect";

/** `#` and the 64 hex digits of a SHA-256 end a hashed key. */
const DIGEST_LENGTH = 65;

// Storage limits count code points, which is what the spread yields.
// oxlint-disable-next-line typescript/no-misused-spread -- code points are the intended unit
const codePoints = (value: string): ReadonlyArray<string> => [...value];

/** WebCrypto is the SHA-256 that Bun, Node, workerd and browsers all provide. */
const sha256Hex = (value: string): Effect.Effect<string> =>
  Effect.promise(() =>
    globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)),
  ).pipe(
    Effect.map((digest) =>
      Array.join(
        Array.map(Array.fromIterable(new Uint8Array(digest)), (byte) =>
          byte.toString(16).padStart(2, "0"),
        ),
        "",
      ),
    ),
  );

/**
 * Fits `key` into `limit` code points. A key within the limit is returned as it is, so
 * keys that storage already holds keep working. A longer key becomes the first code
 * points of `readable`, then `#` and the SHA-256 hex of the full key. The result is at
 * most `limit` code points, and exactly `limit` when `readable` is at least as long.
 */
export const fitKey = (
  key: string,
  limit: number,
  readable: string = key,
): Effect.Effect<string> => {
  if (codePoints(key).length <= limit) return Effect.succeed(key);

  const prefix = Array.join(Array.take(codePoints(readable), limit - DIGEST_LENGTH), "");

  return Effect.map(sha256Hex(key), (digest) => `${prefix}#${digest}`);
};

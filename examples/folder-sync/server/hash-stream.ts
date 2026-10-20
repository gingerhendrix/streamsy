import { Effect, Stream } from "effect";
import { limitBytes } from "./bytes.ts";

/**
 * Hash a body while it streams, and fail once it passes `limit`. Each counted
 * chunk goes to `write` after it enters the digest. Bun only: it uses Bun's hasher.
 */
export const hashStream = <E, E2, E3 = never, R = never>(
  body: Stream.Stream<Uint8Array, E>,
  limit: number,
  tooLarge: (size: number) => E2,
  write: (chunk: Uint8Array) => Effect.Effect<void, E3, R> = () => Effect.void,
): Effect.Effect<{ hash: string; size: number }, E | E2 | E3, R> =>
  Effect.suspend(() => {
    const digest = new Bun.CryptoHasher("sha256");
    let size = 0;

    return limitBytes(body, limit, tooLarge).pipe(
      Stream.runForEach((chunk) => {
        size += chunk.length;
        digest.update(chunk);

        return write(chunk);
      }),
      Effect.map(() => ({ hash: digest.digest("hex"), size })),
    );
  });

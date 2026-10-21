import { Array, Effect, Stream } from "effect";

/** Join chunks into one buffer of the given total size. */
export const concatBytes = (chunks: Iterable<Uint8Array>, size: number) => {
  const bytes = new Uint8Array(size);
  let offset = 0;

  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }

  return bytes;
};

/**
 * Strict UTF-8 text with no NUL. Anything else is binary content.
 * With `truncated`, a code point cut at the end of a prefix does not fail the decode.
 */
export const decodeText = (bytes: Uint8Array, truncated = false): string | undefined => {
  try {
    const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes, {
      stream: truncated,
    });

    return text.includes("\0") ? undefined : text;
  } catch {
    return undefined;
  }
};

/** Lowercase hex, the form of every content hash in the protocol. */
export const hex = (digest: Uint8Array) =>
  Array.join(
    Array.map(Array.fromIterable(digest), (byte) => byte.toString(16).padStart(2, "0")),
    "",
  );

/** Pass chunks through and fail as soon as the running size passes `limit`. */
export const limitBytes = <E, E2>(
  body: Stream.Stream<Uint8Array, E>,
  limit: number,
  tooLarge: (size: number) => E2,
): Stream.Stream<Uint8Array, E | E2> =>
  Stream.suspend(() => {
    let size = 0;

    return body.pipe(
      Stream.mapEffect((chunk) => {
        size += chunk.length;

        return size > limit ? Effect.fail(tooLarge(size)) : Effect.succeed(chunk);
      }),
    );
  });

/** Collect a bounded stream into one buffer. Use only for bodies with a small limit. */
export const collectBytes = Effect.fn("Bytes.collect")(function* <E, E2>(
  body: Stream.Stream<Uint8Array, E>,
  limit: number,
  tooLarge: (size: number) => E2,
) {
  const chunks = yield* Stream.runCollect(limitBytes(body, limit, tooLarge));

  return concatBytes(
    chunks,
    Array.reduce(chunks, 0, (size, chunk) => size + chunk.length),
  );
});

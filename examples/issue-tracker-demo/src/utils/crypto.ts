import { Crypto, Effect } from "effect";

/** Browser entropy for optimistic row and transaction identifiers. */
export const browserCrypto = Crypto.make({
  randomBytes: (size) => globalThis.crypto.getRandomValues(new Uint8Array(size)),
  digest: (algorithm, data) =>
    Effect.promise(() =>
      globalThis.crypto.subtle.digest(algorithm, new Uint8Array(data).buffer),
    ).pipe(Effect.map((buffer) => new Uint8Array(buffer))),
});

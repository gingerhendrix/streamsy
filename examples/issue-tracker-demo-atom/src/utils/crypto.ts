import { SecureRandom } from "effect/crypto";

/** Browser entropy for optimistic row and transaction identifiers. */
export const browserCrypto = SecureRandom.make((size) =>
  globalThis.crypto.getRandomValues(new Uint8Array(size)),
);

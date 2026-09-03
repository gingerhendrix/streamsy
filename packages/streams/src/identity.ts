import { Schema } from "effect";

const IDENTITY_ENCODING_PREFIX = "streamsy.identity.v1:";
const MAX_IDENTITY_NAME_LENGTH = 256;
const isString = Schema.is(Schema.String);

/** A mesh-assigned identity. It is deliberately independent of a stream address. */
export interface StreamIdentity {
  readonly name: string;
}

/** Construct a validated, canonical stream identity. */
export function streamIdentity(name: string): StreamIdentity {
  if (!isString(name)) throw new TypeError("Stream identity name must be a string");
  const canonical = name.normalize("NFC");
  if (canonical.trim().length === 0) {
    throw new TypeError("Stream identity name must not be empty or whitespace-only");
  }
  if (Array.from(canonical).length > MAX_IDENTITY_NAME_LENGTH) {
    throw new TypeError(
      `Stream identity name must not exceed ${MAX_IDENTITY_NAME_LENGTH} characters`,
    );
  }
  if (Array.from(canonical).some(isControlCharacter)) {
    throw new TypeError("Stream identity name must not contain control characters");
  }
  return Object.freeze({ name: canonical });
}

function isControlCharacter(character: string): boolean {
  const codePoint = character.codePointAt(0)!;
  return codePoint <= 0x1f || codePoint === 0x7f;
}

export function streamIdentityEquals(a: StreamIdentity, b: StreamIdentity): boolean {
  return a.name === b.name;
}

/**
 * Encode an identity for durable keys and digest inputs.
 *
 * The explicit version owns the complete encoded shape. A future lifetime or
 * incarnation can therefore use a new version without changing v1 equality.
 */
export function encodeStreamIdentity(identity: StreamIdentity): string {
  const validated = streamIdentity(identity.name);
  return `${IDENTITY_ENCODING_PREFIX}${encodeURIComponent(validated.name)}`;
}

export function decodeStreamIdentity(encoded: string): StreamIdentity {
  if (!encoded.startsWith(IDENTITY_ENCODING_PREFIX)) {
    throw new TypeError("Unsupported stream identity encoding version");
  }
  const component = encoded.slice(IDENTITY_ENCODING_PREFIX.length);
  let name: string;
  try {
    name = decodeURIComponent(component);
  } catch {
    throw new TypeError("Malformed stream identity encoding");
  }
  const identity = streamIdentity(name);
  if (encodeStreamIdentity(identity) !== encoded) {
    throw new TypeError("Non-canonical stream identity encoding");
  }
  return identity;
}

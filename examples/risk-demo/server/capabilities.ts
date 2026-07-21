/**
 * Capability tokens (bearer) with constant-time verification.
 *
 * A token is `rsk_<tokenId>_<secret>`, both hex. Only the tokenId and a SHA-256
 * verifier hash of the secret are persisted — the raw token is returned exactly
 * once at issuance and never stored, logged, or placed in any stream. Verifying
 * looks the row up by tokenId, then compares the secret's hash in constant time.
 */

const encoder = new TextEncoder();

export type CapabilityRole = "host" | "player";

export interface Capability {
  gameId: string;
  playerId: string;
  role: CapabilityRole;
}

export interface IssuedCapability extends Capability {
  /** The raw bearer token — surfaced once, never persisted. */
  token: string;
  tokenId: string;
  verifierHash: string;
}

function toHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}

function randomHex(byteLength: number): string {
  return toHex(crypto.getRandomValues(new Uint8Array(byteLength)));
}

export async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(value));
  return toHex(new Uint8Array(digest));
}

/** Length-independent constant-time comparison of two hex strings. */
export function constantTimeEqual(a: string, b: string): boolean {
  const max = Math.max(a.length, b.length);
  let mismatch = a.length === b.length ? 0 : 1;
  for (let i = 0; i < max; i += 1) {
    mismatch |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  }
  return mismatch === 0;
}

/** Mint a fresh capability. The returned `token` must be shown to the caller once. */
export async function issueCapability(capability: Capability): Promise<IssuedCapability> {
  const tokenId = randomHex(9);
  const secret = randomHex(24);
  const verifierHash = await sha256Hex(secret);
  return {
    ...capability,
    tokenId,
    verifierHash,
    token: `rsk_${tokenId}_${secret}`,
  };
}

export interface ParsedToken {
  tokenId: string;
  secret: string;
}

export function parseToken(token: string): ParsedToken | null {
  const parts = token.split("_");
  if (parts.length !== 3 || parts[0] !== "rsk") return null;
  const [, tokenId, secret] = parts;
  if (!tokenId || !secret) return null;
  return { tokenId, secret };
}

/** Extract a bearer token from an `Authorization` header, or null. */
export function bearerToken(header: string | null | undefined): string | null {
  if (!header) return null;
  const match = /^Bearer\s+(.+)$/i.exec(header.trim());
  return match ? match[1]!.trim() : null;
}

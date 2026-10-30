import type { CreateOptions } from "../../src/protocol/options.ts";

export const expiryValidationCases: ReadonlyArray<{
  readonly name: string;
  readonly options: CreateOptions;
  readonly headers: Record<string, string>;
  readonly message: string;
}> = [
  {
    name: "TTL and expiry",
    options: { ttlSeconds: 5, expiresAt: "2030-01-01T00:00:00Z" },
    headers: { "stream-ttl": "5", "stream-expires-at": "2030-01-01T00:00:00Z" },
    message: "Cannot specify both Stream-TTL and Stream-Expires-At",
  },
  {
    name: "zero TTL and expiry",
    options: { ttlSeconds: 0, expiresAt: "2030-01-01T00:00:00Z" },
    headers: { "stream-ttl": "0", "stream-expires-at": "2030-01-01T00:00:00Z" },
    message: "Cannot specify both Stream-TTL and Stream-Expires-At",
  },
  {
    name: "invalid expiry",
    options: { expiresAt: "tomorrow" },
    headers: { "stream-expires-at": "tomorrow" },
    message: "Invalid Stream-Expires-At format",
  },
];

export const invalidJson = new TextEncoder().encode("{");

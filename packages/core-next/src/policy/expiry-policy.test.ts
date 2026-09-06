import { expect, it } from "bun:test";
import { StreamId } from "../schema/index.ts";
import { ZERO_OFFSET } from "../offset/index.ts";
import { computeExpiresAtMs, isExpired } from "./expiry-policy.ts";
it("TTL uses supplied current time and takes precedence over absolute expiry", () => {
  expect(computeExpiresAtMs({ ttlSeconds: 30, expiresAt: "2030-01-01" }, 1000)).toBe(31000);
});
it("absolute expiry is parsed and absent expiry stays absent", () => {
  expect(computeExpiresAtMs({ expiresAt: "2030-01-01" }, 0)).toBe(Date.parse("2030-01-01"));
  expect(computeExpiresAtMs({}, 0)).toBeUndefined();
});
it("expiry includes the exact deadline and excludes absent or future deadlines", () => {
  const record = {
    id: StreamId.make("s"),
    config: { contentType: "text/plain", createdAt: 0 },
    lifecycle: { closed: false, softDeleted: false },
    currentOffset: ZERO_OFFSET,
  };
  expect(isExpired(record, 100)).toBe(false);
  expect(isExpired({ ...record, lifecycle: { ...record.lifecycle, expiresAtMs: 100 } }, 100)).toBe(
    true,
  );
  expect(isExpired({ ...record, lifecycle: { ...record.lifecycle, expiresAtMs: 101 } }, 100)).toBe(
    false,
  );
});

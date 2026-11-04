import { expect, it } from "vitest";
import { StreamId } from "../../src/schema/index.ts";
import { ZERO_OFFSET } from "../../src/offset/index.ts";
import { computeExpiresAtMs, isExpired, isValidExpiresAt } from "../../src/policy/expiry-policy.ts";

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

// Measured with Effect 4.0.5 DateTime.make at b778af40 on 2026-11-04.
// Keep literal expectations so newer Effect parsers cannot change the accepted wire inputs.
const expiryCompatibility: ReadonlyArray<readonly [string, boolean]> = [
  ["", false],
  ["0", true],
  ["2028", true],
  ["1780000000000", false],
  ["not-a-date", false],
  ["Sat, 01 Jan 2028 00:00:00 UTC", false],
  ["Sat, 01 Jan 2028 00:00:00 GMT", true],
  ["2028-01-01", true],
  ["2028-01-01Z", true],
  ["2028-01-01z", false],
  ["2028-01-01GMT", true],
  ["2028-01-01gmt", false],
  ["2028-01-01UTC", false],
  ["2028-01-01+01:00", false],
  ["2028-01-01-0100", false],
  ["2028-01-01Z ", true],
  ["2028-01-01[UTC]", false],
  ["2028-01-01T00:00:00", true],
  ["2028-01-01T00:00:00Z", true],
  ["2028-01-01T00:00:00z", false],
  ["2028-01-01T00:00:00GMT", false],
  ["2028-01-01T00:00:00gmt", false],
  ["2028-01-01T00:00:00UTC", false],
  ["2028-01-01T00:00:00+01:00", true],
  ["2028-01-01T00:00:00-0100", true],
  ["2028-01-01T00:00:00Z ", false],
  ["2028-01-01T00:00:00[UTC]", false],
  ["2028-01-01t00:00:00", true],
  ["2028-01-01t00:00:00Z", true],
  ["2028-01-01t00:00:00z", false],
  ["2028-01-01t00:00:00GMT", false],
  ["2028-01-01t00:00:00gmt", false],
  ["2028-01-01t00:00:00UTC", false],
  ["2028-01-01t00:00:00+01:00", true],
  ["2028-01-01t00:00:00-0100", true],
  ["2028-01-01t00:00:00Z ", false],
  ["2028-01-01t00:00:00[UTC]", false],
  ["2028-02-30T00:00:00", true],
  ["2028-02-30T00:00:00Z", true],
  ["2028-02-30T00:00:00z", false],
  ["2028-02-30T00:00:00GMT", false],
  ["2028-02-30T00:00:00gmt", false],
  ["2028-02-30T00:00:00UTC", false],
  ["2028-02-30T00:00:00+01:00", true],
  ["2028-02-30T00:00:00-0100", true],
  ["2028-02-30T00:00:00Z ", false],
  ["2028-02-30T00:00:00[UTC]", false],
  ["2028-13-01T00:00:00", false],
  ["2028-13-01T00:00:00Z", false],
  ["2028-13-01T00:00:00z", false],
  ["2028-13-01T00:00:00GMT", false],
  ["2028-13-01T00:00:00gmt", false],
  ["2028-13-01T00:00:00UTC", false],
  ["2028-13-01T00:00:00+01:00", false],
  ["2028-13-01T00:00:00-0100", false],
  ["2028-13-01T00:00:00Z ", false],
  ["2028-13-01T00:00:00[UTC]", false],
];

for (const [value, accepted] of expiryCompatibility) {
  it(`preserves expiry acceptance for ${JSON.stringify(value)}: ${accepted}`, () => {
    expect(isValidExpiresAt(value)).toBe(accepted);
  });
}

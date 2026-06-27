/**
 * Unit coverage for the create-config-matcher helper extracted from
 * StreamProtocol.configMatches. Pins idempotent re-create rules:
 * default content type, forkedFrom/forkOffset matching, inherited fork
 * expiry, and closed-state agreement.
 */

import { describe, it, expect } from "vitest";
import { configMatches } from "../../protocol/helpers/create-config-matcher.ts";
import { ZERO_OFFSET } from "../../protocol/helpers/offset-generator.ts";
import type { StreamRecord } from "../../types/storage.ts";
import type { CreateOptions } from "../../types/protocol.ts";

function makeRecord(
  overrides: {
    contentType?: string;
    ttlSeconds?: number;
    expiresAt?: string;
    closed?: boolean;
    forkedFrom?: string;
    forkOffset?: string;
    forkSubOffset?: number;
  } = {},
): StreamRecord {
  return {
    id: "stream",
    config: {
      contentType: overrides.contentType ?? "application/octet-stream",
      ttlSeconds: overrides.ttlSeconds,
      expiresAt: overrides.expiresAt,
      createdAt: 0,
    },
    lifecycle: {
      forkedFrom: overrides.forkedFrom,
      forkOffset: overrides.forkOffset,
      forkSubOffset: overrides.forkSubOffset,
      closed: overrides.closed,
    },
    currentOffset: ZERO_OFFSET,
    counter: 0,
  };
}

describe("configMatches — content type", () => {
  it("defaults options.contentType to application/octet-stream when both sides omit it", () => {
    const existing = makeRecord({ contentType: "application/octet-stream" });
    expect(configMatches(existing, {} as CreateOptions)).toBe(true);
  });

  it("returns false when content types differ", () => {
    const existing = makeRecord({ contentType: "application/json" });
    expect(configMatches(existing, { contentType: "text/plain" })).toBe(false);
  });

  it("matches case-insensitively and ignores parameters", () => {
    const existing = makeRecord({ contentType: "application/json" });
    expect(configMatches(existing, { contentType: "Application/JSON; charset=utf-8" })).toBe(true);
  });
});

describe("configMatches — forkedFrom / forkOffset", () => {
  it("matches when both sides agree on forkedFrom and forkOffset", () => {
    const existing = makeRecord({
      contentType: "application/json",
      forkedFrom: "parent",
      forkOffset: ZERO_OFFSET,
    });
    expect(
      configMatches(existing, {
        contentType: "application/json",
        forkedFrom: "parent",
        forkOffset: ZERO_OFFSET,
      }),
    ).toBe(true);
  });

  it("returns false when forkedFrom differs (existing forked, options not)", () => {
    const existing = makeRecord({ forkedFrom: "parent" });
    expect(configMatches(existing, {})).toBe(false);
  });

  it("returns false when forkedFrom differs (options forked, existing not)", () => {
    const existing = makeRecord();
    expect(configMatches(existing, { forkedFrom: "parent" })).toBe(false);
  });

  it("treats unspecified forkOffset on the options side as don't-care", () => {
    const existing = makeRecord({ forkedFrom: "parent", forkOffset: ZERO_OFFSET });
    expect(configMatches(existing, { forkedFrom: "parent" })).toBe(true);
  });

  it("returns false when caller specifies a forkOffset that mismatches existing", () => {
    const existing = makeRecord({ forkedFrom: "parent", forkOffset: ZERO_OFFSET });
    expect(
      configMatches(existing, {
        forkedFrom: "parent",
        forkOffset: "0000000000000001_0000000000000000",
      }),
    ).toBe(false);
  });
});

describe("configMatches — expiry", () => {
  it("inherits source expiry on a fork when ttlSeconds and expiresAt are both omitted", () => {
    const existing = makeRecord({ forkedFrom: "parent", ttlSeconds: 60 });
    expect(configMatches(existing, { forkedFrom: "parent" })).toBe(true);
  });

  it("inherits source expiresAt on a fork when caller omits both expiry fields", () => {
    const existing = makeRecord({ forkedFrom: "parent", expiresAt: "2030-01-01T00:00:00Z" });
    expect(configMatches(existing, { forkedFrom: "parent" })).toBe(true);
  });

  it("returns false when ttlSeconds differs and inheritance does not apply (no fork)", () => {
    const existing = makeRecord({ ttlSeconds: 60 });
    expect(configMatches(existing, { ttlSeconds: 120 })).toBe(false);
  });

  it("returns false when expiresAt differs and inheritance does not apply (no fork)", () => {
    const existing = makeRecord({ expiresAt: "2030-01-01T00:00:00Z" });
    expect(configMatches(existing, { expiresAt: "2031-01-01T00:00:00Z" })).toBe(false);
  });

  it("returns false when caller supplies a non-matching ttlSeconds on a fork (inheritance does not apply)", () => {
    const existing = makeRecord({ forkedFrom: "parent", ttlSeconds: 60 });
    expect(configMatches(existing, { forkedFrom: "parent", ttlSeconds: 120 })).toBe(false);
  });
});

describe("configMatches — fork sub-offset", () => {
  it("matches when both sides agree on a positive sub-offset", () => {
    const existing = makeRecord({
      forkedFrom: "parent",
      forkOffset: ZERO_OFFSET,
      forkSubOffset: 2,
    });
    expect(
      configMatches(existing, { forkedFrom: "parent", forkOffset: ZERO_OFFSET, forkSubOffset: 2 }),
    ).toBe(true);
  });

  it("returns false when sub-offsets differ", () => {
    const existing = makeRecord({
      forkedFrom: "parent",
      forkOffset: ZERO_OFFSET,
      forkSubOffset: 2,
    });
    expect(
      configMatches(existing, { forkedFrom: "parent", forkOffset: ZERO_OFFSET, forkSubOffset: 3 }),
    ).toBe(false);
  });

  it("treats an absent sub-offset and 0 as equal", () => {
    const existing = makeRecord({ forkedFrom: "parent", forkOffset: ZERO_OFFSET });
    expect(
      configMatches(existing, { forkedFrom: "parent", forkOffset: ZERO_OFFSET, forkSubOffset: 0 }),
    ).toBe(true);
  });

  it("returns false when caller drops a sub-offset the existing fork was created with", () => {
    const existing = makeRecord({
      forkedFrom: "parent",
      forkOffset: ZERO_OFFSET,
      forkSubOffset: 2,
    });
    expect(configMatches(existing, { forkedFrom: "parent", forkOffset: ZERO_OFFSET })).toBe(false);
  });
});

describe("configMatches — closed state", () => {
  it("matches when both sides are open", () => {
    const existing = makeRecord();
    expect(configMatches(existing, {})).toBe(true);
  });

  it("matches when both sides are closed", () => {
    const existing = makeRecord({ closed: true });
    expect(configMatches(existing, { closed: true })).toBe(true);
  });

  it("returns false when existing is closed and caller omits closed", () => {
    const existing = makeRecord({ closed: true });
    expect(configMatches(existing, {})).toBe(false);
  });

  it("returns false when caller asks for closed but existing is open", () => {
    const existing = makeRecord();
    expect(configMatches(existing, { closed: true })).toBe(false);
  });
});

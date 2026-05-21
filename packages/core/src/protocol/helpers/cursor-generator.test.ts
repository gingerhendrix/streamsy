/**
 * Unit coverage for the cursor-generator helper extracted from
 * StreamProtocol.generateCursor. Pins the epoch/interval, the
 * no-previous and stale-previous paths, and the random bump rules used
 * when the previous cursor is at or above the current interval.
 */

import { describe, it, expect } from "vitest";
import type { Clock } from "../../types/storage.ts";
import {
  CURSOR_EPOCH_MS,
  CURSOR_INTERVAL_MS,
  generateCursor,
} from "../../protocol/helpers/cursor-generator.ts";

function fixedClock(nowMs: number): Clock {
  return {
    now: () => nowMs,
    date: (value?: number | string) => new Date(value ?? nowMs),
  };
}

describe("CURSOR_EPOCH_MS / CURSOR_INTERVAL_MS", () => {
  it("epoch is 2024-10-09T00:00:00.000Z", () => {
    expect(CURSOR_EPOCH_MS).toBe(new Date("2024-10-09T00:00:00.000Z").getTime());
  });

  it("interval is 20 seconds", () => {
    expect(CURSOR_INTERVAL_MS).toBe(20_000);
  });
});

describe("generateCursor — no previous cursor", () => {
  it("returns the current interval as a string", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 5 * CURSOR_INTERVAL_MS);
    expect(generateCursor(clock)).toBe("5");
  });

  it("returns 0 at the epoch boundary", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS);
    expect(generateCursor(clock)).toBe("0");
  });

  it("floors fractional intervals", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 3 * CURSOR_INTERVAL_MS + 1);
    expect(generateCursor(clock)).toBe("3");
  });

  it("treats an empty-string previous cursor as no previous cursor", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 7 * CURSOR_INTERVAL_MS);
    expect(generateCursor(clock, "")).toBe("7");
  });
});

describe("generateCursor — previous cursor below current interval", () => {
  it("returns the current interval as a string", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 10 * CURSOR_INTERVAL_MS);
    expect(generateCursor(clock, "3")).toBe("10");
  });

  it("ignores the random source when advancing to the current interval", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 10 * CURSOR_INTERVAL_MS);
    let called = false;
    const random = () => {
      called = true;
      return 0.5;
    };
    expect(generateCursor(clock, "3", random)).toBe("10");
    expect(called).toBe(false);
  });
});

describe("generateCursor — previous cursor at or above current interval", () => {
  it("bumps by at least 1 when random returns 0", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 5 * CURSOR_INTERVAL_MS);
    expect(generateCursor(clock, "5", () => 0)).toBe("6");
  });

  it("uses Math.floor(random() * 180) for the bump amount", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 5 * CURSOR_INTERVAL_MS);
    // 0.5 * 180 = 90 -> +90
    expect(generateCursor(clock, "5", () => 0.5)).toBe("95");
  });

  it("caps the bump at +179 when random approaches 1", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 5 * CURSOR_INTERVAL_MS);
    // Math.floor(0.9999... * 180) === 179
    expect(generateCursor(clock, "5", () => 0.9999999999)).toBe("184");
  });

  it("bumps from a previous cursor strictly above the current interval", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 5 * CURSOR_INTERVAL_MS);
    expect(generateCursor(clock, "100", () => 0)).toBe("101");
  });

  it("bumps when previous equals the current interval", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 12 * CURSOR_INTERVAL_MS);
    expect(generateCursor(clock, "12", () => 0.25)).toBe(String(12 + Math.floor(0.25 * 180)));
  });

  it("defaults the random source to Math.random when not provided", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 5 * CURSOR_INTERVAL_MS);
    const result = generateCursor(clock, "5");
    const parsed = parseInt(result, 10);
    expect(parsed).toBeGreaterThanOrEqual(6);
    expect(parsed).toBeLessThanOrEqual(184);
  });
});

describe("generateCursor — previous cursor parsing", () => {
  it("parses leading-digit strings via parseInt(..., 10)", () => {
    const clock = fixedClock(CURSOR_EPOCH_MS + 5 * CURSOR_INTERVAL_MS);
    // parseInt("7abc", 10) === 7; 7 > 5 -> bump path
    expect(generateCursor(clock, "7abc", () => 0)).toBe("8");
  });

  it("treats NaN-parsed cursors as not below current interval (bumps from NaN)", () => {
    // parseInt("abc", 10) === NaN; NaN < currentInterval is false, so bump branch.
    // NaN + bump === NaN -> String(NaN) === "NaN".
    const clock = fixedClock(CURSOR_EPOCH_MS + 5 * CURSOR_INTERVAL_MS);
    expect(generateCursor(clock, "abc", () => 0)).toBe("NaN");
  });
});

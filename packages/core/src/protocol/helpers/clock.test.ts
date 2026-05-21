/**
 * Unit coverage for the default `systemClock` extracted from
 * StreamProtocol. Pins `now()` to `Date.now()` and the
 * `date(value?)` semantics that previously lived inline in protocol.ts.
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import { systemClock } from "../../protocol/helpers/clock.ts";

afterEach(() => {
  vi.useRealTimers();
});

describe("systemClock.now", () => {
  it("returns Date.now()", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2025-06-01T12:34:56.000Z"));
    expect(systemClock.now()).toBe(Date.now());
    expect(systemClock.now()).toBe(new Date("2025-06-01T12:34:56.000Z").getTime());
  });

  it("advances when system time advances", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2025-06-01T00:00:00.000Z"));
    const before = systemClock.now();
    vi.setSystemTime(new Date("2025-06-01T00:00:05.000Z"));
    expect(systemClock.now() - before).toBe(5_000);
  });
});

describe("systemClock.date", () => {
  it("returns the current Date when called with no argument", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2025-06-01T12:34:56.000Z"));
    const date = systemClock.date();
    expect(date).toBeInstanceOf(Date);
    expect(date.getTime()).toBe(Date.now());
  });

  it("constructs a Date from a numeric epoch ms argument", () => {
    const ms = new Date("2025-06-01T12:34:56.000Z").getTime();
    const date = systemClock.date(ms);
    expect(date).toBeInstanceOf(Date);
    expect(date.getTime()).toBe(ms);
  });

  it("constructs a Date from an ISO string argument", () => {
    const iso = "2025-06-01T12:34:56.000Z";
    const date = systemClock.date(iso);
    expect(date).toBeInstanceOf(Date);
    expect(date.toISOString()).toBe(iso);
  });

  it("returns an Invalid Date for an unparseable string (matches `new Date(string)` semantics)", () => {
    const date = systemClock.date("not-a-date");
    expect(date).toBeInstanceOf(Date);
    expect(Number.isNaN(date.getTime())).toBe(true);
  });
});

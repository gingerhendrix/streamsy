/**
 * The declaration is inert data with a stable identity.
 *
 * A sink whose fingerprint moved is a different public promise, so the parts a
 * consumer or an operator can observe — the relation key, the handler it is
 * pinned to, the retry budget — are exactly the parts the fingerprint covers.
 */
import { describe, expect, test } from "bun:test";
import {
  backoffAfter,
  defineActionSink,
  type ActionSinkDeliveryPolicy,
} from "@streamsy/serve/action";

interface Payload {
  readonly id: string;
  readonly partition: string;
}

const delivery: ActionSinkDeliveryPolicy = {
  maxAttempts: 3,
  initialBackoffMs: 100,
  backoffFactor: 2,
  maxBackoffMs: 1_000,
};

const define = (overrides: { readonly name?: string; readonly version?: number } = {}) =>
  defineActionSink<Payload, { readonly key: "id" }>({
    name: overrides.name ?? "test.sink",
    from: { key: "id" },
    handler: { name: "test.handler", version: overrides.version ?? 1 },
    payload: {
      encode: (value) => JSON.stringify(value),
      // SAFETY: this fixture's payloads are produced by this file alone, so the
      // stored value is always the `Payload` it was encoded from.
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion
      decode: (value) => value as Payload,
    },
    idempotencyKey: (value) => value.id,
    partitionBy: (value) => value.partition,
    delivery,
  });

describe("an action-sink declaration", () => {
  test("carries the observed relation's key and is frozen", () => {
    const sink = define();
    expect(sink.kind).toBe("checked-action-sink");
    expect(sink.key).toBe("id");
    expect(Object.isFrozen(sink)).toBe(true);
    expect(sink.fingerprint).toMatch(/^[0-9a-f]{8}$/);
  });

  test("the fingerprint moves when the handler version moves", () => {
    expect(define().fingerprint).toBe(define().fingerprint);
    expect(define({ version: 2 }).fingerprint).not.toBe(define().fingerprint);
    expect(define({ name: "test.other" }).fingerprint).not.toBe(define().fingerprint);
  });

  test("an impossible delivery policy is rejected at declaration time", () => {
    expect(() =>
      defineActionSink<Payload, { readonly key: "id" }>({
        ...define(),
        delivery: { ...delivery, maxAttempts: 0 },
      }),
    ).toThrow(/at least one delivery attempt/);
    expect(() =>
      defineActionSink<Payload, { readonly key: "id" }>({
        ...define(),
        delivery: { ...delivery, backoffFactor: 0.5 },
      }),
    ).toThrow(/shrink its backoff/);
    expect(() =>
      defineActionSink<Payload, { readonly key: "id" }>({
        ...define(),
        delivery: { ...delivery, maxBackoffMs: 10 },
      }),
    ).toThrow(/impossible backoff window/);
  });

  test("backoff grows exponentially and stays clamped", () => {
    expect([1, 2, 3, 4, 5].map((attempt) => backoffAfter(delivery, attempt))).toEqual([
      100, 200, 400, 800, 1_000,
    ]);
  });
});

test("infinite delivery backoff retains D8 behavior and has a distinct fingerprint", () => {
  const uncapped = defineActionSink({
    ...define(),
    delivery: { ...delivery, maxBackoffMs: Infinity },
  });
  const capped = defineActionSink({ ...define(), delivery: { ...delivery, maxBackoffMs: 30_000 } });
  expect(uncapped.delivery.maxBackoffMs).toBe(Infinity);
  expect(backoffAfter(uncapped.delivery, 20)).toBe(52_428_800);
  expect(uncapped.fingerprint).not.toBe(capped.fingerprint);
});

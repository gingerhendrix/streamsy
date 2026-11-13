import { expect, test } from "vitest";
import { Effect, Schema } from "effect";
import { Outbox, InvalidConfiguration, PayloadEncodingFailure } from "../src/index.ts";
import { definition, retry, withStore } from "./support.ts";

const key = (value: { id: string }) => value.id;

test("copies and freezes validated retry config without freezing application closures", () => {
  const policy = { ...retry };
  const frozen = Outbox.define({ ...definition, retry: policy, idempotencyKey: key });
  policy.maxAttempts = 999;
  expect(frozen.retry.maxAttempts).toBe(3);
  expect(Object.isFrozen(frozen)).toBe(true);
  expect(Object.isFrozen(frozen.retry)).toBe(true);
  expect(Object.isFrozen(key)).toBe(false);
});

for (const maxAttempts of [NaN, Infinity, -Infinity, 0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])
  test(`rejects attempt budget ${maxAttempts}`, () => {
    expect(() => Outbox.define({ ...definition, retry: { ...retry, maxAttempts } })).toThrow(
      InvalidConfiguration,
    );
  });

for (const field of ["initialDelayMs", "factor", "maxDelayMs"] as const)
  for (const value of [NaN, Infinity, -Infinity, -1])
    test(`rejects ${field}=${value}`, () => {
      expect(() => Outbox.define({ ...definition, retry: { ...retry, [field]: value } })).toThrow(
        InvalidConfiguration,
      );
    });

test("rejects a cap that is not a safe integer", () => {
  expect(() => Outbox.define({ ...definition, retry: { ...retry, maxDelayMs: 1_000.5 } })).toThrow(
    InvalidConfiguration,
  );
});

test("rejects a shrinking factor and impossible cap", () => {
  expect(() => Outbox.define({ ...definition, retry: { ...retry, factor: 0.5 } })).toThrow(
    InvalidConfiguration,
  );
  expect(() => Outbox.define({ ...definition, retry: { ...retry, maxDelayMs: 1 } })).toThrow(
    InvalidConfiguration,
  );
});

for (const mode of ["memory", "sqlite"] as const)
  test(`${mode}: Schema transformations encode JSON and deliver decoded types`, () =>
    withStore(mode, (store) =>
      Effect.gen(function* () {
        const transformed = Outbox.define({
          name: "transformed",
          payload: Schema.Struct({ id: Schema.String, count: Schema.FiniteFromString }),
          idempotencyKey: (value) => value.id,
          retry,
        });

        yield* Outbox.enqueue(transformed, [{ id: "a", count: 42 }]);
        const entries = yield* store.list({ name: transformed.name });
        expect(entries[0]?.encodedPayload).toBe('{"id":"a","count":"42"}');
        expect(entries[0]?.partitionId).toBe("");
        const received: number[] = [];
        yield* Outbox.drain(transformed, ({ payload }) =>
          Effect.sync(() => {
            received.push(payload.count);
          }),
        );
        expect(received).toEqual([42]);
      }),
    ));

for (const mode of ["memory", "sqlite"] as const) {
  test(`${mode}: notBefore holds ordered drains behind the future head`, () =>
    withStore(mode, (store) =>
      Effect.gen(function* () {
        const scheduled = Outbox.define({
          ...definition,
          notBefore: (value) => (value.id === "future" ? 1000 : 0),
        });
        yield* Outbox.enqueue(scheduled, [
          { id: "future", lane: "main" },
          { id: "now", lane: "main" },
        ]);
        expect(yield* Outbox.drain(scheduled, () => Effect.void, { ordered: true })).toMatchObject({
          attempted: 0,
        });
        expect(yield* Outbox.drain(scheduled, () => Effect.void)).toMatchObject({ delivered: 1 });
        expect((yield* store.list({ name: scheduled.name }))[0]?.nextAttemptAtMs).toBe(1000);
      }),
    ));
  for (const value of [
    NaN,
    Infinity,
    -Infinity,
    1.5,
    Number.MAX_SAFE_INTEGER + 1,
    "throw",
  ] as const) {
    test(`${mode}: invalid notBefore ${value} fails the whole batch before writes`, () =>
      withStore(mode, (store) =>
        Effect.gen(function* () {
          const scheduled = Outbox.define({
            ...definition,
            notBefore: (payload) => {
              if (payload.id === "valid") return 1000;
              if (value === "throw") throw new Error("bad time");
              return value;
            },
          });
          expect(
            yield* Outbox.enqueue(scheduled, [
              { id: "valid", lane: "main" },
              { id: "invalid", lane: "main" },
            ]).pipe(Effect.flip),
          ).toBeInstanceOf(PayloadEncodingFailure);
          expect(yield* store.list({ name: scheduled.name })).toEqual([]);
        }),
      ));
  }
}

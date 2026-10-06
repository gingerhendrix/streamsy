/* oxlint-disable effecttsgo/strict-effect-provide -- Tests assemble the complete memory host. */
import { expect, test } from "bun:test";
import { Effect, Schema } from "effect";
import { Streams } from "@streamsy/core/protocol";
import { Commit, Projection } from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";
import * as Family from "./support/families.ts";
import { readAll } from "./support/scenarios.ts";

const input = Family.json("step-input", { schema: Schema.Number });

const output = Family.json("step-output", { schema: Schema.Number });

const other = Family.json("step-other", { schema: Schema.Number });

const host = Family.withFamilies(memoryHost());

const outputs = {
  output: output,
  other: other,
};

test("mapAccum threads the value and preserves entry and output order", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(input, [1, 2, 3]);

      const projection = Projection.mapAccum({
        id: "ordered",
        params: {},
        inputs: { input },
        outputs,
        commit: Commit.producer,
        state: { schema: Schema.Number, initial: 0 },
        mapAccum: (previous, { item }) => [
          previous + item,
          { output: [previous, previous + item] },
        ],
      }).at({});

      yield* Projection.run(projection);
      expect(yield* readAll(output)).toEqual([0, 1, 1, 3, 3, 6]);
      expect((yield* Projection.read(projection)).state).toBe(6);
    }).pipe(Effect.provide(host)),
  ));

const emitLog = (split: boolean) =>
  Effect.gen(function* () {
    yield* Streams.create(input);
    yield* Streams.create(output);
    yield* Streams.create(other);

    const projection = Projection.emit({
      id: "split",
      params: {},
      inputs: { input },
      outputs,
      commit: Commit.producer,
      emit: ({ item }) =>
        item === 0 ? {} : item % 2 === 0 ? { output: [item, item * 10] } : { other: [item] },
    }).at({});

    yield* Streams.append(input, [0, 1, 2]);

    if (split) yield* Projection.run(projection);
    yield* Streams.append(input, [3, 4]);
    yield* Projection.run(projection);

    return { even: yield* readAll(output), odd: yield* readAll(other) };
  }).pipe(Effect.provide(host));

test("emit permits missing keys, chooses one of two outputs, and is independent of unit size", async () => {
  const whole = await Effect.runPromise(emitLog(false));
  const split = await Effect.runPromise(emitLog(true));
  expect(whole).toEqual({ even: [2, 20, 4, 40], odd: [1, 3] });
  expect(split).toEqual(whole);
});

test("undeclared output keys fail at the adapter boundary", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);

      const projection = Projection.emit({
        id: "invalid",
        params: {},
        inputs: { input },
        outputs,
        commit: Commit.producer,
        // @ts-expect-error exercise the runtime check for untyped callers
        emit: () => ({ output: [], unexpected: [1] }),
      }).at({});

      expect(yield* Projection.run(projection).pipe(Effect.result)).toMatchObject({
        _tag: "Failure",
        failure: { phase: "process", reason: "invalid-output" },
      });
    }).pipe(Effect.provide(host)),
  ));

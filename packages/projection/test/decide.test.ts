/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own the runtime boundary and complete host graph. */
import { expect, test } from "bun:test";
import { Effect, Schedule, Schema } from "effect";
import { OffsetMismatch, StreamId, Streamsy, MemoryStorage } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";

class Numbers extends Streamsy.json<Numbers>()("decide/Numbers", "decide/numbers", {
  params: {},
  schema: Schema.Finite,
}) {}

class Words extends Streamsy.json<Words>()("decide/Words", "decide/words", {
  params: {},
  schema: Schema.String,
}) {}

const host = Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Numbers, Words));

const run = <A, E>(effect: Effect.Effect<A, E, Numbers | Words | CheckpointStore>) =>
  Effect.runPromise(effect.pipe(Effect.provide(host), Effect.provide(CheckpointStore.layerMemory)));

const Totals = Schema.Struct({ sum: Schema.Finite, words: Schema.Finite });

const totals = Projection.fold({
  id: "decide-totals",
  params: {},
  inputs: { numbers: Numbers, words: Words },
  state: { schema: Totals, initial: { sum: 0, words: 0 } },
  fold: (state, entry) =>
    entry.input === "numbers"
      ? { ...state, sum: state.sum + entry.item }
      : { ...state, words: state.words + 1 },
}).at({});

const seed = Effect.gen(function* () {
  const numbers = yield* Numbers;
  const words = yield* Words;
  yield* numbers.create({});
  yield* words.create({});
  yield* numbers.append({}, [2, 3]);
  yield* words.append({}, ["a"]);
});

const tailOf = (stream: typeof Numbers.Service | typeof Words.Service) =>
  stream.head({}).pipe(Effect.map((head) => head.nextOffset));

test("decide appends to the named input with that input's offset", () =>
  run(
    Effect.gen(function* () {
      yield* seed;
      const numbers = yield* Numbers;
      const words = yield* Words;
      const expected: Array<string | undefined> = [];
      const wordsTail = yield* tailOf(words);
      const numbersTail = yield* tailOf(numbers);

      const decided = yield* Projection.decide(totals, {
        input: "words",
        decide: ({ state, offsets }) =>
          Effect.succeed({ value: offsets, items: [`sum ${state.sum}`] }),
      }).pipe(
        Effect.provideService(Words, {
          ...words,
          wire: {
            ...words.wire,
            append: (id, options) => {
              expected.push(options.expectedOffset);

              return words.wire.append(id, options);
            },
          },
        }),
      );

      expect(decided.value).toEqual({ numbers: numbersTail, words: wordsTail });
      expect(expected).toEqual([wordsTail]);
      expect(decided.appended).toBe(true);
      expect(decided.offset).toBe(yield* tailOf(words));
      expect((yield* words.collect({})).items).toEqual(["a", "sum 5"]);
      expect(yield* tailOf(numbers)).toBe(numbersTail);
    }),
  ));

test("decide with no items does not append", () =>
  run(
    Effect.gen(function* () {
      yield* seed;
      const words = yield* Words;
      const tail = yield* tailOf(words);

      const decided = yield* Projection.decide(totals, {
        input: "words",
        decide: ({ state }) => Effect.succeed({ value: state, items: [] }),
      });

      expect(decided).toEqual({ value: { sum: 5, words: 1 }, offset: tail, appended: false });
      expect(yield* tailOf(words)).toBe(tail);
    }),
  ));

test("decide runs, reads and decides again after a lost race", () =>
  run(
    Effect.gen(function* () {
      yield* seed;
      const numbers = yield* Numbers;
      const seen: Array<number> = [];

      const decided = yield* Projection.decide(totals, {
        input: "numbers",
        decide: ({ state }) =>
          Effect.gen(function* () {
            seen.push(state.sum);

            // The first decision loses to a rival write.
            if (seen.length === 1) yield* numbers.append({}, [10]);

            return { value: state.sum, items: [-state.sum] };
          }),
      });

      expect(seen).toEqual([5, 15]);
      expect(decided.value).toBe(15);
      expect((yield* numbers.collect({})).items).toEqual([2, 3, 10, -15]);
      expect((yield* Projection.read(totals)).state.sum).toBe(15);
    }),
  ));

test("decide fails with the last OffsetMismatch when the schedule ends", () =>
  run(
    Effect.gen(function* () {
      yield* seed;
      const numbers = yield* Numbers;
      let decisions = 0;

      const failure = yield* Effect.flip(
        Projection.decide(totals, {
          input: "numbers",
          decide: () =>
            Effect.gen(function* () {
              decisions += 1;
              yield* numbers.append({}, [1]);

              return { value: undefined, items: [0] };
            }),
          retry: Schedule.recurs(1),
        }),
      );

      expect(decisions).toBe(2);
      expect(failure).toBeInstanceOf(OffsetMismatch);
    }),
  ));

test("decide works for a projection with no state", () =>
  run(
    Effect.gen(function* () {
      yield* seed;
      const words = yield* Words;
      // The handler's own table stands in for SQL rows.
      const table: Array<string> = [];

      const indexed = Projection.process({
        id: "decide-words-table",
        params: {},
        inputs: { words: Words },
        commit: Commit.atLeastOnce,
        process: (batch) =>
          Effect.sync(() => {
            table.push(...batch.words.items);
          }),
      }).at({});

      const decided = yield* Projection.decide(indexed, {
        input: "words",
        decide: (snapshot) =>
          Effect.succeed({
            value: snapshot.state,
            items: table.includes("b") ? [] : ["b"],
          }),
      });

      expect(decided.value).toBeUndefined();
      expect(decided.appended).toBe(true);
      expect((yield* words.collect({})).items).toEqual(["a", "b"]);

      const again = yield* Projection.decide(indexed, {
        input: "words",
        decide: () =>
          Effect.succeed({ value: [...table], items: table.includes("b") ? [] : ["b"] }),
      });

      expect(again).toMatchObject({ value: ["a", "b"], appended: false });
    }),
  ));

test("decide does not retry an OffsetMismatch that its own decide raises", () =>
  run(
    Effect.gen(function* () {
      yield* seed;
      const numbers = yield* Numbers;
      const tail = yield* tailOf(numbers);

      const own = new OffsetMismatch({
        id: StreamId.make("elsewhere"),
        expected: "a",
        actual: "b",
      });

      let decisions = 0;

      const failure = yield* Effect.flip(
        Projection.decide(totals, {
          input: "numbers",
          decide: () => {
            decisions += 1;

            return Effect.fail(own);
          },
        }),
      );

      expect(failure).toBe(own);
      expect(decisions).toBe(1);
      expect(yield* tailOf(numbers)).toBe(tail);
    }),
  ));

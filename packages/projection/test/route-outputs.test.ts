import { expect, test } from "bun:test";
import { Effect, Schema, Stream } from "effect";
import { Rows, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Families from "./support/families.ts";

class Input extends Streamsy.json<Input>()("multi/Input", "multi/input/:id", {
  params: { id: Schema.String },
  schema: Schema.Finite,
}) {}
class Board extends Streamsy.state<Board>()("multi/Board", "multi/board/:id", {
  params: { id: Schema.String },
  collections: {
    card: { schema: Schema.Struct({ id: Schema.String, title: Schema.String }), key: "id" },
    count: {
      schema: Schema.Struct({ number: Schema.Finite, total: Schema.FiniteFromString }),
      key: "number",
    },
  },
}) {}
for (const commit of [Commit.transaction, Commit.producer]) {
  test(`${commit.name}: one output routes ordered changes to two collections`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const params = { id: "one" };
        const input = yield* Input;
        const board = yield* Board;
        yield* input.create(params);
        yield* board.create(params);
        yield* input.append(params, [2]);
        const declaration = Projection.emit({
          id: "multi",
          params: { id: Schema.String },
          inputs: { input: Input },
          outputs: { renamed: Board },
          commit,
          emit: ({ item }) => ({
            renamed: [
              Rows.upsert("card", { id: "a", title: "A" }),
              Rows.upsert("count", { number: 7, total: item }),
              Rows.remove("card", "old"),
              Rows.remove("count", 8),
            ],
          }),
        });
        expect("outputs" in declaration).toBe(false);
        expect("outputs" in declaration.at(params)).toBe(false);
        yield* Projection.run(declaration.at(params));
        yield* Projection.run(declaration.at(params));
        const batches = yield* Stream.runCollect(board.read(params));
        expect(batches.flatMap((batch) => batch.items)).toEqual([
          {
            type: "card",
            key: "a",
            value: { id: "a", title: "A" },
            headers: { operation: "upsert" },
          },
          {
            type: "count",
            key: "7",
            value: { number: 7, total: 2 },
            headers: { operation: "upsert" },
          },
          { type: "card", key: "old", headers: { operation: "delete" } },
          { type: "count", key: "8", headers: { operation: "delete" } },
        ]);
      }).pipe(Effect.provide(Families.withFamilies(Memory.layerMemory(), Input, Board))),
    ));
}

test("an invalid runtime collection fails without advancing the checkpoint", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const params = { id: "bad" };
      const input = yield* Input;
      yield* input.create(params);
      yield* input.append(params, [1]);
      const projection = Projection.emit({
        id: "invalid-collection",
        params: {},
        inputs: { input: Input.at(params) },
        outputs: { board: Board.at(params) },
        commit: Commit.producer,
        // @ts-expect-error runtime callers can bypass the required collection name
        emit: () => ({ board: [Rows.remove("a")] }),
      }).at({});
      const result = yield* Projection.run(projection).pipe(Effect.result);
      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { phase: "process", reason: "invalid-output" },
      });
      expect((yield* Projection.read(projection)).offsets.input).toBe(ZERO_OFFSET);
    }).pipe(Effect.provide(Families.withFamilies(Memory.layerMemory(), Input, Board))),
  ));

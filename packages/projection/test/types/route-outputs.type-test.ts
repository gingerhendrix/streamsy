import { Schema } from "effect";
import { DurableState, Streamsy } from "@streamsy/core";
import { Commit, Projection, type Emits } from "@streamsy/projection";

class Numbers extends Streamsy.json<Numbers>()("routes/Numbers", "numbers/:id", {
  params: { id: Schema.String },
  schema: Schema.Finite,
}) {}

class Board extends Streamsy.state<Board>()("routes/Board", "board/:id", {
  params: { id: Schema.String },
  collections: {
    card: { schema: Schema.Struct({ id: Schema.String, title: Schema.String }), key: "id" },
  },
}) {}

class Multi extends Streamsy.state<Multi>()("routes/Multi", "multi/:id", {
  params: { id: Schema.String },
  collections: {
    card: Board.collections.card,
    count: { schema: Schema.Struct({ id: Schema.String, n: Schema.Finite }), key: "id" },
  },
}) {}

const outputs = { numbers: Numbers, board: Board, multi: Multi };

export const valid: Emits<typeof outputs> = {
  numbers: [1],
  board: [DurableState.upsert("card", { id: "a", title: "A" }), DurableState.remove("card", "a")],
  multi: [DurableState.upsert("count", { id: "a", n: 2 }), DurableState.remove("card", "a")],
};

export const empty: Emits<typeof outputs> = {};

// @ts-expect-error JSON routes infer their item type
export const wrongJson: Emits<typeof outputs> = { numbers: ["wrong"] };

export const wrongRow: Emits<typeof outputs> = {
  // @ts-expect-error a row retains its schema type
  board: [DurableState.upsert("card", { id: "a", title: 2 })],
};

export const unknownCollection: Emits<typeof outputs> = {
  // @ts-expect-error changes must name a declared collection
  board: [DurableState.upsert("count", { id: "a", n: 2 })],
};

export const mixed: Emits<typeof outputs> = {
  // @ts-expect-error collection and row types remain correlated
  multi: [DurableState.upsert("card", { id: "a", n: 2 })],
};

export const wrongOld: Emits<typeof outputs> = {
  // @ts-expect-error a delete's old value retains its schema type
  multi: [DurableState.remove("count", "a", { oldValue: { id: "a", title: "A" } })],
};

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Assert<T extends true> = T;

export type Checks = [
  Assert<Same<NonNullable<Emits<typeof outputs>["numbers"]>[number], number>>,
  Assert<
    Same<
      NonNullable<Emits<typeof outputs>["multi"]>[number],
      DurableState.Change<typeof Multi.collections>
    >
  >,
];

export const fixed = Projection.emit({
  id: "fixed",
  params: {},
  inputs: {},
  outputs: { board: Board.at({ id: "a" }) },
  commit: Commit.producer,
  emit: () => ({ board: [DurableState.upsert("card", { id: "a", title: "A" })] }),
});

Projection.emit({
  id: "missing-param",
  params: {},
  inputs: {},
  // @ts-expect-error route params must be present in projection params
  outputs: { board: Board },
  commit: Commit.producer,
  emit: () => ({}),
});

Projection.emit({
  id: "wrong-param",
  params: { id: Schema.FiniteFromString },
  inputs: {},
  // @ts-expect-error decoded route params must match projection params
  outputs: { board: Board },
  commit: Commit.producer,
  emit: () => ({}),
});

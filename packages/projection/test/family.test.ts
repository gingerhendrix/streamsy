import { expect, test } from "bun:test";
import { Effect, Option, Schema } from "effect";
import { StreamRoute } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { recordKey } from "@streamsy/projection/checkpoint";
import { producerId } from "../src/stream-output.ts";

const facts = StreamRoute.json("facts/:workspaceId", {
  params: { workspaceId: Schema.String },
  schema: Schema.Finite,
});
const config = StreamRoute.json("config", {
  params: {},
  schema: Schema.String,
});
const output = StreamRoute.json("boards/:workspaceId", {
  params: { workspaceId: Schema.String },
  schema: Schema.Finite,
});

test("member is the ordinary routed projection", () => {
  const board = Projection.family({
    id: "board",
    params: { workspaceId: Schema.String },
    inputs: { facts, config },
    output,
    process: (batch) => Effect.succeed(batch.facts.items),
  });
  const member = board.member({ workspaceId: "ws-42" });
  const direct = Projection.stream({
    id: "board",
    params: { workspaceId: "ws-42" },
    inputs: {
      facts: facts.ref({ workspaceId: "ws-42" }),
      config: config.ref({}),
    },
    output: output.ref({ workspaceId: "ws-42" }),
    process: (batch) => Effect.succeed(batch.facts.items),
  });

  expect(member.params).toEqual(direct.params);
  expect(Projection.key(member)).toEqual(Projection.key(direct));
  expect(recordKey(Projection.key(member))).toBe(recordKey(Projection.key(direct)));
  expect(producerId(member.id, member.version, member.params)).toBe(
    producerId(direct.id, direct.version, direct.params),
  );
  expect(member.inputs.facts.id).toBe(direct.inputs.facts.id);
  expect(member.output.id).toBe(direct.output.id);
  expect(Option.getOrThrow(board.parse("facts/ws-42"))).toEqual({
    workspaceId: "ws-42",
  });
  expect(Option.getOrThrow(board.parse("config"))).toEqual({});
  expect(Option.isNone(board.parse("elsewhere/ws-42"))).toBe(true);
  expect(Option.isNone(board.parse("boards/ws-42"))).toBe(true);
});

test("number parameters encode to strings and parse back", () => {
  const numbered = StreamRoute.json("numbered/:id", {
    params: { id: Schema.FiniteFromString },
    schema: Schema.String,
  });
  const members = Projection.family({
    id: "numbered",
    params: { id: Schema.FiniteFromString },
    inputs: { numbered },
    process: () => Effect.void,
  });
  expect(members.member({ id: 42 }).params).toEqual({ id: "42" });
  expect(Option.getOrThrow(members.parse("numbered/42"))).toEqual({ id: 42 });
  expect(() => members.member({ id: Number.NaN })).toThrow(
    "Cannot encode family numbered parameter id",
  );
});

const stringId = StreamRoute.json("string-id/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
});
Projection.family({
  id: "incompatible-codec",
  params: { id: Schema.FiniteFromString },
  // @ts-expect-error the route's decoded parameter must be assignable to the family parameter
  inputs: { stringId },
  process: () => Effect.void,
});

const literalId = StreamRoute.json("literal-id/:id", {
  params: { id: Schema.Literal("a") },
  schema: Schema.String,
});
Projection.family({
  id: "narrow-route-codec",
  params: { id: Schema.String },
  // @ts-expect-error the family parameter must also be assignable to the route's decoded parameter
  inputs: { literalId },
  process: () => Effect.void,
});

const wrong = StreamRoute.json("wrong/:projectId", {
  params: { projectId: Schema.String },
  schema: Schema.String,
});
Projection.family({
  id: "invalid",
  params: { workspaceId: Schema.String },
  // @ts-expect-error a route cannot require a parameter absent from the family
  inputs: { wrong },
  process: () => Effect.void,
});

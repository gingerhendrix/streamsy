import { Streamsy } from "@streamsy/core";
import { expect, test } from "bun:test";
import { Effect, Option, Schema } from "effect";
import { Output, Projection } from "@streamsy/projection";
import { recordKey } from "@streamsy/projection/checkpoint";
import { emptySlice } from "../src/batch.ts";
import { producerId } from "../src/stream-output.ts";
import { unitOf } from "../src/unit.ts";

class facts extends Streamsy.json<facts>()("declaration.test/facts", "facts/:workspaceId", {
  params: { workspaceId: Schema.String },
  schema: Schema.Finite,
}) {}
class config extends Streamsy.json<config>()("declaration.test/config", "config", {
  params: {},
  schema: Schema.String,
}) {}
class output extends Streamsy.json<output>()("declaration.test/output", "boards/:workspaceId", {
  params: { workspaceId: Schema.String },
  schema: Schema.Finite,
}) {}

test("a declaration binds declared inputs and keeps fixed addresses", () => {
  const board = Projection.outputs({
    id: "board",
    params: { workspaceId: Schema.String },
    inputs: { facts, config: config.at({}) },
    outputs: { output: Output.stream(Schema.Finite, { stream: output }) },
    process: (batch) => Effect.succeed({ output: batch.facts.items }),
  });
  const bound = board.at({ workspaceId: "ws-42" });

  expect(bound._tag).toBe("Outputs");
  expect(bound.params).toEqual({ workspaceId: "ws-42" });
  expect(bound.inputs.facts.id).toBe(facts.at({ workspaceId: "ws-42" }).id);
  expect(bound.inputs.facts.stream.key).toBe(facts.key);
  expect(bound.inputs.config.id).toBe(config.at({}).id);
  expect(bound.streams.output?.id).toBe(output.at({ workspaceId: "ws-42" }).id);
  expect(board.at({ workspaceId: "ws-7" }).inputs.config).toBe(bound.inputs.config);
  expect(Option.getOrThrow(board.parse("facts/ws-42"))).toEqual({
    workspaceId: "ws-42",
  });
  expect(Option.isNone(board.parse("config"))).toBe(true);
  expect(Option.isNone(board.parse("elsewhere/ws-42"))).toBe(true);
  expect(Option.isNone(board.parse("boards/ws-42"))).toBe(true);
});

// Golden strings recorded at febad922, before declarations replaced families.
test("record keys and producer ids match the values before declarations", () => {
  const fixed = Projection.atomic({
    id: "hex-board",
    params: {},
    inputs: { config },
    process: () => Effect.void,
  }).at({});
  const coded = Projection.atomic({
    id: "hex-board",
    params: { gameId: Schema.String },
    inputs: { config },
    process: () => Effect.void,
  }).at({ gameId: "game" });
  const versioned = Projection.atomic({
    id: "issue-tracker",
    version: 2,
    generation: 3,
    params: { workspaceId: Schema.String, b: Schema.String },
    inputs: { config },
    process: () => Effect.void,
  }).at({ workspaceId: "ws-42", b: "x" });

  expect(recordKey(Projection.key(fixed))).toBe('["streamsy.projection.v1","hex-board","1","{}"]');
  expect(producerId(fixed.id, fixed.version, fixed.params)).toBe("hex-board/v1");
  expect(recordKey(Projection.key(coded))).toBe(
    '["streamsy.projection.v1","hex-board","1","{\\"gameId\\":\\"game\\"}"]',
  );
  expect(producerId(coded.id, coded.version, coded.params)).toBe('hex-board/v1/{"gameId":"game"}');
  expect(recordKey(Projection.key(versioned))).toBe(
    '["streamsy.projection.v1","issue-tracker","2","3","{\\"b\\":\\"x\\",\\"workspaceId\\":\\"ws-42\\"}"]',
  );
  expect(producerId(versioned.id, versioned.version, versioned.params)).toBe(
    'issue-tracker/v2/{"b":"x","workspaceId":"ws-42"}',
  );
});

test("the handler unit carries decoded params and the unit key stays encoded", async () => {
  class numbered extends Streamsy.json<numbered>()("declaration.test/units", "units/:id", {
    params: { id: Schema.FiniteFromString },
    schema: Schema.String,
  }) {}
  const seen: Array<{ readonly id: number; readonly key: string }> = [];
  const declared = Projection.atomic({
    id: "units",
    params: { id: Schema.FiniteFromString },
    inputs: { numbered },
    process: (_batch, unit) => Effect.sync(() => seen.push({ id: unit.params.id, key: unit.key })),
  });
  const bound = declared.at({ id: 7 });
  const kernelUnit = unitOf("units", 1, 1, bound.params, {});
  await Effect.runPromise(bound.process({ numbered: emptySlice("0") }, kernelUnit));
  expect(seen).toEqual([{ id: 7, key: kernelUnit.key }]);
  expect(kernelUnit.params).toEqual({ id: "7" });
});

test("number parameters encode to strings and parse back", () => {
  class numbered extends Streamsy.json<numbered>()("declaration.test/numbered", "numbered/:id", {
    params: { id: Schema.FiniteFromString },
    schema: Schema.String,
  }) {}
  const declared = Projection.atomic({
    id: "numbered",
    params: { id: Schema.FiniteFromString },
    inputs: { numbered },
    process: () => Effect.void,
  });
  expect(declared.at({ id: 42 }).params).toEqual({ id: "42" });
  expect(Option.getOrThrow(declared.parse("numbered/42"))).toEqual({ id: 42 });
  expect(() => declared.at({ id: Number.NaN })).toThrow(
    "Cannot encode projection numbered parameter id",
  );
});

class stringId extends Streamsy.json<stringId>()("declaration.test/stringId", "string-id/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}
Projection.atomic({
  id: "incompatible-codec",
  params: { id: Schema.FiniteFromString },
  // @ts-expect-error the stream's decoded parameter must be assignable to the projection parameter
  inputs: { stringId },
  process: () => Effect.void,
});

class literalId extends Streamsy.json<literalId>()("declaration.test/literalId", "literal-id/:id", {
  params: { id: Schema.Literal("a") },
  schema: Schema.String,
}) {}
Projection.atomic({
  id: "narrow-route-codec",
  params: { id: Schema.String },
  // @ts-expect-error the projection parameter must also be assignable to the stream's decoded parameter
  inputs: { literalId },
  process: () => Effect.void,
});

class wrong extends Streamsy.json<wrong>()("declaration.test/wrong", "wrong/:projectId", {
  params: { projectId: Schema.String },
  schema: Schema.String,
}) {}
Projection.atomic({
  id: "invalid",
  params: { workspaceId: Schema.String },
  // @ts-expect-error a stream cannot require a parameter absent from the projection
  inputs: { wrong },
  process: () => Effect.void,
});

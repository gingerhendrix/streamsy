import { Commit } from "@streamsy/projection";
import { Streamsy } from "@streamsy/core";
import { expect, test } from "vitest";
import { it as baseIt } from "@effect/vitest";
import { Effect, Layer, Option, Schema } from "effect";
import { Projection } from "@streamsy/projection";
import { recordKey } from "@streamsy/projection/checkpoint";
import { producerId } from "../src/commit/producer.ts";
import { Streams } from "@streamsy/core/protocol";
import * as FamilyFixture from "./support/families.ts";
import { memoryHost } from "./support/memory-host.ts";

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
  const board = Projection.emit({
    commit: Commit.producer,
    id: "board",
    params: { workspaceId: Schema.String },
    inputs: { facts, config: config.at({}) },
    outputs: { output: output },
    emit: (entry) => Effect.succeed({ output: entry.input === "facts" ? [entry.item] : [] }),
  });

  const bound = board.at({ workspaceId: "ws-42" });

  expect(bound.commit).toBe(Commit.producer);
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
  const fixed = Projection.process({
    commit: Commit.transaction,
    id: "hex-board",
    params: {},
    inputs: { config },
    process: () => Effect.void,
  }).at({});

  const coded = Projection.process({
    commit: Commit.transaction,
    id: "hex-board",
    params: { gameId: Schema.String },
    inputs: { config },
    process: () => Effect.void,
  }).at({ gameId: "game" });

  const versioned = Projection.process({
    commit: Commit.transaction,
    id: "issue-tracker",
    version: 2,
    generation: 3,
    params: { workspaceId: Schema.String, b: Schema.String },
    inputs: { config },
    process: () => Effect.void,
  }).at({ workspaceId: "ws-42", b: "x" });

  expect(recordKey(Projection.key(fixed))).toBe(
    '["streamsy.projection.v1","hex-board","1","1","{}"]',
  );
  expect(producerId(fixed.id, fixed.version, fixed.params)).toBe("hex-board/v1");
  expect(recordKey(Projection.key(coded))).toBe(
    '["streamsy.projection.v1","hex-board","1","1","{\\"gameId\\":\\"game\\"}"]',
  );
  expect(producerId(coded.id, coded.version, coded.params)).toBe('hex-board/v1/{"gameId":"game"}');
  expect(recordKey(Projection.key(versioned))).toBe(
    '["streamsy.projection.v1","issue-tracker","2","3","{\\"b\\":\\"x\\",\\"workspaceId\\":\\"ws-42\\"}"]',
  );
  expect(producerId(versioned.id, versioned.version, versioned.params)).toBe(
    'issue-tracker/v2/{"b":"x","workspaceId":"ws-42"}',
  );
});

baseIt.effect("the handler unit carries decoded params and the unit key stays encoded", () =>
  Effect.gen(function* () {
    class numbered extends Streamsy.json<numbered>()("declaration.test/units", "units/:id", {
      params: { id: Schema.FiniteFromString },
      schema: Schema.String,
    }) {}

    const seen: Array<{ readonly id: number; readonly key: string }> = [];

    const declared = Projection.process({
      commit: Commit.atLeastOnce,
      id: "units",
      params: { id: Schema.FiniteFromString },
      inputs: { numbered },
      process: (_batch, unit) =>
        Effect.sync(() => seen.push({ id: unit.params.id, key: unit.key })),
    });

    const bound = declared.at({ id: 7 });
    const host = FamilyFixture.withFamilies(memoryHost(), numbered);

    const run = Effect.gen(function* () {
      yield* Streams.create(bound.inputs.numbered);
      yield* Streams.append(bound.inputs.numbered, ["a"]);
      yield* Projection.run(bound);
    });

    const services = yield* Layer.build(host);
    yield* run.pipe(Effect.provideContext(services));
    expect(seen.map(({ id }) => id)).toEqual([7]);
    // The key is a list of encoded parts; its params part holds the string "7".
    expect(
      yield* Schema.decodeEffect(Schema.Array(Schema.String).pipe(Schema.fromJsonString))(
        seen[0]?.key ?? "[]",
      ),
    ).toContain(
      yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Struct({ id: Schema.String })))({
        id: "7",
      }),
    );
  }),
);

test("number parameters encode to strings and parse back", () => {
  class numbered extends Streamsy.json<numbered>()("declaration.test/numbered", "numbered/:id", {
    params: { id: Schema.FiniteFromString },
    schema: Schema.String,
  }) {}

  const declared = Projection.process({
    commit: Commit.transaction,
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

Projection.process({
  commit: Commit.transaction,
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

Projection.process({
  commit: Commit.transaction,
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

Projection.process({
  commit: Commit.transaction,
  id: "invalid",
  params: { workspaceId: Schema.String },
  // @ts-expect-error a stream cannot require a parameter absent from the projection
  inputs: { wrong },
  process: () => Effect.void,
});

import { it, expect } from "@effect/vitest";
import { Effect, Option } from "effect";
import { memoryProjectionMarks } from "../../../src/internal/index.ts";
import { projectionMarksContract } from "../../../src/testing/projection-marks-contract.ts";

it.effect("standalone memory marks implement the failure contract", () =>
  projectionMarksContract(memoryProjectionMarks()),
);

it.effect("standalone stores are private and operations run when executed", () =>
  Effect.gen(function* () {
    const first = memoryProjectionMarks();
    const second = memoryProjectionMarks();
    const mark = first.mark("p", ["a"]);
    expect(yield* first.nextDue("p")).toEqual(Option.none());
    yield* mark;
    expect(yield* first.nextDue("p")).toEqual(Option.some(0));
    expect(yield* second.nextDue("p")).toEqual(Option.none());
  }),
);

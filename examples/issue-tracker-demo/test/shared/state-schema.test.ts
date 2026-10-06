import { describe, expect, test } from "bun:test";
import { Result } from "effect";
import { decodeStateEvents, issueTrackerState, type JsonValue } from "../../shared/state-schema.ts";

const project = {
  id: "proj_1",
  name: "Streamsy",
  description: "",
  createdAt: "2026-08-22T00:00:00.000Z",
};

const upsert = issueTrackerState.projects.upsert({
  value: project,
  headers: { timestamp: project.createdAt, txid: "00000000-0000-4000-8000-000000000001" },
});

const isStateEvent = (value: JsonValue | undefined) => Result.isSuccess(decodeStateEvents([value]));

describe("decodeStateEvents", () => {
  test("accepts state writer output with JSON extension fields on events, rows and headers", () => {
    expect(
      isStateEvent({
        ...upsert,
        extension: "event metadata",
        value: { ...project, extension: "entity metadata" },
        headers: { ...upsert.headers, extension: "header metadata" },
      }),
    ).toBe(true);
  });
});

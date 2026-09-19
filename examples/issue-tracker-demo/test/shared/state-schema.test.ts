import { describe, expect, test } from "bun:test";
import { issueTrackerState, stateEventSchema } from "../../shared/state-schema.ts";

const project = {
  id: "proj_1",
  name: "Streamsy",
  description: "",
  createdAt: "2026-08-22T00:00:00.000Z",
};

const upsert = issueTrackerState.projects.upsert({
  value: project,
  headers: { timestamp: project.createdAt, txid: crypto.randomUUID() },
});

describe("isStateEvent", () => {
  test("accepts an event built by the state schema", () => {
    expect(stateEventSchema.safeParse(upsert).success).toBe(true);
  });

  test("accepts a hand-written change event appended straight to the stream", () => {
    expect(
      stateEventSchema.safeParse({
        type: "project",
        key: project.id,
        value: project,
        headers: { operation: "upsert", txid: crypto.randomUUID(), timestamp: project.createdAt },
      }).success,
    ).toBe(true);
  });

  test("preserves JSON extension fields while decoding a public stream event", () => {
    const event = {
      ...upsert,
      extension: "event metadata",
      value: { ...project, extension: "entity metadata" },
      headers: { ...upsert.headers, extension: "header metadata" },
    };

    expect(stateEventSchema.parse(event)).toEqual(event);
  });

  test.each([[undefined], [null], ["text"], [42], [[]]])(
    "rejects the non-object payload %p",
    (value) => {
      expect(stateEventSchema.safeParse(value).success).toBe(false);
    },
  );

  test("rejects an unknown entity type", () => {
    expect(stateEventSchema.safeParse({ ...upsert, type: "invoice" }).success).toBe(false);
  });

  test("rejects a missing key", () => {
    expect(stateEventSchema.safeParse({ ...upsert, key: undefined }).success).toBe(false);
  });

  test("rejects an unknown operation", () => {
    expect(stateEventSchema.safeParse({ ...upsert, headers: { operation: "merge" } }).success).toBe(
      false,
    );
  });

  test("rejects non-string header metadata", () => {
    expect(
      stateEventSchema.safeParse({ ...upsert, headers: { operation: "upsert", txid: 7 } }).success,
    ).toBe(false);
  });

  test("rejects a value that does not match its entity schema", () => {
    expect(stateEventSchema.safeParse({ ...upsert, value: { ...project, name: 7 } }).success).toBe(
      false,
    );
  });

  test("rejects a value that belongs to another entity type", () => {
    expect(stateEventSchema.safeParse({ ...upsert, type: "issue" }).success).toBe(false);
  });

  test("rejects a malformed old_value", () => {
    expect(stateEventSchema.safeParse({ ...upsert, old_value: { id: "proj_1" } }).success).toBe(
      false,
    );
  });
});

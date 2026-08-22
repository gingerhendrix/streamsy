import { describe, expect, test } from "bun:test";
import { isStateEvent, issueTrackerState } from "./state-schema.ts";

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
    expect(isStateEvent(upsert)).toBe(true);
  });

  test("accepts a hand-written change event appended straight to the stream", () => {
    expect(
      isStateEvent({
        type: "project",
        key: project.id,
        value: project,
        headers: { operation: "upsert", txid: crypto.randomUUID(), timestamp: project.createdAt },
      }),
    ).toBe(true);
  });

  test("accepts an update event carrying old_value", () => {
    expect(
      isStateEvent({
        type: "project",
        key: project.id,
        value: project,
        old_value: { ...project, name: "Old" },
        headers: { operation: "update" },
      }),
    ).toBe(true);
  });

  test.each([[undefined], [null], ["text"], [42], [[]]])(
    "rejects the non-object payload %p",
    (value) => {
      expect(isStateEvent(value)).toBe(false);
    },
  );

  test("rejects an unknown entity type", () => {
    expect(isStateEvent({ ...upsert, type: "invoice" })).toBe(false);
  });

  test("rejects a missing key", () => {
    expect(isStateEvent({ ...upsert, key: undefined })).toBe(false);
  });

  test("rejects an unknown operation", () => {
    expect(isStateEvent({ ...upsert, headers: { operation: "merge" } })).toBe(false);
  });

  test("rejects non-string header metadata", () => {
    expect(isStateEvent({ ...upsert, headers: { operation: "upsert", txid: 7 } })).toBe(false);
  });

  test("rejects a value that does not match its entity schema", () => {
    expect(isStateEvent({ ...upsert, value: { ...project, name: 7 } })).toBe(false);
  });

  test("rejects a value that belongs to another entity type", () => {
    expect(isStateEvent({ ...upsert, type: "issue" })).toBe(false);
  });

  test("rejects a malformed old_value", () => {
    expect(isStateEvent({ ...upsert, old_value: { id: "proj_1" } })).toBe(false);
  });
});

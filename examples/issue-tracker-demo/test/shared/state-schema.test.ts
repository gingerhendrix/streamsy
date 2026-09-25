import { describe, expect, test } from "bun:test";
import { Result } from "effect";
import {
  decodeStateEvents,
  issueTrackerState,
  type Issue,
  type JsonValue,
} from "../../shared/state-schema.ts";

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

const isStateEvent = (value: JsonValue | undefined) => Result.isSuccess(decodeStateEvents([value]));
const validateIssue = (value: JsonValue) =>
  issueTrackerState.issues.schema["~standard"].validate(value);

describe("decodeStateEvents", () => {
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

  test("accepts JSON extension fields on events, rows and headers", () => {
    expect(
      isStateEvent({
        ...upsert,
        extension: "event metadata",
        value: { ...project, extension: "entity metadata" },
        headers: { ...upsert.headers, extension: "header metadata" },
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

describe("issueTrackerState", () => {
  const issue: Issue = {
    id: "issue_1",
    projectId: project.id,
    title: "Ship",
    status: "open",
    createdAt: project.createdAt,
    updatedAt: project.createdAt,
  };

  test("checks rows with the Effect entity schemas", () => {
    expect(validateIssue(issue)).toEqual({ value: issue });
    expect(validateIssue({ ...issue, status: "archived" })).toHaveProperty("issues");
  });

  test("drops row fields that the entity schema does not declare", () => {
    expect(validateIssue({ ...issue, extension: "row metadata" })).toEqual({ value: issue });
  });
});

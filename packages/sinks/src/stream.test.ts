/* oxlint-disable anti-slop/no-unknown-parameters -- The test codec exercises the sink's external wire boundary. */
import { describe, expect, test } from "bun:test";
import { defineStreamSink, STREAM_SINK_ERROR_TAGS } from "./stream.ts";

interface Transition {
  readonly issueId: string;
  readonly status: string;
}

const changesOfIssues = {
  kind: "change-stream",
  name: "test.issues.changes",
  key: "issueId",
  order: "arrival",
} as const;

const sink = defineStreamSink({
  name: "test.issue-transitions",
  from: changesOfIssues,
  event: {
    decode: (value: unknown): Transition => {
      if (!(value instanceof Object) || !("issueId" in value)) throw new Error("no issueId");
      return { issueId: String(value.issueId), status: "todo" };
    },
  },
  route: "/feed/:workspaceId/issue-transitions",
  params: { workspaceId: { decode: (value: string) => value } },
  feed: { name: "issue-transitions", type: "issue-transition" },
  protocol: {
    sessionVersion: 1,
    transport: "durable-stream",
    resume: true,
    order: "arrival",
    fallback: "replay-from-start",
  },
  errors: STREAM_SINK_ERROR_TAGS,
});

describe("defineStreamSink", () => {
  test("carries the change stream's key rather than restating it", () => {
    expect(sink.key).toBe("issueId");
    expect(sink.feed).toEqual({
      name: "issue-transitions",
      type: "issue-transition",
      subjectKey: "issueId",
    });
    expect(sink.kind).toBe("checked-stream-sink");
    expect(Object.isFrozen(sink)).toBe(true);
  });

  test("the fingerprint covers the route, feed, source, protocol and errors", () => {
    expect(sink.fingerprint).toMatch(/^[0-9a-f]{8}$/);
    const renamedFeed = defineStreamSink({
      name: "test.issue-transitions",
      from: changesOfIssues,
      event: sink.event,
      route: "/feed/:workspaceId/issue-transitions",
      params: { workspaceId: { decode: (value: string) => value } },
      feed: { name: "activity", type: "issue-transition" },
      protocol: {
        sessionVersion: 1,
        transport: "durable-stream",
        resume: true,
        order: "arrival",
        fallback: "replay-from-start",
      },
      errors: STREAM_SINK_ERROR_TAGS,
    });
    expect(renamedFeed.fingerprint).not.toBe(sink.fingerprint);
  });

  test("the compiled route builds and matches the declared path", () => {
    expect(sink.compiledRoute.build({ workspaceId: "main" })).toBe("/feed/main/issue-transitions");
    expect(sink.compiledRoute.match("/feed/main/issue-transitions")).toEqual({
      kind: "matched",
      params: { workspaceId: "main" },
    });
    expect(sink.compiledRoute.match("/feed/main/other")).toEqual({ kind: "mismatch" });
  });

  test("arrival order is declared, not configurable", () => {
    expect(sink.protocol.order).toBe("arrival");
    expect(sink.from.order).toBe("arrival");
  });
});

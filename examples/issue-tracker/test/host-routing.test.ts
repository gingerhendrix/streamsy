/**
 * Partition ownership, as a pure decision.
 *
 * Resolution is what makes the keyed host safe: it decides which workspace's
 * runtime a request may reach *before* any application code runs. These
 * assertions are on the rule itself, with no server, no storage and no
 * partition — if the rule is right here, isolation is a property of the host
 * rather than of the application's own filtering.
 */
import { describe, expect, test } from "bun:test";
import { boardIssues, issueTransitions, workspaceSummary } from "../domain/declaration.ts";
import { resolveRoute } from "../server/host-routing.ts";

describe("route resolution", () => {
  test("host routes are answered without a partition", () => {
    expect(resolveRoute("/health")).toEqual({ kind: "host", route: "health" });
    expect(resolveRoute("/host/metrics")).toEqual({ kind: "host", route: "metrics" });
  });

  test("every command path names the workspace that owns it", () => {
    expect(resolveRoute("/api/workspaces/left/issues")).toEqual({
      kind: "workspace",
      workspaceId: "left",
      target: "application",
    });
    expect(resolveRoute("/api/workspaces/right/issues/issue-1/status")).toEqual({
      kind: "workspace",
      workspaceId: "right",
      target: "application",
    });
    expect(resolveRoute("/api/workspaces/right/notifications/drain")).toEqual({
      kind: "workspace",
      workspaceId: "right",
      target: "application",
    });
  });

  test("each checked sink route resolves through its own declaration", () => {
    for (const [route, workspaceId] of [
      [boardIssues.compiledRoute.build({ workspaceId: "alpha" }), "alpha"],
      [issueTransitions.compiledRoute.build({ workspaceId: "beta" }), "beta"],
      [workspaceSummary.compiledRoute.build({ workspaceId: "gamma" }), "gamma"],
    ] as const) {
      expect(resolveRoute(route)).toEqual({
        kind: "workspace",
        workspaceId,
        target: "application",
      });
    }
  });

  test("a stream path resolves to the workspace named in the stream id", () => {
    expect(resolveRoute("/streams/workspaces/left/issue-events")).toEqual({
      kind: "workspace",
      workspaceId: "left",
      target: "streams",
    });
    expect(resolveRoute("/streams/state/workspaces/left/issues")).toEqual({
      kind: "workspace",
      workspaceId: "left",
      target: "streams",
    });
  });

  test("a workspace id the domain refuses is a typed refusal, not a partition", () => {
    const resolved = resolveRoute("/api/workspaces/..%2Fescape/issues");
    expect(resolved.kind).toBe("failure");
    if (resolved.kind !== "failure") throw new Error("expected a failure");
    expect(resolved.failure._tag).toBe("InvalidWorkspaceId");
  });

  test("a matched sink route with an undecodable parameter is the sink's own answer", () => {
    expect(resolveRoute("/state/workspaces/NO!/issues")).toEqual({ kind: "sink-params" });
  });

  test("an application path that names no workspace is unroutable", () => {
    for (const pathname of [
      "/api/health",
      "/api/workspaces",
      "/state/workspaces/left/unknown-sink",
      "/streams/unrelated/stream",
    ]) {
      const resolved = resolveRoute(pathname);
      expect(resolved.kind).toBe("failure");
      if (resolved.kind !== "failure") throw new Error("expected a failure");
      expect(resolved.failure._tag).toBe("UnroutableRequest");
    }
  });

  test("anything else is the host's own business", () => {
    expect(resolveRoute("/")).toEqual({ kind: "asset" });
    expect(resolveRoute("/assets/index.js")).toEqual({ kind: "asset" });
  });

  test("a later `workspaces` segment is not a routing key", () => {
    expect(resolveRoute("/streams/other/thing/workspaces/left").kind).toBe("failure");
  });
});

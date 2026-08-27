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
import { globalKey, userKey, workspaceKey } from "../domain/domains.ts";
import { resolveRoute } from "../server/host-routing.ts";

describe("route resolution", () => {
  test("host routes are answered without a partition", () => {
    expect(resolveRoute("/health")).toEqual({ kind: "host", route: "health" });
    expect(resolveRoute("/host/metrics")).toEqual({ kind: "host", route: "metrics" });
  });

  test("every command path names the workspace that owns it", () => {
    expect(resolveRoute("/api/workspaces/left/issues")).toEqual({
      kind: "partition",
      key: workspaceKey("left"),
      target: "application",
    });
    expect(resolveRoute("/api/workspaces/right/issues/issue-1/status")).toEqual({
      kind: "partition",
      key: workspaceKey("right"),
      target: "application",
    });
    expect(resolveRoute("/api/workspaces/right/notifications/drain")).toEqual({
      kind: "partition",
      key: workspaceKey("right"),
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
        kind: "partition",
        key: workspaceKey(workspaceId),
        target: "application",
      });
    }
  });

  test("each domain's API collection names the partition that owns it", () => {
    expect(resolveRoute("/api/users/ada/inbox")).toEqual({
      kind: "partition",
      key: userKey("ada"),
      target: "application",
    });
    expect(resolveRoute("/api/global/exchange")).toEqual({
      kind: "partition",
      key: globalKey(),
      target: "application",
    });
  });

  test("a user id the domain refuses is a typed refusal, not a partition", () => {
    const resolved = resolveRoute("/api/users/..%2Fescape/inbox");
    expect(resolved.kind).toBe("failure");
    if (resolved.kind !== "failure") throw new Error("expected a failure");
    const { _tag: tag } = resolved.failure;
    expect(tag).toBe("InvalidDomainId");
  });

  test("an API collection this host does not serve is unroutable", () => {
    for (const pathname of ["/api/users", "/api/global", "/api/teams/one", "/api/nothing"]) {
      const resolved = resolveRoute(pathname);
      expect(resolved.kind).toBe("failure");
      if (resolved.kind !== "failure") throw new Error("expected a failure");
      const { _tag: tag } = resolved.failure;
      expect(tag).toBe("UnroutableRequest");
    }
  });

  test("a stream path resolves to the workspace named in the stream id", () => {
    expect(resolveRoute("/streams/workspaces/left/issue-events")).toEqual({
      kind: "partition",
      key: workspaceKey("left"),
      target: "streams",
    });
    expect(resolveRoute("/streams/state/workspaces/left/issues")).toEqual({
      kind: "partition",
      key: workspaceKey("left"),
      target: "streams",
    });
  });

  test("a workspace id the domain refuses is a typed refusal, not a partition", () => {
    const resolved = resolveRoute("/api/workspaces/..%2Fescape/issues");
    expect(resolved.kind).toBe("failure");
    if (resolved.kind !== "failure") throw new Error("expected a failure");
    const { _tag: tag } = resolved.failure;
    expect(tag).toBe("InvalidWorkspaceId");
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
      const { _tag: tag } = resolved.failure;
      expect(tag).toBe("UnroutableRequest");
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

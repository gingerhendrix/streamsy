import { describe, expect, test } from "bun:test";
import { compileSinkRoute } from "./route.ts";

const identifier = {
  decode: (value: string): string => {
    if (!/^[a-z0-9-]+$/.test(value)) throw new Error("expected an identifier");
    return value;
  },
};

describe("checked sink routes", () => {
  test("builds and exactly matches typed parameters", () => {
    const route = compileSinkRoute("/state/workspaces/:workspaceId/issues", {
      workspaceId: identifier,
    });
    expect(route.build({ workspaceId: "team-a" })).toBe("/state/workspaces/team-a/issues");
    expect(route.match("/state/workspaces/team-a/issues")).toEqual({
      kind: "matched",
      params: { workspaceId: "team-a" },
    });
    expect(route.match("/state/workspaces/team-a/issues/more")).toEqual({ kind: "mismatch" });
  });

  test("rejects duplicate, missing, extra, and unsafe parameters", () => {
    expect(() =>
      compileSinkRoute("/state/:workspaceId/:workspaceId", { workspaceId: identifier }),
    ).toThrow("duplicate");
    expect(() => compileSinkRoute("/state/:workspaceId", {})).toThrow("no codec");
    expect(() => compileSinkRoute("/state/static", { workspaceId: identifier })).toThrow("absent");
    expect(
      compileSinkRoute("/state/:workspaceId", { workspaceId: identifier }).match(
        "/state/team%2Fsecret",
      ),
    ).toMatchObject({ kind: "invalid", parameter: "workspaceId" });
  });

  test("validates decoded values only after a literal route match", () => {
    const route = compileSinkRoute("/state/:workspaceId/issues", { workspaceId: identifier });
    expect(route.match("/other/NO!/issues")).toEqual({ kind: "mismatch" });
    expect(route.match("/state/NO!/issues")).toMatchObject({
      kind: "invalid",
      parameter: "workspaceId",
    });
  });
});

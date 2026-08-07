import { describe, expect, test } from "vitest";
import { sourceAck, sourceWatermark, streamIdentity } from "../causal.ts";
import { chainedCoverage } from "./coverage-chain.ts";

const events = streamIdentity("issues/ISSUE-42/events");
const detail = streamIdentity("issues/ISSUE-42/detail");
const board = streamIdentity("projects/PROJECT-7/board");

const ack = sourceAck(events, "i42-0088");

function detailHop(sourceThrough: string, output: string) {
  return {
    label: "issue-detail",
    watermark: sourceWatermark(events, sourceThrough),
    output: sourceAck(detail, output),
  };
}

function boardHop(sourceThrough: string, output: string) {
  return {
    label: "project-board",
    watermark: sourceWatermark(detail, sourceThrough),
    output: sourceAck(board, output),
  };
}

describe("chainedCoverage", () => {
  test("proves a complete two-hop chain", () => {
    const result = chainedCoverage(ack, [
      detailHop("i42-0088", "d42-0031"),
      boardHop("d42-0031", "p7-0142"),
    ]);
    expect(result.status).toBe("proven");
    expect(result.hops.map((hop) => hop.label)).toEqual(["issue-detail", "project-board"]);
  });

  test("reports the blocked hop when a later output lags", () => {
    const result = chainedCoverage(ack, [
      detailHop("i42-0088", "d42-0031"),
      boardHop("d42-0030", "p7-0141"),
    ]);
    expect(result).toMatchObject({ status: "not-yet", blockedAt: "project-board" });
    expect(result.hops).toHaveLength(1);
  });

  test("an unstarted hop is not-yet rather than proven", () => {
    const result = chainedCoverage(ack, [{ label: "issue-detail" }]);
    expect(result).toMatchObject({ status: "not-yet", blockedAt: "issue-detail" });
  });

  test("an identity mismatch is incomparable", () => {
    const result = chainedCoverage(ack, [
      {
        label: "issue-detail",
        watermark: sourceWatermark(board, "p7-0142"),
        output: sourceAck(detail, "d42-0031"),
      },
    ]);
    expect(result).toMatchObject({ status: "incomparable", blockedAt: "issue-detail" });
  });
});

import { describe, expect, it } from "vitest";
import { batchEvent } from "../src/stream.ts";
import { issue, upsertIssue } from "./support.ts";

describe("batchEvent", () => {
  it("keeps the changes and skips other items", () => {
    const change = upsertIssue(issue("a"), "tx-1");

    const event = batchEvent({
      items: [{ headers: { control: "up-to-date" } }, change, { type: "unknown" }],
      offset: "1",
      upToDate: true,
      streamClosed: false,
    });

    expect(event).toMatchObject({ _tag: "Batch", isUpToDate: true, changes: [change] });
  });
});

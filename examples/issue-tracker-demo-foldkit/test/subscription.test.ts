import { describe, expect, test } from "vitest";
import { workspaceFromSearch, workspaceHref } from "../src/command.ts";
import { Message } from "../src/message.ts";
import { batchMessage } from "../src/subscription.ts";
import { issue, upsertIssue } from "./support.ts";

describe("batchMessage", () => {
  test("a read batch becomes one ReceivedStateBatch with its changes", () => {
    const change = upsertIssue(issue("issue_a"), "tx-1");

    expect(
      batchMessage({
        items: [change, { headers: { control: "up-to-date" } }, "not a change"],
        offset: "1",
        upToDate: true,
        streamClosed: false,
      }),
    ).toEqual(Message.ReceivedStateBatch({ changes: [change], isUpToDate: true }));
  });
});

describe("workspace links", () => {
  test("?w= selects a workspace; no query is main", () => {
    expect(workspaceFromSearch("?w=team-1")).toBe("team-1");
    expect(workspaceFromSearch("?w=%20")).toBe("main");
    expect(workspaceFromSearch("")).toBe("main");
    expect(workspaceHref("main")).toBe("?");
    expect(workspaceHref("team-1")).toBe("?w=team-1");
  });
});

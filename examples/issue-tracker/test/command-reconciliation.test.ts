/* oxlint-disable effecttsgo/async-function -- bun:test owns the HTTP and protocol fixtures. */
import { afterEach, describe, expect, test } from "bun:test";
import type {
  AppendStreamOptions,
  ClientAppendResult,
  StreamProtocolClient,
  StreamProtocolHandle,
} from "@streamsy/core";
import { streamNames } from "../domain/declaration.ts";
import { CommandResponse, IssuesResponse } from "../shared/api.ts";
import { call, createIssueBody, host, json, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function fresh(applicationClient?: (client: StreamProtocolClient) => StreamProtocolClient): Host {
  const instance = host(applicationClient === undefined ? {} : { applicationClient });
  open.push(instance);
  return instance;
}

describe("command reconciliation", () => {
  test("changed intent under one workspace command id is a 409 and appends nothing", async () => {
    const instance = fresh();
    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Original"),
    );
    const conflict = await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Changed"),
    );
    expect(conflict.status).toBe(409);
    expect(await conflict.json()).toMatchObject({ error: "command-id-conflict" });
    const listed = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(listed.rows).toHaveLength(1);
    expect(listed.rows[0]?.title).toBe("Original");
  });

  test("equal command ids in separate workspaces address separate lanes and receipts", async () => {
    const instance = fresh();
    const left = await json(
      await call(
        instance,
        "POST",
        "/api/workspaces/left/issues",
        createIssueBody("same", "left-issue", "Left"),
      ),
      CommandResponse,
    );
    const right = await json(
      await call(
        instance,
        "POST",
        "/api/workspaces/right/issues",
        createIssueBody("same", "right-issue", "Right"),
      ),
      CommandResponse,
    );
    expect(left.reconciled).toBe(false);
    expect(right.reconciled).toBe(false);
    expect(left.workspaceId).toBe("left");
    expect(right.workspaceId).toBe("right");
  });

  test("a missing receipt is recovered from the bounded source scan at its original offset", async () => {
    const instance = fresh();
    const stream = instance.client.stream(streamNames.issueEvents("main"));
    await stream.create({ contentType: "application/json" });
    const original = await stream.append(
      JSON.stringify({
        type: "IssueCreated",
        eventId: "cmd-lost-receipt",
        workspaceId: "main",
        issueId: "issue-1",
        sequence: 0,
        occurredAt: "2026-08-24T10:00:00.000Z",
        title: "Recovered",
        projectId: "streamsy",
        status: "backlog",
      }),
      { contentType: "application/json" },
    );
    expect(original.status).toBe("appended");
    await stream.append(
      JSON.stringify({
        type: "IssueCreated",
        eventId: "later",
        workspaceId: "main",
        issueId: "issue-2",
        sequence: 1,
        occurredAt: "2026-08-24T10:01:00.000Z",
        title: "Later",
        projectId: "streamsy",
        status: "todo",
      }),
      { contentType: "application/json" },
    );

    const recovered = await json(
      await call(
        instance,
        "POST",
        "/api/workspaces/main/issues",
        createIssueBody("cmd-lost-receipt", "issue-1", "Recovered"),
      ),
      CommandResponse,
    );
    expect(recovered.reconciled).toBe(true);
    expect(recovered.ack.offset).toBe(original.status === "appended" ? original.offset : "");
    expect(recovered.sequence).toBe(0);
  });

  test("an append that commits but loses its response reconciles before retrying", async () => {
    const instance = fresh((client) => new LoseFirstAppendResponse(client));
    const response = await json(
      await call(
        instance,
        "POST",
        "/api/workspaces/main/issues",
        createIssueBody("cmd-lost-response", "issue-1", "Committed once"),
      ),
      CommandResponse,
    );
    expect(response.reconciled).toBe(true);
    const listed = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(listed.rows).toHaveLength(1);
  });
});

class LoseFirstAppendResponse implements StreamProtocolClient {
  private lose = true;

  constructor(private readonly inner: StreamProtocolClient) {}

  stream(streamId: string): StreamProtocolHandle {
    const handle = this.inner.stream(streamId);
    return {
      id: handle.id,
      head: (options) => handle.head(options),
      create: (options) => handle.create(options),
      append: async (data: Uint8Array | string, options?: AppendStreamOptions) => {
        const result = await handle.append(data, options);
        if (this.lose && streamId.endsWith("/issue-events") && result.status === "appended") {
          this.lose = false;
          return {
            status: "error",
            code: "transport",
            message: "response lost after commit",
            retryable: true,
          } satisfies ClientAppendResult;
        }
        return result;
      },
      appendJsonBatch: (items, options) => handle.appendJsonBatch(items, options),
      close: (options) => handle.close(options),
      read: (options) => handle.read(options),
    };
  }

  close(cause?: unknown): Promise<void> {
    return this.inner.close(cause);
  }
}

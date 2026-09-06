/* oxlint-disable effecttsgo/async-function -- bun:test owns concurrent HTTP requests. */
import { afterEach, describe, expect, test } from "bun:test";
import type {
  AppendStreamOptions,
  StreamProtocolClient,
  StreamProtocolHandle,
} from "@streamsy/core";
import { CommandResponse, IssuesResponse } from "../shared/api.ts";
import { call, createIssueBody, host, json, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

describe("command CAS", () => {
  test("concurrent new commands receive unique increasing source sequences", async () => {
    const instance = host();
    open.push(instance);
    const responses = await Promise.all(
      Array.from({ length: 12 }, (_, index) =>
        call(
          instance,
          "POST",
          "/api/workspaces/main/issues",
          createIssueBody(`cmd-${index}`, `issue-${index}`, `Issue ${index}`),
        ).then((response) => json(response, CommandResponse)),
      ),
    );
    expect(responses.map((response) => response.sequence).toSorted((a, b) => a - b)).toEqual(
      Array.from({ length: 12 }, (_, index) => index),
    );
    const listed = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(listed.rows).toHaveLength(12);
  });

  test("concurrent copies of one command append one event", async () => {
    const instance = host();
    open.push(instance);
    const body = createIssueBody("same-command", "issue-1", "Exactly once");
    const responses = await Promise.all(
      Array.from({ length: 8 }, () =>
        call(instance, "POST", "/api/workspaces/main/issues", body).then((response) =>
          json(response, CommandResponse),
        ),
      ),
    );
    expect(new Set(responses.map((response) => response.ack.offset)).size).toBe(1);
    const listed = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(listed.rows).toHaveLength(1);
  });

  test("exhausted expected-offset contention maps to HTTP 409", async () => {
    const instance = host({ applicationClient: (client) => new AlwaysContendedClient(client) });
    open.push(instance);
    const response = await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("blocked", "issue-blocked", "Never accepted"),
    );
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({ error: "command-contention" });
  });
});

class AlwaysContendedClient implements StreamProtocolClient {
  private sequence = 100;

  constructor(private readonly inner: StreamProtocolClient) {}

  stream(streamId: string): StreamProtocolHandle {
    const handle = this.inner.stream(streamId);
    return {
      id: handle.id,
      head: (options) => handle.head(options),
      create: (options) => handle.create(options),
      append: async (data: Uint8Array | string, options?: AppendStreamOptions) => {
        if (streamId.endsWith("/issue-events") && options?.producer !== undefined) {
          const sequence = this.sequence++;
          await handle.append(
            JSON.stringify({
              type: "IssueCreated",
              eventId: `contender-${sequence}`,
              workspaceId: "main",
              issueId: `contender-${sequence}`,
              sequence,
              occurredAt: "2026-08-24T10:00:00.000Z",
              title: `Contender ${sequence}`,
              projectId: "streamsy",
              status: "backlog",
            }),
            { contentType: "application/json" },
          );
        }
        return handle.append(data, options);
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

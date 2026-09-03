import { describe, expect, it } from "vitest";
import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  ZERO_OFFSET,
  type CommitEvent,
  type JsonValue,
  type StreamProtocolFactory,
} from "./index.ts";
import { createJsonProtocol, type JsonCodec } from "./json.ts";

type CommitFrame = {
  streamId: string;
  offset: string;
  closed: boolean;
  softDeleted: boolean;
};

type JsonRecord = { readonly [key: string]: JsonValue };

function isJsonRecord(value: JsonValue): value is JsonRecord {
  return value !== null && value instanceof Object && !Array.isArray(value);
}

function isJsonString(value: JsonValue | undefined): value is string {
  return value?.constructor === String;
}

function isJsonBoolean(value: JsonValue | undefined): value is boolean {
  return value?.constructor === Boolean;
}

function parseCommitFrame(value: JsonValue): CommitFrame {
  if (!isJsonRecord(value)) throw new Error("invalid commit frame");
  const { streamId, offset, closed, softDeleted } = value;
  if (
    !isJsonString(streamId) ||
    !isJsonString(offset) ||
    !isJsonBoolean(closed) ||
    !isJsonBoolean(softDeleted)
  ) {
    throw new Error("invalid commit frame");
  }
  return { streamId, offset, closed, softDeleted };
}

const commitFrameCodec: JsonCodec<CommitFrame> = {
  encode(value) {
    return value;
  },
  decode: parseCommitFrame,
};

function createProtocol(): StreamProtocolFactory {
  return createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
}

function frameOf(commit: CommitEvent): CommitFrame {
  return {
    streamId: commit.streamId,
    offset: commit.offset,
    closed: commit.closed,
    softDeleted: commit.softDeleted,
  };
}

describe("after-commit JSON fan-in recipe", () => {
  it("pushes offset-only commit frames through a pre-created typed JsonStream", async () => {
    // Supersedes the v1 plan section 3 snippet per cmt_8011b639: create the
    // subscription stream once outside the hook and append typed JSON objects.
    const sources = createProtocol();
    const writes = createProtocol();
    const jsonWrites = createJsonProtocol<CommitFrame>(writes, commitFrameCodec);

    const createdSub = await jsonWrites.create("agent-overview");
    expect(createdSub.status).toBe("created");
    if (createdSub.status !== "created") throw new Error("expected subscription stream");
    const subStream = createdSub.stream;

    const pending: Promise<unknown>[] = [];
    const failures: unknown[] = [];
    sources.onAfterCommit((commit) => {
      if (commit.streamId !== "agent/42") return;
      if (commit.offset === ZERO_OFFSET) return;
      pending.push(subStream.append(frameOf(commit)).catch((error) => failures.push(error)));
    });

    const source = await sources.create("agent/42", { contentType: "text/plain" });
    expect(source.status).toBe("created");
    if (source.status !== "created") throw new Error("expected source stream");
    const appended = await source.stream.append({
      contentType: "text/plain",
      data: new TextEncoder().encode("update"),
    });
    expect(appended.status).toBe("appended");
    if (appended.status !== "appended") throw new Error("expected source append");
    await Promise.all(pending);

    expect(failures).toEqual([]);
    const read = await subStream.read();
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected read");
    expect(read.messages.map((message) => message.value)).toEqual([
      { streamId: "agent/42", offset: appended.offset, closed: false, softDeleted: false },
    ]);
  });
});

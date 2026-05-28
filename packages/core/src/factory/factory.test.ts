import { describe, expect, it } from "vitest";
import { composeStream, notSupported, isNotSupported } from "../index.ts";
import type { StreamRecord } from "../types/storage.ts";

describe("composeStream", () => {
  it("binds record and message operations to one storage stream", async () => {
    let record: StreamRecord | null = null;
    const messages: unknown[] = [];
    const stream = composeStream({
      id: "s",
      recordStore: {
        getRecord: async () => record,
        createRecord: async (next) => {
          if (record) return { status: "exists", record };
          record = next;
          return { status: "created" };
        },
        updateRecord: async (patch) => {
          record = {
            ...record!,
            config: { ...record!.config, ...patch.config },
            lifecycle: { ...record!.lifecycle, ...patch.lifecycle },
            currentOffset: patch.currentOffset ?? record!.currentOffset,
            counter: patch.counter ?? record!.counter,
          };
          return record;
        },
        deleteRecord: async () => {
          record = null;
        },
      },
      messageStore: {
        appendMessages: async (next) => void messages.push(...next),
        listMessages: async () => messages as never,
        deleteMessages: async () => void (messages.length = 0),
      },
    });

    expect(stream.id).toBe("s");
    expect(await stream.getRecord()).toBeNull();
    expect(isNotSupported(notSupported("x"))).toBe(true);
  });
});

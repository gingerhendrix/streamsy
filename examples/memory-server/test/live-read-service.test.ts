import { describe, expect, it } from "vitest";
import {
  LiveReadService,
  type LiveReadDeps,
} from "../../../packages/core/src/protocol/live-read-service.ts";
import type {
  Clock,
  StoredMessage,
  StreamRecord,
  StreamStoreAdapter,
  WaitForEventResult,
} from "../../../packages/core/src/types/storage.ts";

const ZERO_OFFSET = "0000000000000000_0000000000000000";

function record(overrides: Partial<StreamRecord> = {}): StreamRecord {
  return {
    id: "stream-1",
    config: { contentType: "application/octet-stream", createdAt: 0 },
    lifecycle: { childRefCount: 0 },
    currentOffset: "0000000000000002_0000000000000000",
    counter: 2,
    ...overrides,
  };
}

function message(offset: string): StoredMessage {
  return {
    offset,
    data: new TextEncoder().encode(offset),
    timestamp: 0,
  };
}

function fakeClock(nowMs: number): Clock {
  return {
    now: () => nowMs,
    date: (value?: number | string) => (value === undefined ? new Date(nowMs) : new Date(value)),
  };
}

interface StoreOptions {
  records: Array<StreamRecord | null>;
  waitForEvent?: (streamId: string) => Promise<WaitForEventResult>;
}

function makeStore(opts: StoreOptions): StreamStoreAdapter & { getCalls: number } {
  const { records } = opts;
  let i = 0;
  const store: StreamStoreAdapter & { getCalls: number } = {
    getCalls: 0,
    async get(streamId) {
      expect(streamId).toBe("stream-1");
      const next = i < records.length ? records[i]! : records[records.length - 1]!;
      i += 1;
      store.getCalls += 1;
      return next;
    },
    async create() {
      throw new Error("unexpected create");
    },
    async update() {
      throw new Error("unexpected update");
    },
    async delete() {
      throw new Error("unexpected delete");
    },
    async append() {
      throw new Error("unexpected append");
    },
    async list() {
      throw new Error("unexpected list");
    },
    async deleteMessages() {
      throw new Error("unexpected deleteMessages");
    },
    async getProducerState() {
      throw new Error("unexpected getProducerState");
    },
    async setProducerState() {
      throw new Error("unexpected setProducerState");
    },
    async deleteProducerStates() {
      throw new Error("unexpected deleteProducerStates");
    },
    async incrementChildRefCount() {
      throw new Error("unexpected incrementChildRefCount");
    },
    async decrementChildRefCount() {
      throw new Error("unexpected decrementChildRefCount");
    },
  };
  if (opts.waitForEvent) store.waitForEvent = opts.waitForEvent;
  return store;
}

function unusedDeps(): LiveReadDeps {
  return {
    readChain: async () => {
      throw new Error("unexpected readChain");
    },
    readOwn: async () => {
      throw new Error("unexpected readOwn");
    },
    touch: async () => {
      throw new Error("unexpected touch");
    },
  };
}

const CURSOR_EPOCH_MS = new Date("2024-10-09T00:00:00.000Z").getTime();
// Pin clock.now() so generateCursor returns "0".
const PINNED_NOW = CURSOR_EPOCH_MS;

describe("LiveReadService.execute", () => {
  it("returns not-found with empty cursor and no other calls", async () => {
    const service = new LiveReadService(
      makeStore({ records: [null] }),
      fakeClock(PINNED_NOW),
      1500,
      unusedDeps(),
    );

    await expect(
      service.execute("stream-1", { offset: ZERO_OFFSET, mode: "long-poll" }),
    ).resolves.toEqual({
      status: "not-found",
      messages: [],
      nextOffset: "",
      upToDate: false,
      cursor: "",
    });
  });

  it("returns gone with empty cursor for soft-deleted records", async () => {
    const service = new LiveReadService(
      makeStore({
        records: [record({ lifecycle: { childRefCount: 0, softDeleted: true } })],
      }),
      fakeClock(PINNED_NOW),
      1500,
      unusedDeps(),
    );

    await expect(
      service.execute("stream-1", { offset: ZERO_OFFSET, mode: "long-poll" }),
    ).resolves.toEqual({
      status: "gone",
      messages: [],
      nextOffset: "",
      upToDate: false,
      cursor: "",
    });
  });

  describe("closed-stream replay", () => {
    it("returns timeout/closed=true for an empty replay at the tail", async () => {
      const closed = record({
        currentOffset: "0000000000000005_0000000000000000",
        lifecycle: { childRefCount: 0, closed: true },
      });
      const deps: LiveReadDeps = {
        ...unusedDeps(),
        readChain: async (id, rec, after) => {
          expect(id).toBe("stream-1");
          expect(rec).toBe(closed);
          expect(after).toBe("0000000000000005_0000000000000000");
          return [];
        },
      };
      const service = new LiveReadService(
        makeStore({ records: [closed] }),
        fakeClock(PINNED_NOW),
        1500,
        deps,
      );

      await expect(
        service.execute("stream-1", {
          offset: "0000000000000005_0000000000000000",
          mode: "long-poll",
        }),
      ).resolves.toEqual({
        status: "timeout",
        messages: [],
        nextOffset: "0000000000000005_0000000000000000",
        upToDate: true,
        cursor: "0",
        closed: true,
      });
    });

    it("returns ok with messages and closed=true when last message is at the tail", async () => {
      const closed = record({
        currentOffset: "0000000000000005_0000000000000000",
        lifecycle: { childRefCount: 0, closed: true },
      });
      const messages = [message("0000000000000004_0000000000000000")];
      const deps: LiveReadDeps = { ...unusedDeps(), readChain: async () => messages };
      const service = new LiveReadService(
        makeStore({ records: [closed] }),
        fakeClock(PINNED_NOW),
        1500,
        deps,
      );

      await expect(
        service.execute("stream-1", { offset: ZERO_OFFSET, mode: "long-poll" }),
      ).resolves.toEqual({
        status: "ok",
        messages,
        nextOffset: "0000000000000005_0000000000000000",
        upToDate: true,
        cursor: "0",
        closed: true,
      });
    });

    it("uses last-message offset as nextOffset when ahead of currentOffset", async () => {
      const closed = record({
        currentOffset: "0000000000000005_0000000000000000",
        lifecycle: { childRefCount: 0, closed: true },
      });
      const messages = [message("0000000000000006_0000000000000000")];
      const deps: LiveReadDeps = { ...unusedDeps(), readChain: async () => messages };
      const service = new LiveReadService(
        makeStore({ records: [closed] }),
        fakeClock(PINNED_NOW),
        1500,
        deps,
      );

      await expect(
        service.execute("stream-1", { offset: ZERO_OFFSET, mode: "long-poll" }),
      ).resolves.toEqual({
        status: "ok",
        messages,
        nextOffset: "0000000000000006_0000000000000000",
        upToDate: true,
        cursor: "0",
        closed: false,
      });
    });
  });

  describe("fork-source upstream tail", () => {
    it("uses readChain when offset is below forkOffset", async () => {
      const fork = record({
        currentOffset: "0000000000000005_0000000000000000",
        lifecycle: {
          childRefCount: 0,
          forkedFrom: "parent",
          forkOffset: "0000000000000003_0000000000000000",
        },
      });
      const messages = [message("0000000000000002_0000000000000000")];
      const deps: LiveReadDeps = {
        ...unusedDeps(),
        readChain: async (id, rec, after) => {
          expect(id).toBe("stream-1");
          expect(rec).toBe(fork);
          expect(after).toBe(ZERO_OFFSET);
          return messages;
        },
      };
      const service = new LiveReadService(
        makeStore({ records: [fork] }),
        fakeClock(PINNED_NOW),
        1500,
        deps,
      );

      await expect(
        service.execute("stream-1", { offset: ZERO_OFFSET, mode: "long-poll" }),
      ).resolves.toEqual({
        status: "ok",
        messages,
        nextOffset: "0000000000000005_0000000000000000",
        upToDate: true,
        cursor: "0",
      });
    });

    it("does not take fork-tail branch when offset is at or above forkOffset", async () => {
      const fork = record({
        currentOffset: "0000000000000005_0000000000000000",
        lifecycle: {
          childRefCount: 0,
          forkedFrom: "parent",
          forkOffset: "0000000000000003_0000000000000000",
        },
      });
      const ownMessages = [message("0000000000000004_0000000000000000")];
      let touched = false;
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async (id, after) => {
          expect(id).toBe("stream-1");
          expect(after).toBe("0000000000000003_0000000000000000");
          return { messages: ownMessages, nextOffset: "0000000000000004_0000000000000000" };
        },
        touch: async (id, rec) => {
          expect(id).toBe("stream-1");
          expect(rec).toBe(fork);
          touched = true;
        },
      };
      const service = new LiveReadService(
        makeStore({ records: [fork] }),
        fakeClock(PINNED_NOW),
        1500,
        deps,
      );

      const result = await service.execute("stream-1", {
        offset: "0000000000000003_0000000000000000",
        mode: "long-poll",
      });
      expect(touched).toBe(true);
      expect(result).toEqual({
        status: "ok",
        messages: ownMessages,
        nextOffset: "0000000000000004_0000000000000000",
        upToDate: true,
        cursor: "0",
      });
    });
  });

  describe("immediate own-message path", () => {
    it("returns ok immediately when readOwn yields messages", async () => {
      const open = record({ currentOffset: "0000000000000005_0000000000000000" });
      const ownMessages = [message("0000000000000004_0000000000000000")];
      let touched = false;
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async (id, after) => {
          expect(id).toBe("stream-1");
          expect(after).toBe(ZERO_OFFSET);
          return { messages: ownMessages, nextOffset: "0000000000000004_0000000000000000" };
        },
        touch: async () => {
          touched = true;
        },
      };
      const service = new LiveReadService(
        makeStore({ records: [open] }),
        fakeClock(PINNED_NOW),
        1500,
        deps,
      );

      const result = await service.execute("stream-1", {
        offset: ZERO_OFFSET,
        mode: "long-poll",
      });
      expect(touched).toBe(true);
      expect(result).toEqual({
        status: "ok",
        messages: ownMessages,
        nextOffset: "0000000000000004_0000000000000000",
        upToDate: true,
        cursor: "0",
      });
    });
  });

  describe("wait/timeout path", () => {
    it("returns timeout when waitForEvent times out and tail is unchanged", async () => {
      const open = record({ currentOffset: "0000000000000005_0000000000000000" });
      const empty = { messages: [], nextOffset: "0000000000000005_0000000000000000" };
      const store = makeStore({
        records: [open, open],
        waitForEvent: async () => ({ status: "timeout" }),
      });
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async () => empty,
        touch: async () => {},
      };
      const service = new LiveReadService(store, fakeClock(PINNED_NOW), 1500, deps);

      const result = await service.execute("stream-1", {
        offset: "0000000000000005_0000000000000000",
        mode: "long-poll",
      });
      expect(result).toEqual({
        status: "timeout",
        messages: [],
        nextOffset: "0000000000000005_0000000000000000",
        upToDate: true,
        cursor: "0",
        closed: false,
      });
      expect(store.getCalls).toBe(2);
    });

    it("returns ok when waitForEvent times out but the final read yields messages", async () => {
      const before = record({ currentOffset: "0000000000000005_0000000000000000" });
      const after = record({ currentOffset: "0000000000000006_0000000000000000" });
      const newMessages = [message("0000000000000006_0000000000000000")];
      const store = makeStore({
        records: [before, after],
        waitForEvent: async () => ({ status: "timeout" }),
      });
      let readOwnCalls = 0;
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async () => {
          readOwnCalls += 1;
          if (readOwnCalls === 1)
            return { messages: [], nextOffset: "0000000000000005_0000000000000000" };
          return { messages: newMessages, nextOffset: "0000000000000006_0000000000000000" };
        },
        touch: async () => {},
      };
      const service = new LiveReadService(store, fakeClock(PINNED_NOW), 1500, deps);

      const result = await service.execute("stream-1", {
        offset: "0000000000000005_0000000000000000",
        mode: "long-poll",
      });
      expect(result).toEqual({
        status: "ok",
        messages: newMessages,
        nextOffset: "0000000000000006_0000000000000000",
        upToDate: true,
        cursor: "0",
        closed: false,
      });
    });

    it("returns ok when waitForEvent is aborted but the final read yields messages", async () => {
      const before = record({ currentOffset: "0000000000000005_0000000000000000" });
      const after = record({ currentOffset: "0000000000000006_0000000000000000" });
      const newMessages = [message("0000000000000006_0000000000000000")];
      const store = makeStore({
        records: [before, after],
        waitForEvent: async () => ({ status: "aborted" }),
      });
      let readOwnCalls = 0;
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async () => {
          readOwnCalls += 1;
          if (readOwnCalls === 1)
            return { messages: [], nextOffset: "0000000000000005_0000000000000000" };
          return { messages: newMessages, nextOffset: "0000000000000006_0000000000000000" };
        },
        touch: async () => {},
      };
      const service = new LiveReadService(store, fakeClock(PINNED_NOW), 1500, deps);

      const result = await service.execute("stream-1", {
        offset: "0000000000000005_0000000000000000",
        mode: "long-poll",
      });
      expect(result).toEqual({
        status: "ok",
        messages: newMessages,
        nextOffset: "0000000000000006_0000000000000000",
        upToDate: true,
        cursor: "0",
        closed: false,
      });
    });

    it("returns ok and closed=true when notified, latest is closed, and reachedTail", async () => {
      const before = record({ currentOffset: "0000000000000005_0000000000000000" });
      const after = record({
        currentOffset: "0000000000000006_0000000000000000",
        lifecycle: { childRefCount: 0, closed: true },
      });
      const newTail = [message("0000000000000006_0000000000000000")];
      const store = makeStore({
        records: [before, after],
        waitForEvent: async () => ({ status: "notified", type: "message" }),
      });
      let readOwnCalls = 0;
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async () => {
          readOwnCalls += 1;
          if (readOwnCalls === 1)
            return { messages: [], nextOffset: "0000000000000005_0000000000000000" };
          return { messages: newTail, nextOffset: "0000000000000006_0000000000000000" };
        },
        touch: async () => {},
      };
      const service = new LiveReadService(store, fakeClock(PINNED_NOW), 1500, deps);

      const result = await service.execute("stream-1", {
        offset: "0000000000000005_0000000000000000",
        mode: "long-poll",
      });
      expect(result).toEqual({
        status: "ok",
        messages: newTail,
        nextOffset: "0000000000000006_0000000000000000",
        upToDate: true,
        cursor: "0",
        closed: true,
      });
    });

    it("returns timeout for aborted waits", async () => {
      const open = record({ currentOffset: "0000000000000005_0000000000000000" });
      const empty = { messages: [], nextOffset: "0000000000000005_0000000000000000" };
      const store = makeStore({
        records: [open, open],
        waitForEvent: async () => ({ status: "aborted" }),
      });
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async () => empty,
        touch: async () => {},
      };
      const service = new LiveReadService(store, fakeClock(PINNED_NOW), 1500, deps);

      await expect(
        service.execute("stream-1", {
          offset: "0000000000000005_0000000000000000",
          mode: "long-poll",
        }),
      ).resolves.toMatchObject({ status: "timeout" });
    });

    it("returns not-found when the record disappears during the wait", async () => {
      const open = record({ currentOffset: "0000000000000005_0000000000000000" });
      const store = makeStore({
        records: [open, null],
        waitForEvent: async () => ({ status: "timeout" }),
      });
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async () => ({
          messages: [],
          nextOffset: "0000000000000005_0000000000000000",
        }),
        touch: async () => {},
      };
      const service = new LiveReadService(store, fakeClock(PINNED_NOW), 1500, deps);

      await expect(
        service.execute("stream-1", {
          offset: "0000000000000005_0000000000000000",
          mode: "long-poll",
        }),
      ).resolves.toEqual({
        status: "not-found",
        messages: [],
        nextOffset: "",
        upToDate: false,
        cursor: "",
      });
    });

    it("returns gone when the record becomes soft-deleted during the wait", async () => {
      const open = record({ currentOffset: "0000000000000005_0000000000000000" });
      const deleted = record({
        currentOffset: "0000000000000005_0000000000000000",
        lifecycle: { childRefCount: 0, softDeleted: true },
      });
      const store = makeStore({
        records: [open, deleted],
        waitForEvent: async () => ({ status: "timeout" }),
      });
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async () => ({
          messages: [],
          nextOffset: "0000000000000005_0000000000000000",
        }),
        touch: async () => {},
      };
      const service = new LiveReadService(store, fakeClock(PINNED_NOW), 1500, deps);

      await expect(
        service.execute("stream-1", {
          offset: "0000000000000005_0000000000000000",
          mode: "long-poll",
        }),
      ).resolves.toEqual({
        status: "gone",
        messages: [],
        nextOffset: "",
        upToDate: false,
        cursor: "",
      });
    });

    it("falls back to setTimeout when adapter has no waitForEvent", async () => {
      const open = record({ currentOffset: "0000000000000005_0000000000000000" });
      const store = makeStore({ records: [open, open] });
      const deps: LiveReadDeps = {
        readChain: async () => {
          throw new Error("unexpected readChain");
        },
        readOwn: async () => ({
          messages: [],
          nextOffset: "0000000000000005_0000000000000000",
        }),
        touch: async () => {},
      };
      const service = new LiveReadService(store, fakeClock(PINNED_NOW), 0, deps);

      await expect(
        service.execute("stream-1", {
          offset: "0000000000000005_0000000000000000",
          mode: "long-poll",
        }),
      ).resolves.toMatchObject({ status: "timeout" });
    });
  });

  it("forwards the abort signal and timeout to waitForEvent", async () => {
    const open = record({ currentOffset: "0000000000000005_0000000000000000" });
    const ac = new AbortController();
    let observedTimeout: number | undefined;
    let observedSignal: AbortSignal | undefined;
    const store = makeStore({
      records: [open, open],
      waitForEvent: async (id) => {
        expect(id).toBe("stream-1");
        return { status: "timeout" };
      },
    });
    store.waitForEvent = async (id, opts) => {
      expect(id).toBe("stream-1");
      observedTimeout = opts.timeoutMs;
      observedSignal = opts.signal;
      return { status: "timeout" };
    };
    const deps: LiveReadDeps = {
      readChain: async () => {
        throw new Error("unexpected readChain");
      },
      readOwn: async () => ({
        messages: [],
        nextOffset: "0000000000000005_0000000000000000",
      }),
      touch: async () => {},
    };
    const service = new LiveReadService(store, fakeClock(PINNED_NOW), 2222, deps);

    await service.execute("stream-1", {
      offset: "0000000000000005_0000000000000000",
      mode: "long-poll",
      signal: ac.signal,
    });

    expect(observedTimeout).toBe(2222);
    expect(observedSignal).toBe(ac.signal);
  });
});

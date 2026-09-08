import { test, expect } from "bun:test";
import { access, rm } from "node:fs/promises";
import type { ExampleLifecycle, ExampleResponse } from "./cloudflare-usage.ts";
import { runOwnedExample } from "./cloudflare-usage.ts";

const response = (status = 201): ExampleResponse => ({
  status,
  arrayBuffer: async () => new ArrayBuffer(0),
});

const exists = async (path: string): Promise<boolean> => {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
};

const messages = (error: Error | AggregateError): string =>
  error instanceof AggregateError
    ? error.errors.map((item) => messages(item)).join(" | ")
    : error instanceof Error
      ? error.message
      : String(error);

const baseLifecycle = (overrides: Partial<ExampleLifecycle> = {}): ExampleLifecycle => ({
  build: async () => "/tmp/example-worker.js",
  start: () => ({
    ready: Promise.resolve({ origin: "http://127.0.0.1:1" }),
    dispose: async () => {},
  }),
  request: async () => response(),
  disposalTimeoutMs: 10,
  ...overrides,
});

test("example lifecycle removes a root only after successful disposal", async () => {
  let root = "";
  let disposed = 0;
  await runOwnedExample(
    baseLifecycle({
      onRoot: (path) => {
        root = path;
      },
      start: () => ({
        ready: Promise.resolve({ origin: "http://127.0.0.1:1" }),
        dispose: async () => {
          disposed++;
        },
      }),
    }),
  );
  expect(disposed).toBe(1);
  expect(await exists(root)).toBe(false);
});

test.each([
  [
    "build",
    (lifecycle: ExampleLifecycle): ExampleLifecycle => ({
      ...lifecycle,
      build: async () => {
        throw new Error("build failed");
      },
    }),
  ],
  [
    "constructor",
    (lifecycle: ExampleLifecycle): ExampleLifecycle => ({
      ...lifecycle,
      start: () => {
        throw new Error("constructor failed");
      },
    }),
  ],
  [
    "ready",
    (lifecycle: ExampleLifecycle): ExampleLifecycle => ({
      ...lifecycle,
      start: () => ({ ready: Promise.reject(new Error("ready failed")), dispose: async () => {} }),
    }),
  ],
  [
    "body",
    (lifecycle: ExampleLifecycle): ExampleLifecycle => ({
      ...lifecycle,
      request: async () => ({
        status: 201,
        arrayBuffer: async () => {
          throw new Error("body failed");
        },
      }),
    }),
  ],
] as const)("preserves %s failures and reclaims a disposable root", async (_label, alter) => {
  let root = "";
  const lifecycle = alter(
    baseLifecycle({
      onRoot: (path) => {
        root = path;
      },
    }),
  );
  let failed = false;
  try {
    await runOwnedExample(lifecycle);
  } catch {
    failed = true;
  }
  expect(failed).toBe(true);
  expect(await exists(root)).toBe(false);
});

test("a transient disposal failure is retried before root removal", async () => {
  let root = "";
  let attempts = 0;
  await runOwnedExample(
    baseLifecycle({
      onRoot: (path) => {
        root = path;
      },
      start: () => ({
        ready: Promise.resolve({ origin: "http://127.0.0.1:1" }),
        dispose: async () => {
          attempts++;
          if (attempts === 1) throw new Error("transient dispose failure");
        },
      }),
    }),
  );
  expect(attempts).toBe(2);
  expect(await exists(root)).toBe(false);
});

test("unresolved disposal retains the root and combines the primary failure", async () => {
  let root = "";
  let attempts = 0;
  const primary = new Error("ready failed");
  let caught: Error | AggregateError | undefined;
  try {
    await runOwnedExample(
      baseLifecycle({
        onRoot: (path) => {
          root = path;
        },
        start: () => ({
          ready: Promise.reject(primary),
          dispose: () => {
            attempts++;
            return new Promise<void>((_, reject) =>
              setTimeout(() => reject(new Error("dispose failed")), 20),
            );
          },
        }),
      }),
    );
  } catch (error) {
    caught = error instanceof Error ? error : new Error(String(error));
  }
  expect(attempts).toBe(2);
  if (caught === undefined) throw new Error("expected unresolved disposal to fail");
  expect(messages(caught)).toContain("ready failed");
  expect(messages(caught)).toContain("retained root");
  expect(await exists(root)).toBe(true);
  await rm(root, { recursive: true, force: true });
});

test("root-removal failure is retained rather than reported as cleanup success", async () => {
  let root = "";
  const removal = new Error("remove failed");
  let caught: Error | AggregateError | undefined;
  try {
    await runOwnedExample(
      baseLifecycle({
        onRoot: (path) => {
          root = path;
        },
        removeRoot: async () => {
          throw removal;
        },
      }),
    );
  } catch (error) {
    caught = error instanceof Error ? error : new Error(String(error));
  }
  if (caught === undefined) throw new Error("expected removal failure");
  expect(messages(caught)).toContain("remove failed");
  expect(await exists(root)).toBe(true);
  await rm(root, { recursive: true, force: true });
});

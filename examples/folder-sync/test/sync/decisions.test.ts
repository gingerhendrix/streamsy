import { expect, test } from "bun:test";
import {
  applyChange,
  conflictBase,
  encodeFilePath,
  feedReset,
  folderDirectory,
  isEcho,
} from "../../sync/decisions.ts";
import { parseArguments } from "../../sync/arguments.ts";

const head = { version: "0000000000000001_0000000000000002", hash: "a", localHash: "b" };

test("echo checks distinguish a pending edit reverted to the server's bytes", () => {
  expect(isEcho("b", head, true)).toBe(true);
  expect(isEcho("a", head, true)).toBe(false);
  expect(isEcho("a", head)).toBe(true);
  expect(isEcho(null, undefined)).toBe(true);
  expect(isEcho("c", head)).toBe(false);
});

test("apply decisions ignore duplicate and older per-folder versions", () => {
  expect(applyChange(head, head.version)).toBe(false);
  expect(applyChange(head, "0000000000000001_0000000000000001")).toBe(false);
  expect(applyChange(head, "0000000000000001_0000000000000003")).toBe(true);
  expect(applyChange(undefined, head.version)).toBe(true);
  expect(conflictBase(head)).toBe(head.version);
  expect(conflictBase({ ...head, hash: null, localHash: null })).toBe(head.version);
  expect(conflictBase(undefined)).toBeNull();
});

test("mapping preserves nested IDs, URL encoding preserves individual file segments", () => {
  expect(folderDirectory("daily/demo")).toBe("daily/demo");
  expect(encodeFilePath("docs/雪 100%.md")).toBe("docs/%E9%9B%AA%20100%25.md");
  expect(feedReset("b", "a")).toBe(true);
  expect(feedReset("a", "b")).toBe(false);
});

test("arguments validate required values and writable selection", () => {
  expect(
    parseArguments([
      "--server",
      "http://localhost:1339",
      "--dir",
      "mirror",
      "--folders",
      "daily/demo,ro",
      "--write",
      "daily/demo",
    ]),
  ).toEqual({
    server: "http://localhost:1339",
    directory: "mirror",
    folders: ["daily/demo", "ro"],
    writable: ["daily/demo"],
  });
  expect(parseArguments(["--server", "http://localhost:1339", "--dir", "mirror"]).writable).toEqual(
    [],
  );
  expect(() => parseArguments(["--dir", "mirror"])).toThrow();
  expect(() => parseArguments(["--server", "file:///root", "--dir", "mirror"])).toThrow();
  expect(() =>
    parseArguments([
      "--server",
      "http://localhost",
      "--dir",
      "mirror",
      "--folders",
      "a",
      "--write",
      "b",
    ]),
  ).toThrow();
});

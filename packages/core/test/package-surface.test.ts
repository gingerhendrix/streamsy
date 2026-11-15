import { Record } from "effect";
import { expect, test } from "vitest";
import * as Root from "@streamsy/core";
import * as Internal from "@streamsy/core/internal";
import * as Protocol from "@streamsy/core/protocol";

test("public root exposes fault schemas, guards and result matchers", () => {
  for (const name of [
    "StreamsFault",
    "isStreamsFault",
    "isRetryableFault",
    "CreateResult",
    "InvalidCreateRequest",
    "AppendResult",
  ]) {
    expect(Record.keys(Root)).toContain(name);
  }

  expect(
    Root.CreateResult.$is("Exists")(
      Root.CreateResult.Exists({
        nextOffset: Root.ZERO_OFFSET,
        contentType: "application/json",
        closed: false,
      }),
    ),
  ).toBe(true);
  expect(
    Root.AppendResult.$is("Appended")(
      Root.AppendResult.Appended({ offset: Root.ZERO_OFFSET, closed: false }),
    ),
  ).toBe(true);
});

test("public roots isolate internals", () => {
  expect(Record.keys(Root)).not.toContain("OwnerRecords");
  expect(Record.keys(Protocol.Protocol)).not.toContain("create");
  expect(Record.keys(Protocol.Protocol)).not.toContain("expireIfNeeded");
});

test("Streamsy keeps declaration internals and wake classification private", () => {
  for (const name of [
    "isWake",
    "WakeKey",
    "declareStream",
    "wireApi",
    "DeclaredClass",
    "Bound",
    "Wires",
    "WakeApi",
  ]) {
    expect(Record.keys(Root.Streamsy)).not.toContain(name);
  }
});

test("read offset helpers are internal exports only", () => {
  for (const name of ["attachOffsets", "offsetsOf"]) {
    expect(Record.keys(Internal)).toContain(name);
    expect(Record.keys(Root)).not.toContain(name);
    expect(Record.keys(Protocol)).not.toContain(name);
  }
});

import { expect, test } from "bun:test";
import * as Root from "@streamsy/core";
import * as Protocol from "@streamsy/core/protocol";

test("public root exposes fault schemas, guards and result matchers", () => {
  for (const name of [
    "StreamsFault",
    "isStreamsFault",
    "isRetryableFault",
    "CreateResult",
    "AppendResult",
  ]) {
    expect(Object.keys(Root)).toContain(name);
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
  expect(Object.keys(Root)).not.toContain("OwnerRecords");
  expect(Object.keys(Protocol.Protocol)).not.toContain("create");
  expect(Object.keys(Protocol.Protocol)).not.toContain("expireIfNeeded");
});

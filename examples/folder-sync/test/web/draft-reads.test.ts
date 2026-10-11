/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise -- These browser regression fixtures exercise native delayed promises and AbortSignals. */
import { expect, test } from "bun:test";
import { draftReads } from "../../src/draft-reads.ts";

test("a delayed clean refresh keeps keystrokes and the opened save base", async () => {
  const reads = draftReads();
  let draft = "opened text";
  let base = "opened version";
  let release!: () => void;

  const delayed = new Promise<void>((resolve) => {
    release = resolve;
  });

  const apply = reads.begin();

  const refresh = delayed.then(() =>
    apply(() => {
      draft = "remote text";
      base = "remote version";
    }),
  );

  reads.edited();
  draft = "typed during refresh";
  release();
  await refresh;
  expect(draft).toBe("typed during refresh");
  expect(base).toBe("opened version");

  // A later clean refresh is still allowed. An edit followed by undo invalidates it too.
  const clean = reads.begin();
  clean(() => {
    base = "next version";
  });
  expect(base).toBe("next version");
  const undone = reads.begin();
  reads.edited();
  reads.edited();
  undone(() => {
    base = "discarded version";
  });
  expect(base).toBe("next version");
});

/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise -- These browser regression fixtures exercise native delayed promises and AbortSignals. */
import { expect, test } from "vitest";
import { trackConnection } from "../../src/connection.ts";

for (const event of ["online", "visibilitychange"]) {
  test(`${event} retries the interrupted live read immediately`, async () => {
    const target = new EventTarget();
    let attempts = 0;
    let failed!: () => void;

    const offline = new Promise<void>((resolve) => {
      failed = resolve;
    });

    const fetcher = Object.assign(
      async () => {
        attempts++;

        if (attempts === 1) throw new TypeError("offline");

        return new Response("live");
      },
      { preconnect: globalThis.fetch.preconnect },
    );

    const connection = trackConnection(fetcher, {
      online: target,
      visibility: target,
      isVisible: () => true,
    });

    connection.subscribe(() => {
      if (!connection.isOnline()) failed();
    });
    const read = connection.fetch("http://localhost/streams/manifest?offset=known");
    await offline;
    target.dispatchEvent(new Event(event));
    expect(await (await read).text()).toBe("live");
    expect(attempts).toBe(2);
    expect(connection.isOnline()).toBe(true);
  });
}

test("closing the stream aborts its offline retry wait", async () => {
  const target = new EventTarget();
  let failed!: () => void;

  const offline = new Promise<void>((resolve) => {
    failed = resolve;
  });

  let attempts = 0;

  const fetcher = Object.assign(
    async () => {
      attempts++;
      throw new TypeError("offline");
    },
    { preconnect: globalThis.fetch.preconnect },
  );

  const connection = trackConnection(fetcher, {
    online: target,
    visibility: target,
    isVisible: () => true,
  });

  connection.subscribe(failed);
  const controller = new AbortController();
  const read = connection.fetch("http://localhost/streams/manifest", { signal: controller.signal });
  const ended = read.catch((error: Error) => error);
  await offline;
  controller.abort();
  expect(await ended).toBe(controller.signal.reason);
  target.dispatchEvent(new Event("online"));
  expect(attempts).toBe(1);
});

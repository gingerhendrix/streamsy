import { describe, expect, it } from "vitest";
import { Effect, Layer, Stream } from "effect";
import { BrowserCrypto } from "@effect/platform-browser";
import { FetchHttpClient } from "effect/http";
import { Response } from "effect/ai";
import {
  emptySnapshot,
  encodeEntry,
  finish,
  resumeState,
  Session,
} from "@streamsy/durable-response-contract";
import { Message } from "../src/message.ts";
import { init, update } from "../src/update.ts";
import { subscriptions } from "../src/subscription.ts";

const initial = () => init({ id: "one", reader: "reader-a", snapshot: emptySnapshot }).model;

const delta = encodeEntry(Response.makePart("text-delta", { id: "a", delta: "Hello" }));

describe("Foldkit response lifecycle", () => {
  it("folds a batch, drops without losing state, reconnects from the saved offset, then stops", () => {
    const first = update(
      initial(),
      Message.ReceivedBatch({ batch: { items: [delta], offset: "three", closed: false } }),
    );

    expect(first.model.snapshot.view.text).toBe("Hello");
    expect(first.commands).toHaveLength(1);
    const dropped = update(first.model, Message.ClickedDrop()).model;
    expect(subscriptions.response.modelToDependencies(dropped).active).toBe(false);

    const stale = update(
      dropped,
      Message.ReceivedBatch({ batch: { items: [delta], offset: "stale", closed: false } }),
    );

    expect(stale.model).toEqual(dropped);
    const reconnected = update(dropped, Message.ClickedReconnect()).model;
    expect(subscriptions.response.modelToDependencies(reconnected)).toEqual({
      id: "one",
      active: true,
      offset: "three",
    });
    expect(update(reconnected, Message.ClickedStop()).commands).toHaveLength(1);

    const stopped = update(
      reconnected,
      Message.ReceivedBatch({
        batch: {
          items: [encodeEntry(finish({ by: "reader-b", reason: "Enough", at: "now" }))],
          offset: "end",
          closed: true,
        },
      }),
    ).model;

    expect(stopped.snapshot.view.text).toBe("Hello");
    expect(stopped.snapshot.view.status).toContain("Stopped by reader-b");
    expect(subscriptions.response.modelToDependencies(stopped).active).toBe(false);
    expect(update(stopped, Message.ClickedStop()).commands).toBeUndefined();
    expect(update(stopped, Message.ReceivedStopReply({ alreadyClosed: true })).model.notice).toBe(
      "Already finished",
    );
  });

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest awaits the Foldkit Command at the test boundary.
  it("saves through its Command and initializes a reload from that complete checkpoint", async () => {
    const values = new Map<string, string>();

    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => {
        values.set(key, value);
      },
    };

    const received = update(
      initial(),
      Message.ReceivedBatch({ batch: { items: [delta], offset: "three", closed: false } }),
    );

    const command = received.commands?.[0];
    expect(command).toBeDefined();

    if (!command) throw Error("Missing Save command");
    await Effect.runPromise(
      command.effect.pipe(
        Effect.provide([
          Layer.succeed(Session, storage),
          FetchHttpClient.layer,
          BrowserCrypto.layerSecureRandom,
        ]),
      ),
    );

    const restored = init({
      id: "one",
      reader: "reader-a",
      snapshot: resumeState(storage, "one"),
    }).model;

    expect(restored.snapshot).toEqual(received.model.snapshot);
    expect(subscriptions.response.modelToDependencies(restored).offset).toBe("three");
  });

  // oxlint-disable-next-line effecttsgo/async-function -- Vitest awaits a subscription Effect at the test boundary.
  it("does not run a reader when dropped; bad wire data retains the checkpoint", async () => {
    const stopped = update(initial(), Message.ClickedDrop()).model;
    expect(
      await Effect.runPromise(
        Stream.runCollect(
          subscriptions.response.dependenciesToStream(
            subscriptions.response.modelToDependencies(stopped),
          ),
        ).pipe(
          Effect.provide([
            Layer.succeed(Session, { getItem: () => null, setItem: () => {} }),
            FetchHttpClient.layer,
            BrowserCrypto.layerSecureRandom,
          ]),
        ),
      ),
    ).toEqual([]);

    const invalid = update(
      initial(),
      Message.ReceivedBatch({
        batch: { items: [{ type: "text-delta" }], offset: "invalid", closed: false },
      }),
    ).model;

    expect(invalid.snapshot.offset).toBe("-1");
    expect(invalid.mode).toBe("dropped");
  });
});

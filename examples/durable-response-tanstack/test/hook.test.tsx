/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/new-promise, effecttsgo/global-timers -- React integration retains native HTTP and waits one turn for the hook's documented deferred disposal. */
import { afterEach, expect, it, vi } from "vitest";
import { act, useMemo, useState } from "react";
import { Window } from "happy-dom";
import { createRoot } from "react-dom/client";
import { flushSync } from "react-dom";
import { useChat } from "@tanstack/ai-react";
import { EventType } from "@tanstack/ai/client";
import { Array, Option, Schema } from "effect";
import { answer } from "../src/model.ts";
import { STOP_EVENT, Stopped } from "../src/contract.ts";
import { boot, read } from "./support.ts";

let dispose = async () => {};

let unmount = () => {};

afterEach(async () => {
  await act(async () => {
    unmount();
  });
  await dispose();
  // Native useChat intentionally defers client disposal with setTimeout(..., 0).
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  await act(async () => {});
  vi.unstubAllGlobals();
});

const dom = () => {
  const window = new Window();
  vi.stubGlobal("window", window);
  vi.stubGlobal("document", window.document);
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const container = document.createElement("div");
  document.body.append(container);

  return container;
};

it("real useChat sends once and a fresh hook/connection after reload reconstructs one full assistant message", async () => {
  const container = dom();
  const app = await boot(10);
  dispose = app.dispose;
  const id = "hook-reload";
  expect((await app.prepare(id)).status).toBe(201);

  const Hook = () => {
    const connection = useMemo(() => app.native(id), []);

    const { messages, sendMessage, sessionGenerating } = useChat({
      threadId: id,
      connection,
      live: true,
    });

    const assistants = Array.filter(messages, (message) => message.role === "assistant");

    const text = Array.join(
      Array.flatMap(assistants, (message) =>
        Array.flatMap(message.parts, (part) => (part.type === "text" ? [part.content] : [])),
      ),
      "",
    );

    return (
      <>
        <button onClick={() => void sendMessage("Explain durable streams")}>Start</button>
        <output data-count={assistants.length} data-generating={sessionGenerating}>
          {text}
        </output>
      </>
    );
  };

  const first = createRoot(container);
  unmount = () => flushSync(() => first.unmount());
  flushSync(() => first.render(<Hook />));
  container.querySelector("button")?.click();
  await vi.waitFor(
    () => {
      const text = container.querySelector("output")?.textContent ?? "";
      expect(text.length).toBeGreaterThan(20);
      expect(text.length).toBeLessThan(answer.length);
    },
    { interval: 5 },
  );
  unmount();
  const next = createRoot(container);
  unmount = () => flushSync(() => next.unmount());
  flushSync(() => next.render(<Hook />));
  await vi.waitFor(
    () => {
      expect(container.querySelector("output")?.textContent).toBe(answer);
      expect(container.querySelector("output")?.dataset["count"]).toBe("1");
      expect(container.querySelector("output")?.dataset["generating"]).toBe("false");
    },
    { interval: 5, timeout: 3000 },
  );
  expect(await app.stats()).toEqual({ calls: 1, interruptions: 0 });
});

it("native hook callbacks observe the stopped convention and cancelled terminal for live and late mounts", async () => {
  const container = dom();
  const app = await boot(15);
  dispose = app.dispose;
  const id = await app.start("hook-stop");

  const Hook = ({ reader }: { reader: string }) => {
    const connection = useMemo(() => app.native(id), []);
    const [stopped, setStopped] = useState<Stopped>();
    const [terminal, setTerminal] = useState("");

    const { messages } = useChat({
      threadId: id,
      connection,
      live: true,
      onCustomEvent: (name, data) => {
        if (name === STOP_EVENT)
          setStopped(Option.getOrUndefined(Schema.decodeUnknownOption(Stopped)(data)));
      },
      onChunk: (chunk) => {
        if (chunk.type === EventType.RUN_FINISHED) setTerminal(chunk.outcome?.type ?? "finished");
      },
    });

    const text = Array.join(
      Array.flatMap(messages, (message) =>
        Array.flatMap(message.parts, (part) => (part.type === "text" ? [part.content] : [])),
      ),
      "",
    );

    return (
      <output data-reader={reader} data-stop={JSON.stringify(stopped)} data-terminal={terminal}>
        {text}
      </output>
    );
  };

  const root = createRoot(container);
  unmount = () => flushSync(() => root.unmount());
  flushSync(() =>
    root.render(
      <>
        <Hook key="a" reader="a" />
        <Hook key="b" reader="b" />
      </>,
    ),
  );
  await read(`${app.base}/responses/${id}`, "-1", 6);
  const result = await app.post(`/api/chat/${id}/stop`, { by: "a", reason: "Enough" });
  expect(result.status).toBe(200);
  flushSync(() =>
    root.render(
      <>
        <Hook key="a" reader="a" />
        <Hook key="b" reader="b" />
        <Hook key="late" reader="late" />
      </>,
    ),
  );
  await vi.waitFor(
    () => {
      const outputs = Array.fromIterable(container.querySelectorAll("output"));
      expect(outputs).toHaveLength(3);
      expect(new Set(Array.map(outputs, (o) => o.textContent)).size).toBe(1);
      expect(new Set(Array.map(outputs, (o) => o.dataset["stop"])).size).toBe(1);

      for (const o of outputs) {
        expect(o.dataset["stop"]).toContain('"by":"a"');
        expect(o.dataset["terminal"]).toBe("cancelled");
      }
    },
    { interval: 5, timeout: 3000 },
  );
  expect(await app.stats()).toEqual({ calls: 1, interruptions: 1 });
});

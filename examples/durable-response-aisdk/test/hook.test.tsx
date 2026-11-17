/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- React hook integration owns native Promise and DOM lifetimes. */
import { afterEach, expect, it, vi } from "vitest";
import { act } from "react";
import { Window } from "happy-dom";
import { createRoot } from "react-dom/client";
import { flushSync } from "react-dom";
import { useChat } from "@ai-sdk/react";
import { Array } from "effect";
import { chunks } from "../src/model.ts";
import { boot } from "./support.ts";

let dispose = async () => {};

let unmount = () => {};

afterEach(async () => {
  await act(async () => {
    unmount();
  });
  await dispose();
  await act(async () => {});
  vi.unstubAllGlobals();
});

it("a fresh useChat mount after reload replays one complete answer without a second model call", async () => {
  // Install DOM only; keep Bun's real fetch/Request/Response for the in-process HTTP host.
  const window = new Window();
  vi.stubGlobal("window", window);
  vi.stubGlobal("document", window.document);
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const app = await boot(30);
  dispose = app.dispose;
  const id = await app.start("hook-reload");
  const transport = app.transport(id);

  const Hook = () => {
    const { messages, status } = useChat({ id, transport, resume: true });

    const text = Array.join(
      Array.flatMap(messages, (message) =>
        Array.flatMap(message.parts, (part) => (part.type === "text" ? [part.text] : [])),
      ),
      "",
    );

    return (
      <output data-status={status} data-count={messages.length}>
        {text}
      </output>
    );
  };

  const container = document.createElement("div");
  document.body.append(container);
  const first = createRoot(container);
  unmount = () => flushSync(() => first.unmount());
  flushSync(() => first.render(<Hook />));
  await vi.waitFor(
    () => {
      expect(container.textContent?.length).toBeGreaterThan(20);
      expect(container.textContent?.length).toBeLessThan(Array.join(chunks, "").length);
    },
    { interval: 5 },
  );
  unmount();
  // A real page reload discards hook state. The published transport replays from -1.
  const next = createRoot(container);
  unmount = () => flushSync(() => next.unmount());
  flushSync(() => next.render(<Hook />));
  await vi.waitFor(
    () => {
      expect(container.textContent).toBe(Array.join(chunks, ""));
      expect(container.querySelector("output")?.dataset["count"]).toBe("1");
      expect(container.querySelector("output")?.dataset["status"]).toBe("ready");
    },
    { interval: 5, timeout: 3000 },
  );
  const count = await app.raw(id);
  expect(count.closed).toBe("true");
  expect(await fetch(`${app.base}/debug/model-calls`).then((r) => r.json())).toEqual({
    calls: 1,
    interruptions: 0,
  });
});

/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/global-timers, effecttsgo/crypto-random-uuid -- React browser boundary uses native hooks, fetch, tab identity, and polling. */
import { useChat } from "@ai-sdk/react";
import { createDurableChatTransport } from "@durable-streams/aisdk-transport";
import { useEffect, useMemo, useState } from "react";
import type { UIMessage } from "ai";
import { Array, Option, Schema } from "effect";
import { Id, Metadata, Stats, Status, StopReply } from "./contract.ts";

type Message = UIMessage<typeof Metadata.Type>;

const routeId = () => window.location.pathname.match(/^\/r\/([a-zA-Z0-9_-]+)$/)?.[1];

const readerId = () => {
  const saved = sessionStorage.getItem("aisdk-reader");

  if (saved) return saved;
  const reader = `reader-${crypto.randomUUID().slice(0, 8)}`;
  sessionStorage.setItem("aisdk-reader", reader);

  return reader;
};

export const App = () => {
  const [id] = useState(() => Schema.decodeSync(Id)(routeId() ?? crypto.randomUUID()));
  const [started, setStarted] = useState(() => routeId() !== undefined);
  const [reader] = useState(readerId);
  const [prompt, setPrompt] = useState("Explain durable streams");
  const [notice, setNotice] = useState("");
  const [closed, setClosed] = useState(false);
  const [stats, setStats] = useState({ calls: 0, interruptions: 0 });

  const transport = useMemo(
    () =>
      createDurableChatTransport<Message>({
        api: "/api/chat",
        reconnectApi: `/api/chat/${id}/replay`,
      }),
    [id],
  );

  const { messages, sendMessage, status, error } = useChat<Message>({
    id,
    transport,
    resume: true,
  });

  const assistant = Option.getOrUndefined(
    Array.findFirst(messages, (message) => message.role === "assistant"),
  );

  // Validate application metadata before displaying it. Text is folded by useChat itself.
  const metadata = Schema.decodeOption(Metadata)(assistant?.metadata ?? {});
  const meta = Option.getOrUndefined(metadata);
  const stopped = meta?.stopped;

  const text = Array.join(
    Array.flatMap(assistant?.parts ?? [], (part) => (part.type === "text" ? [part.text] : [])),
    "",
  );

  useEffect(() => {
    let active = true;
    let timer: ReturnType<typeof setTimeout>;

    const poll = async () => {
      try {
        const [count, state] = await Promise.all([
          fetch("/debug/model-calls").then((response) => response.json()),
          fetch(`/api/chat/${id}/status`).then((response) => response.json()),
        ]);

        if (active) {
          setStats(Schema.decodeUnknownSync(Stats)(count));
          setClosed(Schema.decodeUnknownSync(Status)(state).closed);
        }
      } catch {
        if (active) setNotice("Debug status unavailable");
      }

      if (active) timer = setTimeout(poll, 500);
    };

    void poll();

    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [id]);

  const stop = async () => {
    try {
      const response = await fetch(`/api/chat/${id}/stop`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ by: reader, reason: "Stopped from the page" }),
      });

      if (!response.ok) throw Error(await response.text());
      const result = Schema.decodeUnknownSync(StopReply)(await response.json());
      setNotice(result.alreadyClosed ? "Already finished" : "Stop recorded for every reader");
      // Keep this read open: it must consume the durable abort and message-metadata chunks.
    } catch (cause) {
      setNotice(String(cause));
    }
  };

  return (
    <main className="shell">
      <header>
        <div>
          <div className="eyebrow">Streamsy · AI SDK</div>
          <h1>Durable response</h1>
        </div>
        <span className="reader">Reader: {reader}</span>
      </header>
      <p className="intro">One generation. Reload to replay. Stop for everyone.</p>
      <section className="card">
        {!started ? (
          <form
            onSubmit={(event) => {
              event.preventDefault();
              setStarted(true);
              window.history.replaceState(null, "", `/r/${id}`);
              void sendMessage({ text: prompt });
            }}
          >
            <label htmlFor="prompt">Prompt</label>
            <textarea
              id="prompt"
              value={prompt}
              onChange={(event) => setPrompt(event.target.value)}
            />
            <button className="primary" disabled={!prompt.trim()} type="submit">
              Start response
            </button>
          </form>
        ) : (
          <>
            <div className="prompt">Prompt: {meta?.prompt ?? prompt}</div>
            <div className="answer" data-testid="answer">
              {text || "Waiting for the first part…"}
            </div>
            <div className="status" role="status">
              {stopped
                ? `Stopped by ${stopped.by} at ${stopped.at} · ${stopped.reason}`
                : closed
                  ? "Finished"
                  : "Generating"}
            </div>
            <div className="controls">
              <button
                className="stop"
                disabled={closed || stopped !== undefined}
                onClick={() => void stop()}
              >
                Stop
              </button>
            </div>
            <div className="debug">
              <span>Hook: {status}</span>
              <span>Stream: {closed ? "closed" : "open"}</span>
              <span>Offset: not exposed by useChat</span>
              <span>Reload: replay from start (-1)</span>
              <span>Model calls: {stats.calls} (scripted)</span>
              <span>Provider interruptions: {stats.interruptions}</span>
            </div>
          </>
        )}
        <div className="notice" role="alert">
          {error?.message ?? notice}
        </div>
      </section>
      <footer>
        Native useChat · Published durable transport · SQLite log · <a href="/">New response</a>
        <br />
        Reload replays the recorded message. Stop aborts the provider and records the reason before
        closure.
      </footer>
    </main>
  );
};

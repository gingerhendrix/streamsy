/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/global-timers, effecttsgo/crypto-random-uuid -- React owns tab identity and native hook/HTTP lifetimes at the browser boundary. */
import { EventType } from "@tanstack/ai/client";
import { useChat } from "@tanstack/ai-react";
import { useEffect, useMemo, useState } from "react";
import { Array, Option, Schema } from "effect";
import { Id, RequestMetadata, Stats, Status, StopReply, Stopped, STOP_EVENT } from "./contract.ts";
import { connection } from "./connection.ts";

const routeId = () => window.location.pathname.match(/^\/r\/([a-zA-Z0-9_-]+)$/)?.[1];

const readerId = () => {
  const saved = sessionStorage.getItem("tanstack-reader");

  if (saved) return saved;
  const reader = `reader-${crypto.randomUUID().slice(0, 8)}`;
  sessionStorage.setItem("tanstack-reader", reader);

  return reader;
};

export const App = () => {
  const [id] = useState(() => Schema.decodeSync(Id)(routeId() ?? crypto.randomUUID()));
  const [started, setStarted] = useState(() => routeId() !== undefined);
  const [starting, setStarting] = useState(false);
  const [reader] = useState(readerId);
  const [prompt, setPrompt] = useState("Explain durable streams");
  const [recordedPrompt, setRecordedPrompt] = useState("");
  const [stopped, setStopped] = useState<Stopped>();
  const [ended, setEnded] = useState(false);
  const [events, setEvents] = useState(0);
  const [notice, setNotice] = useState("");
  const [closed, setClosed] = useState(false);
  const [stats, setStats] = useState({ calls: 0, interruptions: 0 });
  const nativeConnection = useMemo(() => connection(id), [id]);

  const { messages, sendMessage, status, connectionStatus, error } = useChat({
    threadId: id,
    connection: nativeConnection,
    live: started,
    onChunk: (chunk) => {
      setEvents((n) => n + 1);

      if (chunk.type === EventType.RUN_STARTED) {
        const metadata = Option.getOrUndefined(
          Schema.decodeUnknownOption(RequestMetadata)(chunk.metadata?.["durableResponse"]),
        );

        if (metadata) setRecordedPrompt(metadata.prompt);
      }

      if (chunk.type === EventType.RUN_FINISHED || chunk.type === EventType.RUN_ERROR)
        setEnded(true);
    },
    onCustomEvent: (name, value) => {
      if (name !== STOP_EVENT) return;
      const reason = Option.getOrUndefined(Schema.decodeUnknownOption(Stopped)(value));

      if (reason) setStopped(reason);
      else setNotice("Invalid stopped event");
    },
  });

  const text = Array.join(
    Array.flatMap(messages, (message) =>
      message.role === "assistant"
        ? Array.flatMap(message.parts, (part) => (part.type === "text" ? [part.content] : []))
        : [],
    ),
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

  const start = async () => {
    setStarting(true);

    try {
      const prepared = await fetch(`/api/chat/${id}/prepare`, { method: "POST" });

      if (!prepared.ok) throw Error(await prepared.text());
      window.history.replaceState(null, "", `/r/${id}`);
      setStarted(true);
      await sendMessage(prompt);
    } catch (cause) {
      setNotice(String(cause));
    } finally {
      setStarting(false);
    }
  };

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
      // Keep consuming: native stop() alone only detaches this client with this published connection.
    } catch (cause) {
      setNotice(String(cause));
    }
  };

  return (
    <main className="shell">
      <header>
        <div>
          <div className="eyebrow">Streamsy · TanStack AI</div>
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
              void start();
            }}
          >
            <label htmlFor="prompt">Prompt</label>
            <textarea
              id="prompt"
              value={prompt}
              onChange={(event) => setPrompt(event.target.value)}
            />
            <button className="primary" disabled={starting || !prompt.trim()} type="submit">
              {starting ? "Starting…" : "Start response"}
            </button>
          </form>
        ) : (
          <>
            <div className="prompt">Prompt: {recordedPrompt || prompt}</div>
            <div className="answer" data-testid="answer">
              {text || "Waiting for the first part…"}
            </div>
            <div className="status" role="status">
              {stopped
                ? `Stopped by ${stopped.by} at ${stopped.at} · ${stopped.reason}`
                : error
                  ? `Error: ${error.message}`
                  : ended
                    ? "Finished"
                    : "Generating"}
            </div>
            <div className="controls">
              <button
                className="stop"
                disabled={closed || ended || stopped !== undefined}
                onClick={() => void stop()}
              >
                Stop
              </button>
            </div>
            <div className="debug">
              <span>Hook: {status}</span>
              <span>
                Read: {connectionStatus} · {closed ? "closed" : "open"}
              </span>
              <span>Offset: not exposed by useChat</span>
              <span>Reload: full raw replay from start (-1)</span>
              <span>Events seen: {events}</span>
              <span>Model calls: {stats.calls} (scripted)</span>
              <span>Provider interruptions: {stats.interruptions}</span>
              <span>Stop reason: application CUSTOM convention</span>
            </div>
          </>
        )}
        <div className="notice" role="alert">
          {error?.message ?? notice}
        </div>
      </section>
      <footer>
        Native TanStack useChat · Published durable transport · SQLite log ·{" "}
        <a href="/">New response</a>
        <br />
        This example records a CUSTOM stopped event after native cancellation, followed by a
        cancelled RUN_FINISHED event.
      </footer>
    </main>
  );
};

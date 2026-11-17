import { useAtom, useAtomMount, useAtomSet, useAtomValue } from "@effect/atom-react";
import {
  busyAtom,
  callsAtom,
  modeAtom,
  noticeAtom,
  promptAtom,
  readAtom,
  readerIdAtom,
  responseIdAtom,
  resumedAtAtom,
  snapshotAtom,
} from "./state.ts";
import { startAtom, stopAtom } from "./writes.ts";

export const App = () => {
  useAtomMount(readAtom);
  const id = useAtomValue(responseIdAtom);
  const reader = useAtomValue(readerIdAtom);
  const [prompt, setPrompt] = useAtom(promptAtom);
  const [mode, setMode] = useAtom(modeAtom);
  const snapshot = useAtomValue(snapshotAtom);
  const resumedAt = useAtomValue(resumedAtAtom);
  const calls = useAtomValue(callsAtom);
  const notice = useAtomValue(noticeAtom);
  const busy = useAtomValue(busyAtom);
  const start = useAtomSet(startAtom);
  const stop = useAtomSet(stopAtom);

  return (
    <main className="shell">
      <header>
        <div>
          <div className="eyebrow">Streamsy · Effect Atom</div>
          <h1>Durable response</h1>
        </div>
        <span className="reader">Reader: {reader}</span>
      </header>
      <p className="intro">One generation. Resume anywhere. Stop for everyone.</p>
      <section className="card">
        {!id ? (
          <form
            onSubmit={(event) => {
              event.preventDefault();
              start();
            }}
          >
            <label htmlFor="prompt">Prompt</label>
            <textarea
              id="prompt"
              value={prompt}
              onChange={(event) => setPrompt(event.target.value)}
            />
            <button className="primary" disabled={busy || !prompt.trim()} type="submit">
              {busy ? "Starting…" : "Start response"}
            </button>
          </form>
        ) : (
          <>
            <div className="prompt">Prompt: {snapshot.view.prompt || "Loading…"}</div>
            <div className="answer" data-testid="answer">
              {snapshot.view.text || "Waiting for the first part…"}
            </div>
            <div className="status" role="status">
              {snapshot.view.status}
            </div>
            <div className="controls">
              <button className="stop" disabled={snapshot.view.terminal} onClick={() => stop()}>
                Stop
              </button>
              <button
                disabled={mode === "dropped" || snapshot.closed}
                onClick={() => setMode("dropped")}
              >
                Drop connection
              </button>
              <button
                disabled={mode === "connected" || snapshot.closed}
                onClick={() => setMode("connected")}
              >
                Reconnect
              </button>
            </div>
            <div className="debug">
              <span>Read: {snapshot.closed ? "ended" : mode}</span>
              <span>Offset: {snapshot.offset}</span>
              <span>Resumed at: {resumedAt}</span>
              <span>Entries: {snapshot.view.entries}</span>
              <span>Model calls: {calls.calls} (scripted)</span>
            </div>
          </>
        )}
        <div className="notice" role="alert">
          {notice}
        </div>
      </section>
      <footer>
        Scripted model · SQLite log · <a href="/">New response</a>
        <br />
        Drop disconnects this reader. Stop interrupts the provider and closes the response for all
        readers.
      </footer>
    </main>
  );
};

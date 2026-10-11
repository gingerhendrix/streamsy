import { useEffect, useState, useSyncExternalStore } from "react";
import { useLiveQuery } from "@tanstack/react-db";
import { createFileDb } from "./db.ts";
import type { FileDb } from "./db.ts";
import type { SearchResult } from "../server/search-result.ts";
import { search } from "./client.ts";
import type { Selection } from "./client.ts";
import { Tree, fileTree } from "./Tree.tsx";
import { Snippet } from "./Snippet.tsx";
import { FilePanel } from "./FilePanel.tsx";

function Browser({ db, loadMs }: { db: FileDb; loadMs: number }) {
  const online = useSyncExternalStore(db.connection.subscribe, db.connection.isOnline);

  const { data } = useLiveQuery((q) =>
    q.from({ file: db.collections.files }).orderBy(({ file }) => file.path, "asc"),
  );

  const [selection, setSelection] = useState<Selection | null>(null);
  const [query, setQuery] = useState("");
  const [submitted, setSubmitted] = useState("");
  const [searchRevision, setSearchRevision] = useState(0);
  const [hits, setHits] = useState<ReadonlyArray<SearchResult>>([]);
  const [error, setError] = useState("");
  const [searching, setSearching] = useState(false);

  useEffect(() => {
    if (!submitted) {
      setHits([]);

      return undefined;
    }

    const controller = new AbortController();
    setSearching(true);
    setError("");
    void search(submitted, controller.signal)
      .then(setHits)
      .catch((failure: Error) => {
        if (!controller.signal.aborted) setError(failure.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setSearching(false);
      });

    return () => controller.abort();
  }, [submitted, searchRevision]);

  const current = data.find(
    (file) => file.folderId === selection?.folderId && file.path === selection.path,
  );

  return (
    <main className="shell">
      <header>
        <a className="brand" href="/">
          ▧ <strong>Folder sync</strong>
          <span>Streamsy</span>
        </a>
        <span className={online ? "connection" : "connection offline"}>
          {online ? "Live" : "Offline"}
        </span>
      </header>
      <div className="workspace">
        <nav className="sidebar" aria-label="Folder tree">
          <div className="sidebar-heading">
            <h2>Files</h2>
            <span>{data.length}</span>
          </div>
          <p className="muted">Your files, with every version.</p>
          <Tree branch={fileTree(data)} selection={selection} open={setSelection} />
          <p className="load-time">Manifest loaded in {Math.round(loadMs)} ms</p>
        </nav>
        <div className="content">
          <form
            className="search-bar"
            onSubmit={(event) => {
              event.preventDefault();
              setSubmitted(query.trim());
              setSearchRevision((value) => value + 1);
            }}
          >
            <input
              aria-label="Search files"
              placeholder="Search file contents…"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
            <button type="submit">Search</button>
            {submitted && (
              <button
                type="button"
                onClick={() => {
                  setSubmitted("");
                  setQuery("");
                }}
              >
                Clear
              </button>
            )}
          </form>
          {submitted && (
            <section className="search-results" aria-label="Search results">
              <h2>{searching ? "Searching…" : `${hits.length} results for “${submitted}”`}</h2>
              {error && <p role="alert">{error}</p>}
              {hits.map((hit) => (
                <button
                  className="search-hit"
                  key={`${hit.folderId}/${hit.path}`}
                  onClick={() =>
                    setSelection({ folderId: hit.folderId, path: hit.path, at: hit.version })
                  }
                >
                  <strong>
                    {hit.folderId} / {hit.path}
                  </strong>
                  <span>
                    <Snippet parts={hit.snippet} />
                  </span>
                </button>
              ))}
            </section>
          )}
          {selection ? (
            <FilePanel
              key={JSON.stringify(selection)}
              selection={selection}
              headVersion={current?.version}
              open={setSelection}
            />
          ) : (
            <div className="welcome">
              <p className="eyebrow">DURABLE FOLDERS</p>
              <h1>
                A place for files.
                <br />A record of every change.
              </h1>
              <p>
                Choose a file to read, edit, or explore its history.
                <br />
                Search finds text across every folder.
              </p>
              {data.length === 0 && (
                <p>
                  Load the docs corpus with <code>bun run seed</code> to get started.
                </p>
              )}
            </div>
          )}
        </div>
      </div>
    </main>
  );
}

export function App() {
  const [db, setDb] = useState<FileDb | null>(null);
  const [loadMs, setLoadMs] = useState(0);
  const [error, setError] = useState("");
  useEffect(() => {
    let cancelled = false;
    const created = createFileDb();
    const started = performance.now();
    void created
      .preload()
      .then(() => {
        if (cancelled) return;
        setLoadMs(performance.now() - started);
        setDb(created);
      })
      .catch((failure: Error) => {
        if (!cancelled) setError(failure.message);
      });

    return () => {
      cancelled = true;
      created.close();
    };
  }, []);

  return db ? (
    <Browser db={db} loadMs={loadMs} />
  ) : (
    <main className="loading">
      <h1>Folder sync</h1>
      <p>{error || "Loading file manifest…"}</p>
    </main>
  );
}

/* oxlint-disable effecttsgo/async-function, effecttsgo/global-date, effecttsgo/global-fetch, effecttsgo/global-timers -- React owns this browser lifecycle edge; Web fetch, wall-clock rendering, and interval cleanup are the platform contract here. */
import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { createHnDb, type HnDb } from "./db.ts";
import { useLiveQuery } from "@tanstack/react-db";
import { Option, Schema } from "effect";
import { ApiStatus, type HnStory } from "../state-schema.ts";
import "./styles.css";

function formatAge(unixSeconds: number): string {
  const diff = Date.now() - unixSeconds * 1000;
  const minutes = Math.max(0, Math.round(diff / 60000));
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function storyHref(story: HnStory): string {
  return story.url ?? `https://news.ycombinator.com/item?id=${story.id}`;
}

function hnCommentsHref(story: HnStory): string {
  return `https://news.ycombinator.com/item?id=${story.id}`;
}

function App() {
  const [db, setDb] = useState<HnDb | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const created = createHnDb(window.location.origin);

    void created.preload().then(
      () => {
        if (cancelled) {
          created.close();
          return;
        }
        setDb(created);
      },
      (error) => {
        if (!cancelled) setLoadError(error instanceof Error ? error.message : String(error));
        created.close();
      },
    );

    return () => {
      cancelled = true;
      created.close();
    };
  }, []);

  if (loadError) {
    return (
      <Shell>
        <div className="panel error">Unable to load stream DB: {loadError}</div>
      </Shell>
    );
  }

  if (!db) {
    return (
      <Shell>
        <div className="panel loading">Loading durable stream database…</div>
      </Shell>
    );
  }

  return <HnApp db={db} />;
}

function HnApp({ db }: { db: HnDb }) {
  const storiesQuery = useLiveQuery((q) => q.from({ stories: db.collections.stories }));
  const stories = useMemo(
    () => (storiesQuery.data ?? []).toSorted((a, b) => b.time - a.time || b.id - a.id),
    [storiesQuery.data],
  );
  const [status, setStatus] = useState<ApiStatus | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function refresh() {
      const response = await fetch("/api/status");
      if (!response.ok) return;
      const next = Schema.decodeUnknownOption(ApiStatus)(await response.json());
      if (!cancelled && Option.isSome(next)) setStatus(next.value);
    }
    void refresh();
    const interval = setInterval(() => void refresh(), 5000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  return (
    <Shell status={status}>
      <section className="panel intro">
        <p className="eyebrow">Streamsy projection demo</p>
        <h1>Hacker News newest stories</h1>
        <p>
          A Bun server reconciles HN polls into a durable JSON source stream, and publishes the
          newest stories at <code>/state/newest</code>. This page loads the retained changes, then
          updates live as stories change.
        </p>
      </section>

      <section className="panel story-list" aria-live="polite">
        <div className="list-header">
          <h2>Newest {stories.length || ""}</h2>
          <span>{status?.polling ? "Polling HN…" : "Live stream connected"}</span>
        </div>
        {stories.length === 0 ? (
          <div className="empty">Waiting for the first projection batch…</div>
        ) : (
          stories.map((story) => <StoryRow key={story.id} story={story} />)
        )}
      </section>
    </Shell>
  );
}

function StoryRow({ story }: { story: HnStory }) {
  return (
    <article className="story-row">
      <div className="story-main">
        <a href={storyHref(story)} target="_blank" rel="noreferrer" className="story-title">
          {story.title}
        </a>
        <div className="story-meta">
          <span>{story.score ?? 0} points</span>
          {story.by ? <span>by {story.by}</span> : null}
          <span>{formatAge(story.time)}</span>
          <a href={hnCommentsHref(story)} target="_blank" rel="noreferrer">
            {story.descendants ?? 0} comments
          </a>
        </div>
      </div>
    </article>
  );
}

function Shell({ children, status }: { children: React.ReactNode; status?: ApiStatus | null }) {
  return (
    <main className="shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true">
            S
          </span>
          <span className="brand-name">Streamsy</span>
          <span className="brand-divider">/</span>
          <span className="brand-app">HN newest stream</span>
        </div>
        {status ? (
          <div className="status">
            <span>{status.lastStoryCount} rows</span>
            <span>{status.lastChangedStories + status.lastRemovedStories} changed</span>
            <span>poll {status.pollIntervalMs / 1000}s</span>
            {status.lastPollError || status.projection.lastError ? (
              <span className="bad">error</span>
            ) : (
              <span className="good">{status.projection.running ? "running" : "stopped"}</span>
            )}
          </div>
        ) : null}
      </header>
      {children}
    </main>
  );
}

const root = document.getElementById("root");
if (!root) throw new Error("Hacker News app root element is missing");
createRoot(root).render(<App />);

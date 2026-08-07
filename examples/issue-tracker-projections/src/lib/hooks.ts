/** React bindings over the durable feeds and the shareable URL. */
import { useCallback, useEffect, useRef, useState } from "react";
import { foldCollection } from "./state.ts";
import { subscribeToStream, type FeedStatus } from "./stream.ts";

export interface Feed<T> {
  readonly rows: ReadonlyMap<string, T>;
  readonly status: FeedStatus;
  readonly ready: boolean;
}

/**
 * Tail one durable State stream and fold a single collection out of it.
 * Reserved framework lineage rows in the same stream are ignored.
 */
export function useStateFeed<T>(streamName: string | null, collection: string): Feed<T> {
  const [feed, setFeed] = useState<Feed<T>>({
    rows: new Map(),
    status: "connecting",
    ready: false,
  });

  useEffect(() => {
    if (streamName === null) return;
    setFeed({ rows: new Map(), status: "connecting", ready: false });
    const controller = new AbortController();
    subscribeToStream(
      streamName,
      {
        onItems: (items) =>
          setFeed((previous) => ({
            ...previous,
            rows: foldCollection<T>(previous.rows, items, collection),
          })),
        onStatus: (status) => setFeed((previous) => ({ ...previous, status })),
        onReady: () => setFeed((previous) => ({ ...previous, ready: true })),
      },
      controller.signal,
    );
    return () => controller.abort();
  }, [streamName, collection]);

  return feed;
}

export interface WorkspaceLocation {
  readonly workspaceId: string;
  readonly projectId: string | null;
  readonly issueId: string | null;
}

function readLocation(): WorkspaceLocation {
  const params = new URLSearchParams(globalThis.location.search);
  return {
    workspaceId: params.get("workspace") ?? "main",
    projectId: params.get("project"),
    issueId: params.get("issue"),
  };
}

/** The URL is the shareable source of truth for workspace, project, and issue. */
export function useWorkspaceLocation(): readonly [
  WorkspaceLocation,
  (next: Partial<WorkspaceLocation>) => void,
] {
  const [location, setLocation] = useState<WorkspaceLocation>(readLocation);

  useEffect(() => {
    const onPop = () => setLocation(readLocation());
    globalThis.addEventListener("popstate", onPop);
    return () => globalThis.removeEventListener("popstate", onPop);
  }, []);

  const navigate = useCallback((next: Partial<WorkspaceLocation>) => {
    setLocation((current) => {
      const merged = { ...current, ...next };
      const params = new URLSearchParams();
      params.set("workspace", merged.workspaceId);
      if (merged.projectId !== null) params.set("project", merged.projectId);
      if (merged.issueId !== null) params.set("issue", merged.issueId);
      globalThis.history.replaceState(null, "", `?${params.toString()}`);
      return merged;
    });
  }, []);

  return [location, navigate];
}

/** A coarse clock for relative times and bounded optimistic overlays. */
export function useNow(intervalMs = 1_000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs]);
  return now;
}

/** Latest value without re-subscribing effects. */
export function useLatest<T>(value: T): { readonly current: T } {
  const ref = useRef(value);
  ref.current = value;
  return ref;
}

import {
  createCollection,
  localOnlyCollectionOptions,
  type Collection,
} from "@tanstack/db";
import type {
  BootstrapPayload,
  Comment,
  EntityType,
  Issue,
  Project,
  StateEvent,
} from "../types.ts";

export type DemoCollections = {
  projects: Collection<Project, string | number>;
  issues: Collection<Issue, string | number>;
  comments: Collection<Comment, string | number>;
};

export type DurableDbSnapshot = {
  projects: Project[];
  issues: Issue[];
  comments: Comment[];
  offset: string;
};

export type DurableDb = DemoCollections & {
  start: () => Promise<void>;
  stop: () => void;
  getOffset: () => string;
  getSnapshot: () => DurableDbSnapshot;
  subscribe: (listener: () => void) => () => void;
};

type CollectionFor<T extends EntityType> = T extends "project"
  ? Collection<Project, string | number>
  : T extends "issue"
    ? Collection<Issue, string | number>
    : Collection<Comment, string | number>;

function createCollections(seed: BootstrapPayload): DemoCollections {
  return {
    projects: createCollection(
      localOnlyCollectionOptions<Project>({
        id: "projects",
        getKey: (project) => project.id,
        initialData: seed.projects,
      }),
    ),
    issues: createCollection(
      localOnlyCollectionOptions<Issue>({
        id: "issues",
        getKey: (issue) => issue.id,
        initialData: seed.issues,
      }),
    ),
    comments: createCollection(
      localOnlyCollectionOptions<Comment>({
        id: "comments",
        getKey: (comment) => comment.id,
        initialData: seed.comments,
      }),
    ),
  };
}

function collectionFor<T extends EntityType>(
  collections: DemoCollections,
  type: T,
): CollectionFor<T> {
  if (type === "project") return collections.projects as CollectionFor<T>;
  if (type === "issue") return collections.issues as CollectionFor<T>;
  return collections.comments as CollectionFor<T>;
}

function applyEvent(collections: DemoCollections, event: StateEvent): void {
  const collection = collectionFor(collections, event.type) as Collection<any, string | number>;
  const operation = event.headers.operation;

  if (operation === "delete") {
    if (collection.has(event.key)) collection.delete(event.key);
    return;
  }

  if (!event.value) return;

  if (collection.has(event.key)) {
    collection.update(event.key, (draft) => {
      Object.assign(draft, event.value);
    });
  } else {
    collection.insert(event.value);
  }
}

async function readEvents(
  streamUrl: string,
  offset: string,
  signal: AbortSignal,
): Promise<{ events: StateEvent[]; nextOffset: string }> {
  const response = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}&live=long-poll`, { signal });
  const nextOffset = response.headers.get("stream-next-offset") ?? offset;

  if (response.status === 204) {
    return { events: [], nextOffset };
  }
  if (!response.ok) {
    throw new Error(`stream read failed: ${response.status} ${await response.text()}`);
  }

  return {
    events: (await response.json()) as StateEvent[],
    nextOffset,
  };
}

export async function createDurableDb(): Promise<DurableDb> {
  const seed = (await fetch("/api/bootstrap").then((r) => r.json())) as BootstrapPayload;
  const collections = createCollections(seed);
  let currentOffset = seed.nextOffset;
  let currentSnapshot: DurableDbSnapshot = {
    projects: collections.projects.toArray as Project[],
    issues: collections.issues.toArray as Issue[],
    comments: collections.comments.toArray as Comment[],
    offset: currentOffset,
  };
  let running = false;
  let stopped = false;
  let streamController: AbortController | null = null;
  const listeners = new Set<() => void>();

  const updateSnapshot = () => {
    currentSnapshot = {
      projects: collections.projects.toArray as Project[],
      issues: collections.issues.toArray as Issue[],
      comments: collections.comments.toArray as Comment[],
      offset: currentOffset,
    };
  };

  const notify = () => {
    for (const listener of listeners) listener();
  };

  const onCollectionChange = () => {
    updateSnapshot();
    notify();
  };

  const subscriptions = [
    collections.projects.subscribeChanges(onCollectionChange, { includeInitialState: false }),
    collections.issues.subscribeChanges(onCollectionChange, { includeInitialState: false }),
    collections.comments.subscribeChanges(onCollectionChange, { includeInitialState: false }),
  ];

  async function start() {
    if (running || stopped) return;
    running = true;
    try {
      while (!stopped) {
        streamController = new AbortController();
        try {
          const { events, nextOffset } = await readEvents(seed.streamUrl, currentOffset, streamController.signal);
          for (const event of events) applyEvent(collections, event);
          currentOffset = nextOffset;
          updateSnapshot();
          notify();
        } catch (error) {
          if (stopped && error instanceof DOMException && error.name === "AbortError") return;
          console.error(error);
          await new Promise((resolve) => setTimeout(resolve, 1000));
        } finally {
          streamController = null;
        }
      }
    } finally {
      running = false;
    }
  }

  return {
    ...collections,
    start,
    stop: () => {
      if (stopped) return;
      stopped = true;
      streamController?.abort();
      for (const subscription of subscriptions) subscription.unsubscribe();
      listeners.clear();
    },
    getOffset: () => currentOffset,
    getSnapshot: () => currentSnapshot,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

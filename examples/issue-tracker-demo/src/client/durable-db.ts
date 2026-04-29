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

export type DurableDb = DemoCollections & {
  start: () => Promise<void>;
  stop: () => void;
  getOffset: () => string;
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
): Promise<{ events: StateEvent[]; nextOffset: string }> {
  const response = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}&live=long-poll`);
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
  let running = false;
  let abort = false;
  const listeners = new Set<() => void>();

  const notify = () => {
    for (const listener of listeners) listener();
  };

  const subscriptions = [
    collections.projects.subscribeChanges(notify, { includeInitialState: false }),
    collections.issues.subscribeChanges(notify, { includeInitialState: false }),
    collections.comments.subscribeChanges(notify, { includeInitialState: false }),
  ];

  async function start() {
    if (running) return;
    running = true;
    abort = false;
    while (!abort) {
      try {
        const { events, nextOffset } = await readEvents(seed.streamUrl, currentOffset);
        for (const event of events) applyEvent(collections, event);
        currentOffset = nextOffset;
        notify();
      } catch (error) {
        console.error(error);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  return {
    ...collections,
    start,
    stop: () => {
      abort = true;
      for (const subscription of subscriptions) subscription.unsubscribe();
    },
    getOffset: () => currentOffset,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

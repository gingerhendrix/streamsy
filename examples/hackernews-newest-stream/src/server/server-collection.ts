import { createCollection, type Collection } from "@tanstack/db";
import type { ChangeMessageOrDeleteKeyMessage } from "@tanstack/db";

type SyncOps<T extends object, TKey extends string | number> = {
  begin: (options?: { immediate?: boolean }) => void;
  write: (message: ChangeMessageOrDeleteKeyMessage<T, TKey>) => void;
  commit: () => void;
  markReady: () => void;
};

export type ServerCollectionWriter<T extends object, TKey extends string | number> = {
  upsert(value: T): void;
  upsertMany(values: T[]): void;
  delete(key: TKey): void;
};

export function createServerCollection<T extends object, TKey extends string | number>(config: {
  id: string;
  getKey: (value: T) => TKey;
}): { collection: Collection<T, TKey>; writer: ServerCollectionWriter<T, TKey> } {
  let ops: SyncOps<T, TKey> | undefined;
  const known = new Set<TKey>();

  const collection = createCollection<T, TKey>({
    id: config.id,
    getKey: config.getKey,
    sync: {
      rowUpdateMode: "full",
      sync: ({ begin, write, commit, markReady }) => {
        ops = { begin, write, commit, markReady };
        markReady();
        return () => {
          ops = undefined;
        };
      },
    },
    startSync: true,
    gcTime: 0,
  });

  function requireOps() {
    if (!ops) {
      collection.startSyncImmediate();
    }
    if (!ops) throw new Error(`collection ${config.id} sync not started`);
    return ops;
  }

  const writer: ServerCollectionWriter<T, TKey> = {
    upsert(value) {
      writer.upsertMany([value]);
    },
    upsertMany(values) {
      if (values.length === 0) return;
      const active = requireOps();
      active.begin({ immediate: true });
      for (const value of values) {
        const key = config.getKey(value);
        active.write({ type: known.has(key) ? "update" : "insert", value });
        known.add(key);
      }
      active.commit();
    },
    delete(key) {
      const active = requireOps();
      active.begin({ immediate: true });
      active.write({ type: "delete", key });
      known.delete(key);
      active.commit();
    },
  };

  return { collection, writer };
}

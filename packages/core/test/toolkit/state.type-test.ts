/** Compile-time contract for Durable State collections. */
import { Array, Schema } from "effect";
import * as DurableState from "../../src/toolkit/durable-state.ts";
import * as StreamAddress from "../../src/toolkit/address.ts";

const Entry = Schema.Struct({
  id: Schema.String,
  metadata: Schema.Struct({ source: Schema.String }),
});

const Note = Schema.Struct({ noteId: Schema.Finite, text: Schema.String });

const workspace = StreamAddress.state("entries", {
  collections: {
    entry: { schema: Entry, key: "id" },
    note: { schema: Note, key: "noteId" },
  },
});

DurableState.changes(workspace, { offset: "0" }, [
  DurableState.upsert("entry", { id: "a", metadata: { source: "s" } }),
  DurableState.remove("note", 1, { oldValue: { noteId: 1, text: "t" } }),
  DurableState.remove("entry", "a"),
]);

DurableState.changes(workspace, {}, [DurableState.upsert("note", { noteId: 2, text: "u" })]);

const change: DurableState.Change<typeof workspace.collections> = DurableState.remove("note", 1);

export const tag: "Upsert" | "Delete" = change._tag;

// @ts-expect-error the old value must match the named collection's schema
DurableState.changes(workspace, {}, [DurableState.remove("note", 1, { oldValue: { id: "a" } })]);

// @ts-expect-error the collection name is required
DurableState.upsert({ noteId: 1, text: "t" });

// @ts-expect-error state keys must decode to strings or numbers
StreamAddress.state("entries", { collections: { entry: { schema: Entry, key: "metadata" } } });

// @ts-expect-error the key must be a field of the same collection's value
StreamAddress.state("entries", { collections: { note: { schema: Note, key: "id" } } });

// @ts-expect-error the change type must be a declared collection
DurableState.changes(workspace, { offset: "0" }, [DurableState.upsert("comment", { id: "c" })]);

const noteWithoutFields = DurableState.upsert("note", { id: "a" });

// @ts-expect-error the value must match the named collection's schema
DurableState.changes(workspace, { offset: "0" }, [noteWithoutFields]);

// State addresses carry changes and controls.
type Item = Schema.Schema.Type<typeof workspace.codec>;

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Assert<T extends true> = T;

export type ItemChecks = [
  Assert<Same<Item, StreamAddress.CollectionsEvent<typeof workspace.collections>>>,
  Assert<
    Same<
      Item,
      StreamAddress.CollectionsChange<typeof workspace.collections> | StreamAddress.StateControl
    >
  >,
  Assert<Same<Extract<Item, StreamAddress.StateControl>, StreamAddress.StateControl>>,
];

export const describe = (item: Item): string => {
  if (StreamAddress.isControl(item)) {
    const control: StreamAddress.StateControlName = item.headers.control;

    // @ts-expect-error a control has no collection type
    const missing: unknown = item.type;

    return `${control}:${String(missing)}`;
  }

  const type: "entry" | "note" = item.type;
  const key: string = item.key;
  const operation: "upsert" | "delete" = item.headers.operation;

  return `${type}:${key}:${operation}`;
};

export const changesOnly = (items: ReadonlyArray<Item>) => {
  const changes = Array.filter(items, StreamAddress.isChange);
  const first: "entry" | "note" | undefined = changes[0]?.type;

  return first;
};

declare const item: Item;

// @ts-expect-error a change needs its `type` narrowed away from controls first
export const unnarrowed = item.key;

const mixed: ReadonlyArray<StreamAddress.CollectionsEvent<typeof workspace.collections>> =
  DurableState.changes(workspace, { offset: "0" }, [
    StreamAddress.reset(),
    DurableState.upsert("note", { noteId: 3, text: "v" }),
    StreamAddress.snapshotStart({ offset: "0" }),
    StreamAddress.snapshotEnd(),
  ]);

export const entries: ReadonlyArray<DurableState.Entry<typeof workspace.collections>> = [
  StreamAddress.reset(),
  DurableState.remove("entry", "a"),
];

export { mixed };

// @ts-expect-error a control name must be one of the three protocol controls
export const truncate: StreamAddress.StateControl = { headers: { control: "truncate" } };

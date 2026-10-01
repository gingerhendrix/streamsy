/** Compile-time contract for Durable State collections. */
import { Schema } from "effect";
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

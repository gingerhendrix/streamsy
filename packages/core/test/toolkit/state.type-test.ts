/** Compile-time contract for Durable State collections. */
import { Schema } from "effect";
import * as State from "../../src/toolkit/state.ts";
import * as StreamRef from "../../src/toolkit/ref.ts";

const Entry = Schema.Struct({
  id: Schema.String,
  metadata: Schema.Struct({ source: Schema.String }),
});
const Note = Schema.Struct({ noteId: Schema.Finite, text: Schema.String });

const workspace = StreamRef.state("entries", {
  collections: {
    entry: { schema: Entry, key: "id" },
    note: { schema: Note, key: "noteId" },
  },
});

State.changes(workspace, { offset: "0" }, [
  State.upsert("entry", { id: "a", metadata: { source: "s" } }),
  State.delete("note", { noteId: 1, text: "t" }),
]);

// @ts-expect-error state keys must decode to strings or numbers
StreamRef.state("entries", { collections: { entry: { schema: Entry, key: "metadata" } } });

// @ts-expect-error the key must be a field of the same collection's value
StreamRef.state("entries", { collections: { note: { schema: Note, key: "id" } } });

// @ts-expect-error the change type must be a declared collection
State.changes(workspace, { offset: "0" }, [State.upsert("comment", { id: "c" })]);

const noteWithoutFields = State.upsert("note", { id: "a" });
// @ts-expect-error the value must match the named collection's schema
State.changes(workspace, { offset: "0" }, [noteWithoutFields]);

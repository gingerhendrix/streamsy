/**
 * Compile-time contract for `StreamRoute`.
 *
 * `Params` must be inferred from the codec record, and the template's `:name`
 * set must equal the codec record's key set. `bun test` does not collect this
 * file, so every assertion below is checked by `tsc` alone.
 */
import { Schema } from "effect";
import type { Params } from "./route.ts";
import * as StreamRoute from "./route.ts";

const Entry = Schema.Struct({ text: Schema.String });
const codecs = { user: Schema.String };
type Codecs = typeof codecs;

const inferred: Params<Codecs> = { user: "ann" };
void inferred;
// @ts-expect-error the parameter type comes from the codec, not from the caller
const wrongType: Params<Codecs> = { user: 7 };
void wrongType;
// @ts-expect-error the parameter set is exactly the codec record's key set
const wrongKey: Params<Codecs> = { account: "ann" };
void wrongKey;

const numbers = { orderId: Schema.NumberFromString };
const numeric: Params<typeof numbers> = { orderId: 7 };
void numeric;
// @ts-expect-error a numeric codec decodes to a number, not a string
const numericText: Params<typeof numbers> = { orderId: "7" };
void numericText;

const two = { user: Schema.String, kind: Schema.String };
const both: Params<typeof two> = { user: "ann", kind: "note" };
void both;

// A matching template and codec record is accepted.
StreamRoute.json("journal/:user", { params: { user: Schema.String }, schema: Entry });

// @ts-expect-error the template names a parameter that the codec record omits
StreamRoute.json("journal/:user", { params: { account: Schema.String }, schema: Entry });

// @ts-expect-error the codec record carries a parameter that the template omits
StreamRoute.json("journal/:user", {
  params: { user: Schema.String, extra: Schema.String },
  schema: Entry,
});

// @ts-expect-error a two-parameter template needs both codecs
StreamRoute.json("journal/:user/:kind", { params: { user: Schema.String }, schema: Entry });

StreamRoute.bytes("uploads/:name", { params: { name: Schema.String }, contentType: "image/png" });

// @ts-expect-error the bytes template names a parameter that the codec record omits
StreamRoute.bytes("uploads/:name", { params: { account: Schema.String } });

// A widened string template cannot be checked, so the declaration is accepted.
const widened: string = "journal/:user";
StreamRoute.json(widened, { params: { user: Schema.String }, schema: Entry });

// The decoded parameter type flows into `ref`, so a route builds its own ref shape.
const journal = StreamRoute.json("journal/:user", {
  params: { user: Schema.String },
  schema: Entry,
});
const built = journal.ref({ user: "ann" });
void built;
// @ts-expect-error the ref constructor takes the decoded parameter set
journal.ref({ account: "ann" });

import { Schema } from "effect";
import { Http, Streamsy } from "@streamsy/core";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { StreamsyObject } from "../../../src/cloudflare/object.ts";

class Notes extends Streamsy.json<Notes>()("ObjectTypes/Notes", "notes", {
  params: {},
  schema: Schema.String,
}) {}
class Other extends Streamsy.bytes<Other>()("ObjectTypes/Other", "other", { params: {} }) {}
const group = Streamsy.group(Notes, Other);
const app = Http.routes(group, { access: "full" });
const object = StreamsyObject.make<unknown, Streamsy.Services<typeof group>>({
  app,
  layer: (state) => DurableObjectStorage.layer(group, { client: { storage: state.storage } }),
});
const missing = StreamsyObject.make<unknown, Streamsy.Services<typeof group>>({
  app,
  // @ts-expect-error Storage and Notes do not provide the Other stream required by the object.
  layer: (state) => DurableObjectStorage.layer(Notes, { client: { storage: state.storage } }),
});
void [object, missing];

const defaultRequirements = StreamsyObject.make<unknown>({
  // @ts-expect-error Specifying only Env uses R = never; stream requirements must be named too.
  app,
  layer: (state) => DurableObjectStorage.layer(group, { client: { storage: state.storage } }),
});
void defaultRequirements;

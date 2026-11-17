import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Id } from "@streamsy/durable-response-contract";

export class Responses extends Streamsy.json<Responses>()(
  "DurableResponse/Responses",
  "responses/:id",
  {
    params: { id: Id },
    schema: Schema.Json,
  },
) {}

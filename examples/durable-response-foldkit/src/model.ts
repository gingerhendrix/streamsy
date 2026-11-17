import { Schema } from "effect";
import { ResumeSnapshot } from "@streamsy/durable-response-contract";

export const Model = Schema.Struct({
  id: Schema.String,
  reader: Schema.String,
  prompt: Schema.String,
  snapshot: ResumeSnapshot,
  resumedAt: Schema.String,
  mode: Schema.Literals(["connected", "dropped", "ended"]),
  calls: Schema.Int,
  notice: Schema.String,
  busy: Schema.Boolean,
});

export type Model = typeof Model.Type;

export const Flags = Schema.Struct({
  id: Schema.String,
  reader: Schema.String,
  snapshot: ResumeSnapshot,
});

export type Flags = typeof Flags.Type;

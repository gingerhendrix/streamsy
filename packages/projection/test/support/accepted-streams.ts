import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";

export class Source extends Streamsy.json<Source>()("AcceptedWorker/Source", "source/:id", {
  params: { id: Schema.String },
  schema: Schema.Finite,
}) {}

export class Accepted extends Streamsy.wake<Accepted>()("AcceptedWorker/Accepted", "accepted/:id", {
  params: { id: Schema.String },
}) {}

export const group = Streamsy.group(Source, Accepted);

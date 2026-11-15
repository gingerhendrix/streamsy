import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";

export const Timer = Schema.Struct({
  id: Schema.String,
  dueAt: Schema.Finite,
  cron: Schema.NullOr(Schema.String),
});

export class Timers extends Streamsy.state<Timers>()("Timers/Rows", "timers", {
  params: {},
  collections: { timer: { schema: Timer, key: "id" } },
}) {}

export class Fired extends Streamsy.json<Fired>()("Timers/Fired", "timer-fired", {
  params: {},
  schema: Schema.Struct({ id: Schema.String, dueAt: Schema.Finite, firedAt: Schema.Finite }),
}) {}

export class Accepted extends Streamsy.wake<Accepted>()("Timers/Accepted", "timer-accepted", {
  params: {},
}) {}

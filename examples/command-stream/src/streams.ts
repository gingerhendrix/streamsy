import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";

export const Request = Schema.Struct({ key: Schema.String, text: Schema.String });

export type Request = typeof Request.Type;

export class Commands extends Streamsy.json<Commands>()("Command/Requests", "commands", {
  params: {},
  schema: Request,
}) {}

export class CommandChanges extends Streamsy.wake<CommandChanges>()(
  "Command/Changes",
  "command-changes",
  {
    params: {},
  },
) {}

export class Accepted extends Streamsy.wake<Accepted>()("Command/Accepted", "command-accepted", {
  params: {},
}) {}

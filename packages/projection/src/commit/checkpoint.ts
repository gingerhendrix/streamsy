import { Effect } from "effect";
import { make, type Folded } from "./model.ts";
import { CheckpointStore } from "../checkpoint-store.ts";
import { load, remove, retire, settle } from "./internal.ts";

export const checkpoint = make<Folded<unknown>, never, CheckpointStore>({
  name: "checkpoint",
  guarantee: "exactly-once",
  load,
  remove,
  retire,
  apply: (pending) => Effect.flatMap(pending.run, (result) => settle(pending, result)),
});

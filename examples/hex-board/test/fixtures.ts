import { Schema } from "effect";
import { GameEvent } from "../src/domain/events.ts";
import full from "./fixtures/full.json";
import long from "./fixtures/long.json";

const decode = Schema.decodeUnknownSync(Schema.Array(GameEvent));
export const fullGame = decode(full);
export const longGame = decode(long);

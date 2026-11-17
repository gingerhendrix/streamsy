import { BrowserCrypto } from "@effect/platform-browser";
import { Layer } from "effect";
import { Http } from "foldkit";
import { sessionLayer } from "@streamsy/durable-response-contract";

export const resources = Layer.mergeAll(Http.layer, BrowserCrypto.layerSecureRandom, sessionLayer);

export type Resources = Layer.Success<typeof resources>;

import { BrowserCrypto, BrowserKeyValueStore, Clipboard } from "@effect/platform-browser";
import { Layer } from "effect";
import { Http } from "foldkit";

/**
 * The browser services that Commands and the Flags use. The Foldkit runtime
 * builds this Layer once and provides it to every Command.
 */
export const resources = Layer.mergeAll(
  Http.layer,
  BrowserCrypto.layer,
  BrowserKeyValueStore.layerLocalStorage,
  Clipboard.layer,
);

export type Resources = Layer.Success<typeof resources>;

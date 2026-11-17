import { BrowserCrypto } from "@effect/platform-browser";
import { Context, Layer, type Stream } from "effect";
import { FetchHttpClient } from "effect/http";
import { Atom } from "effect/reactivity";
import { sessionLayer, streamReader, type ReadBatch } from "@streamsy/durable-response-contract";

export class ResponseReader extends Context.Service<
  ResponseReader,
  {
    readonly read: (id: string, offset: string) => Stream.Stream<ReadBatch, string>;
  }
>()("DurableResponseAtom/Reader") {}

export const atomRuntime = Atom.runtime(
  Layer.mergeAll(
    FetchHttpClient.layer,
    BrowserCrypto.layerSecureRandom,
    sessionLayer,
    Layer.succeed(ResponseReader, { read: streamReader }),
  ),
);

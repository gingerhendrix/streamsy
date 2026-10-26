import { Array, Context, Effect, Layer, Option } from "effect";
import { StorageFault, type Streamsy } from "@streamsy/core";
import type { LinkClient } from "./object-link.ts";

const clashMessage = (stream: Streamsy.Any) =>
  `${stream.key} is linked to another object, but this object's layer also provides it. ` +
  "Remove it from the storage group.";

/**
 * Puts the link layers under the object's own layer and beside it. The own
 * layer sees the linked services at start, and the object sees them at run
 * time. An own layer that also provides a linked stream fails with a
 * `StorageFault`, because its rows would go to a local stream that no
 * client reads.
 */
export function withLinks<Out, E, In, L>(
  own: Layer.Layer<Out, E, In>,
  links: ReadonlyArray<LinkClient<L>> | undefined,
): Layer.Layer<Out | L, E | StorageFault, Exclude<In, L>> {
  const clients = links ?? [];
  const streams = Array.flatMap(clients, (link) => link.streams);

  // One layer value, so the build below shares a single instance of it.
  const linked = Layer.mergeAll(Layer.empty, ...Array.map(clients, (link) => link.layer));

  // `provide` keeps the linked services out of the own layer's output, so a
  // linked key in that output came from the own layer itself.
  const checked = Layer.provide(own, linked).pipe(
    Layer.tap((local: Context.Context<Out>) =>
      Option.match(
        Array.findFirst(streams, (stream) => Option.isSome(Context.getOption(local, stream))),
        {
          onNone: () => Effect.void,
          onSome: (stream) =>
            Effect.fail(
              new StorageFault({
                operation: "streamsy.links",
                message: clashMessage(stream),
                retryable: false,
              }),
            ),
        },
      ),
    ),
  );

  return Layer.merge(checked, linked);
}

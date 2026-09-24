import { Context, Effect } from "effect";
import type { Reader, Writer } from "../protocol/tags.ts";
import type { StreamsFault } from "../fault.ts";
import type { Storage } from "../storage/storage.ts";

/** Private bridge: a family guard dies with this sentinel; the fused runner translates it. */
export class ForeignOwner extends Error {
  constructor(readonly stream: string) {
    super(`Family write to ${stream} does not share the checkpoint owner`);
  }
}
export const CurrentOwner = Context.Reference<object | undefined>(
  "@streamsy/core/internal/CurrentOwner",
  { defaultValue: () => undefined },
);
type Owner = (typeof Storage.Service)["owner"];
export const checkWriteOwner = (owner: Owner, id: string) =>
  Effect.flatMap(CurrentOwner, (expected) =>
    expected === undefined || expected === owner ? Effect.void : Effect.die(new ForeignOwner(id)),
  );

// Metadata stays attached to the actual protocol wire, not an unrelated ambient Storage.
const Metadata = Symbol.for("@streamsy/core/internal/StorageOwner");
export const attachStorage = <A extends object>(wire: A, storage: typeof Storage.Service): A =>
  Object.assign(wire, { [Metadata]: storage });
export const storageOf = (
  wire: Reader<StreamsFault> | Writer<StreamsFault>,
): typeof Storage.Service | undefined => {
  if (!(Metadata in wire)) return undefined;
  // SAFETY: only attachStorage writes this private protocol metadata.
  return wire[Metadata] as typeof Storage.Service;
};

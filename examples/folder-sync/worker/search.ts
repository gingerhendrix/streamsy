import { StreamAddress } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";
import { Effect, Option } from "effect";
import { indexFolder } from "../server/index-live.ts";
import { Files, FolderWake } from "../server/streams.ts";

/** Advance the wake cursor only after every affected folder's index has committed. */
export const wakeSearch = Projection.process({
  id: "wake-search",
  params: {},
  inputs: { wake: FolderWake },
  commit: Commit.transaction,
  prepare: (batch) =>
    Effect.gen(function* () {
      const folders = new Set<string>();

      for (const change of batch.wake.items) {
        if (!StreamAddress.isChange(change)) continue;
        const parsed = Files.parse(change.key);

        if (Option.isSome(parsed)) folders.add(parsed.value.folderId);
      }

      for (const folderId of folders) yield* indexFolder(folderId);
    }),
  process: () => Effect.void,
});

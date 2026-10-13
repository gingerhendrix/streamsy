import { Effect } from "effect";
import { InvalidFile, normalisePath } from "../server/domain.ts";

/** Both filesystem clients use relative, disjoint folder roots outside their state directory. */
export const folderRoot = (folderId: string, mapped: string, roots: ReadonlyMap<string, string>) =>
  Effect.gen(function* () {
    const relative = yield* normalisePath(mapped);

    if (relative === ".folder-sync" || relative.startsWith(".folder-sync/"))
      return yield* new InvalidFile({
        message: "Folder mapping uses reserved .folder-sync directory",
      });

    for (const [id, existing] of roots) {
      if (
        relative === existing ||
        relative.startsWith(`${existing}/`) ||
        existing.startsWith(`${relative}/`)
      )
        return yield* new InvalidFile({
          message: `Overlapping folder directories: ${id} and ${folderId}. Supply disjoint folderDirectory mappings.`,
        });
    }

    return relative;
  });

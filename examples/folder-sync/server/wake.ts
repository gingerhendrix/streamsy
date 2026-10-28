import { Projection } from "@streamsy/projection";
import { Files, FolderKey, FolderWake } from "./streams.ts";

/** Each folder's `Files` writes give one upsert in the deployment's one wake stream. */
export const filesWake = Projection.wake({
  id: "files-wake",
  params: { folderId: FolderKey },
  input: Files,
  output: FolderWake,
});

/**
 * Run `filesWake` after each local `Files` commit. Build it with the storage
 * layer, so that it subscribes before the first write. The pending mark lets a
 * restart produce a wake row that a stop left behind.
 */
export const WakeLive = Projection.attach(filesWake, { durable: true });

import { StreamAddress } from "@streamsy/core";
import { Array, Option, Schema } from "effect";
import { FileValue, isTombstone, type File } from "./domain.ts";
import { Folder } from "./streams.ts";

export const FileChange = StreamAddress.stateChange({ type: "file", schema: FileValue });

export type FileChange = typeof FileChange.Type;

export const FolderChange = StreamAddress.stateChange({ type: "folder", schema: Folder });

export type FolderChange = typeof FolderChange.Type;

/**
 * One message of a folder's `Files` stream: a file change or a State control.
 * Server code reads the stream through the `Files` route. HTTP clients decode
 * the JSON body with this codec.
 */
export const FileMessage = Schema.Union([FileChange, StreamAddress.stateControl]);

export type FileMessage = typeof FileMessage.Type;

/** One message of the `Folders` registry stream. */
export const FolderMessage = Schema.Union([FolderChange, StreamAddress.stateControl]);

export type FolderMessage = typeof FolderMessage.Type;

/** A change or a control of one State collection with values of type `A`. */
export type StateMessage<A> = StreamAddress.StateEvent<A>;

/**
 * The State `reset` control. It means "clear the state and apply the changes
 * after it". Only `410 Gone` sends a reader back to `-1` (D17). Readers skip
 * the `snapshot-start` and `snapshot-end` controls.
 */
export const isReset = <A>(message: StateMessage<A>): message is StreamAddress.StateControl =>
  StreamAddress.isControl(message) && message.headers.control === "reset";

export const isUpsert = <A>(message: StateMessage<A>): message is StreamAddress.StateUpsert<A> =>
  StreamAddress.isChange(message) && message.headers.operation === "upsert";

/** An upsert of a live file. A tombstone upsert is a delete. */
export const isLive = (
  message: StateMessage<FileValue>,
): message is StreamAddress.StateUpsert<File> => isUpsert(message) && !isTombstone(message.value);

/** Apply file messages to a map of live files by path. A tombstone removes its path. */
export const applyFileChanges = (
  rows: Map<string, File>,
  messages: Iterable<StateMessage<FileValue>>,
): Map<string, File> => {
  for (const message of messages) {
    if (StreamAddress.isControl(message)) {
      if (isReset(message)) rows.clear();
    } else if (isLive(message)) rows.set(message.key, message.value);
    else rows.delete(message.key);
  }

  return rows;
};

/** The messages that a reader must apply: everything after the last reset in the batch. */
export const afterLastReset = <M extends StateMessage<unknown>>(messages: ReadonlyArray<M>) =>
  messages.slice(Option.getOrElse(Array.findLastIndex(messages, isReset), () => -1) + 1);

/** Apply messages to a map of live values by key. A reset clears the map. */
export const applyChanges = <A>(
  rows: Map<string, A>,
  messages: Iterable<StateMessage<A>>,
): Map<string, A> => {
  for (const message of messages) {
    if (StreamAddress.isControl(message)) {
      if (isReset(message)) rows.clear();
    } else if (isUpsert(message)) rows.set(message.key, message.value);
    else rows.delete(message.key);
  }

  return rows;
};

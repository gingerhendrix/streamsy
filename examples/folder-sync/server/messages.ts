import { StreamAddress } from "@streamsy/core";
import { Schema } from "effect";
import { File } from "./domain.ts";
import { Folder } from "./streams.ts";

/**
 * The State `reset` control. It means "clear the state and apply the changes
 * after it". Only `410 Gone` sends a reader back to `-1` (D17).
 */
export const ResetControl = Schema.Struct({
  headers: Schema.Struct({ control: Schema.Literal("reset") }),
});

export type ResetControl = typeof ResetControl.Type;

export const FileChange = StreamAddress.stateChange({ type: "file", schema: File });

export type FileChange = typeof FileChange.Type;

export const FolderChange = StreamAddress.stateChange({ type: "folder", schema: Folder });

export type FolderChange = typeof FolderChange.Type;

/**
 * One message of a folder's `Files` stream as a reader sees it. State's public
 * change codec omits controls, so every reader decodes through this union.
 */
export const FileMessage = Schema.Union([FileChange, ResetControl]);

export type FileMessage = typeof FileMessage.Type;

/** One message of the `Folders` registry stream. */
export const FolderMessage = Schema.Union([FolderChange, ResetControl]);

export type FolderMessage = typeof FolderMessage.Type;

/** A change or a reset of one State collection with values of type `A`. */
export type StateMessage<A> = StreamAddress.StateChange<A> | ResetControl;

export const isReset = <A>(message: StateMessage<A>): message is ResetControl =>
  !("type" in message);

export const isUpsert = <A>(message: StateMessage<A>): message is StreamAddress.StateUpsert<A> =>
  !isReset(message) && message.headers.operation === "upsert";

/** The messages that a reader must apply: everything after the last reset in the batch. */
export const afterLastReset = <M extends StateMessage<unknown>>(messages: ReadonlyArray<M>) =>
  messages.slice(messages.findLastIndex(isReset) + 1);

/** Apply messages to a map of live values by key. A reset clears the map. */
export const applyChanges = <A>(
  rows: Map<string, A>,
  messages: Iterable<StateMessage<A>>,
): Map<string, A> => {
  for (const message of messages) {
    if (isReset(message)) rows.clear();
    else if (isUpsert(message)) rows.set(message.key, message.value);
    else rows.delete(message.key);
  }

  return rows;
};

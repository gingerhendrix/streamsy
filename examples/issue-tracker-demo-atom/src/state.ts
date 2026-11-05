import { Option } from "effect";
import { Atom } from "effect/reactivity";
import type { PendingWrite, StatusFilter } from "./domain/workspace.ts";
import { mainWorkspaceId } from "./utils/workspace.ts";

// The atoms the page writes to. Every other value is derived in `queries.ts`.

/** Read once per page load, in `main.tsx`: switching workspace is a full navigation. */
export const workspaceIdAtom = Atom.keepAlive(Atom.make(mainWorkspaceId));

/** Holds an explicit user selection only. `selectedProjectAtom` falls back to the first project. */
export const selectedProjectIdAtom = Atom.keepAlive(Atom.make(Option.none<string>()));

export const filterAtom = Atom.keepAlive(Atom.make<StatusFilter>("all"));

/**
 * Every local write that the server has not refused, oldest first. A refusal
 * removes its write. The txid on the stream confirms a write, and
 * `pendingWritesAtom` derives that, so the stream never writes to this atom.
 */
export const localWritesAtom = Atom.keepAlive(Atom.make<ReadonlyArray<PendingWrite>>([]));

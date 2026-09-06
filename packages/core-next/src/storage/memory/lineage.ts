/** Synchronous port of cascade-reclaim and chained message composition.
 * Called only inside the memory mutation lock or synchronous read snapshot.
 */
import type { StoredMessage, StreamId, StreamRecord } from "../../schema/index.ts";
import type { State } from "./state.ts";

export function addEdge(state: State, parent: StreamId, child: StreamId): void {
  const children = state.children.get(parent) ?? new Set<StreamId>();
  children.add(child);
  state.children.set(parent, children);
}
export const hasDependents = (state: State, id: StreamId): boolean =>
  (state.children.get(id)?.size ?? 0) > 0;
export function purge(
  state: State,
  record: StreamRecord,
  chain: boolean,
  changed: Set<StreamId>,
): void {
  let current: StreamRecord | undefined = record;
  while (current) {
    const childId = current.id;
    const parentId: StreamId | undefined = chain ? current.lifecycle.forkedFrom : undefined;
    state.entries.delete(childId);
    state.children.delete(childId);
    changed.add(childId);
    if (parentId === undefined) return;
    const children = state.children.get(parentId);
    children?.delete(childId);
    if (children?.size === 0) state.children.delete(parentId);
    const parent: StreamRecord | undefined = state.entries.get(parentId)?.record;
    if (!parent?.lifecycle.softDeleted || hasDependents(state, parentId)) return;
    current = parent;
  }
}
export function composeMessages(state: State, id: StreamId, chain: boolean): StoredMessage[] {
  const entry = state.entries.get(id);
  if (!entry) return [];
  const { forkedFrom, forkOffset } = entry.record.lifecycle;
  const inherited =
    chain && forkedFrom !== undefined && forkOffset !== undefined
      ? composeMessages(state, forkedFrom, true).filter((message) => message.offset <= forkOffset)
      : [];
  return [...inherited, ...entry.messages].toSorted((a, b) =>
    a.offset < b.offset ? -1 : a.offset > b.offset ? 1 : 0,
  );
}

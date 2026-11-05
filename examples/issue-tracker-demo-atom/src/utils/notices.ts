import { Array, Effect } from "effect";
import { Atom, AtomRegistry } from "effect/reactivity";

export interface Notice {
  readonly id: number;
  readonly message: string;
}

/** How long a notice stays on the page. */
const noticeMillis = 4000;

/** Short messages that remove themselves, for writes that the server refused. */
export const noticesAtom = Atom.keepAlive(Atom.make<ReadonlyArray<Notice>>([]));

let nextId = 0;

/** Show a notice and wait until it has gone. */
export const showNotice = Effect.fnUntraced(function* (message: string) {
  const registry = yield* AtomRegistry.AtomRegistry;
  const id = nextId++;

  registry.update(noticesAtom, Array.append({ id, message }));
  yield* Effect.sleep(noticeMillis);
  registry.update(
    noticesAtom,
    Array.filter((notice) => notice.id !== id),
  );
});

import { Option, String } from "effect";
import { Atom } from "effect/reactivity";

/** The base app's key. `Atom.kvs` would save JSON, and the base app saves the plain name. */
const storageKey = "issue-tracker-demo:display-name";

/**
 * The name this browser writes with, kept in `localStorage`. Each tab of the
 * same browser starts with the last saved name.
 */
export const displayNameAtom = Atom.keepAlive(
  Atom.writable(
    () => localStorage.getItem(storageKey) ?? "",
    (ctx, name: string) => {
      ctx.setSelf(name);

      if (name.trim()) localStorage.setItem(storageKey, name.trim());
      else localStorage.removeItem(storageKey);
    },
  ),
);

/** The name to write with, when the user gave one. */
export const writerAtom = Atom.map(displayNameAtom, (name) =>
  Option.liftPredicate(name.trim(), String.isNonEmpty),
);

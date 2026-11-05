import { Option } from "effect";
import { AtomRegistry } from "effect/reactivity";
import { afterEach, describe, expect, it } from "vitest";
import { displayNameAtom, writerAtom } from "../../src/utils/display-name.ts";

const key = "issue-tracker-demo:display-name";

describe("the display name", () => {
  afterEach(() => localStorage.clear());

  it("reads the name that the base app saved", () => {
    localStorage.setItem(key, "Ada");

    expect(AtomRegistry.make().get(displayNameAtom)).toBe("Ada");
  });

  it("keeps the typed text and saves the plain trimmed name", () => {
    const registry = AtomRegistry.make();

    registry.set(displayNameAtom, " Ada ");

    expect(registry.get(displayNameAtom)).toBe(" Ada ");
    expect(localStorage.getItem(key)).toBe("Ada");
    expect(registry.get(writerAtom)).toEqual(Option.some("Ada"));
  });

  it("removes the key for an empty name, and the writer is anonymous", () => {
    const registry = AtomRegistry.make();

    registry.set(displayNameAtom, "Ada");
    registry.set(displayNameAtom, "  ");

    expect(localStorage.getItem(key)).toBeNull();
    expect(Option.isNone(registry.get(writerAtom))).toBe(true);
  });
});

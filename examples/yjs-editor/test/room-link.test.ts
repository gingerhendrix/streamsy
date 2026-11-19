import { nanoid } from "nanoid";
import { describe, expect, it } from "vitest";
import { roomIdFromHash } from "../src/room-link.ts";

const roomId = "AbCdEfGhIjKlMnOpQr1_-";

describe("room links", () => {
  it("accepts the default Nano ID and preserves case and URL-safe punctuation", () => {
    const generated = nanoid();

    expect(roomIdFromHash(`#${generated}`)).toBe(generated);
    expect(roomIdFromHash(`#${roomId}`)).toBe(roomId);
  });

  it.each([
    "",
    "#",
    "#notes",
    "#ideas",
    "#draft",
    roomId,
    `#${roomId.slice(1)}`,
    `#${roomId}A`,
    `#${roomId}\n`,
    `#${roomId.slice(1)}/`,
    `#${roomId.slice(1)}.`,
    `#${roomId.slice(1)}?`,
    `#${roomId.slice(1)}#`,
    "#AbCdEfGhIjKlMnOpQr%2F",
    `#${roomId.replace("_", "%5F")}`,
  ])("rejects %j", (hash) => {
    expect(roomIdFromHash(hash)).toBeNull();
  });
});

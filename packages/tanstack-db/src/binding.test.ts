import { expect, test } from "bun:test";
import { lowerResetResponse } from "./binding.ts";

test("reset lowering deletes stale rows before snapshot upserts", async () => {
  const response = new Response(
    JSON.stringify([
      { headers: { control: "reset" } },
      { headers: { control: "snapshot-start" } },
      {
        type: "row",
        key: "current",
        value: { id: "current" },
        headers: { operation: "upsert" },
      },
      { headers: { control: "snapshot-end" } },
    ]),
    { headers: { "x-streamsy-state-sink-reset": "snapshot" } },
  );
  const lowered = await lowerResetResponse(response, [{ id: "stale" }, { id: "current" }], {
    type: "row",
    primaryKey: "id",
  });
  const messages = await lowered.json();
  expect(messages).toEqual([
    { headers: { control: "snapshot-start" } },
    { type: "row", key: "stale", headers: { operation: "delete" } },
    { type: "row", key: "current", headers: { operation: "delete" } },
    {
      type: "row",
      key: "current",
      value: { id: "current" },
      headers: { operation: "upsert" },
    },
    { headers: { control: "snapshot-end" } },
  ]);
});

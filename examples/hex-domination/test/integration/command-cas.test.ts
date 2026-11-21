/* oxlint-disable effecttsgo/async-function -- Vitest owns the HTTP race. */
import { expect, it } from "vitest";
import { readHistory } from "../../server/game/canonical.ts";
import {
  boardFor,
  checkedNumber,
  checkedString,
  createGame,
  decisionFor,
  post,
  riskHarness,
} from "../harness.ts";

it("commits one payload when two legal commands race with the same commandId", async () => {
  const h = riskHarness();
  const game = await createGame(h.app);
  const before = await boardFor(h.app, game);
  const active = checkedString(before.game.activePlayerId, "active seat");
  const decision = await decisionFor(h.app, game, active);
  const move = decision.legalMoves.find((value: { type: string }) => value.type === "reinforce");
  const pool = checkedNumber(move.pool, "reinforcement pool");
  const territoryIds = [0, 1].map((index) => checkedString(move.territoryIds[index], "territory"));
  const commands = territoryIds.map((territoryId) => ({
    commandId: "same-cas-id",
    turnId: checkedString(decision.turn.id, "turn"),
    action: { type: "reinforce", placements: [{ territoryId, armies: pool }] },
  }));
  const results = await Promise.all(commands.map((command) => post(h.app, game, active, command)));
  const winner = results.findIndex((response) => response.body.status === "accepted");
  expect(winner).toBeGreaterThanOrEqual(0);
  const accepted = results[winner]!;
  expect(accepted.status).toBe(200);
  expect(results[1 - winner]).toMatchObject({
    status: 409,
    body: { error: { code: "COMMAND_ID_REUSED" } },
  });
  const retry = await post(h.app, game, active, commands[winner]!);
  expect(retry).toMatchObject({
    status: 200,
    body: { status: "duplicate", eventOffset: accepted.body.eventOffset },
  });
  const canonical = await h.streams.runPromise(readHistory(game.gameId));
  const receipts = canonical.commands.filter(
    ({ batch }) => batch.events[0].commandId === "same-cas-id",
  );
  expect(receipts).toHaveLength(1);
  expect(receipts[0]?.offset).toBe(accepted.body.eventOffset);
  const after = await boardFor(h.app, game);
  expect(after.sourceThroughOffset).toBe(accepted.body.eventOffset);
  expect(after.game.phase).toBe("attack");
});

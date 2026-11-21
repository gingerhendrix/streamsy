/**
 * The front page is an invitation, not a form.
 *
 * These assertions are about what the page *does not* ask for as much as what it
 * says: the previous landing page collected a player name, two agent names, and a
 * choice between two creation paths, and every one of those has moved to the lobby.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { Landing } from "./App.tsx";

function renderLanding(overrides: { busy?: boolean; joinId?: string; notice?: string } = {}) {
  return renderToStaticMarkup(
    <Landing
      busy={overrides.busy ?? false}
      joinId={overrides.joinId ?? ""}
      notice={overrides.notice ?? ""}
      onJoinId={() => {}}
      onCreate={() => {}}
      onOpen={() => {}}
    />,
  );
}

describe("Landing", () => {
  it("carries the invitation verbatim", () => {
    const markup = renderLanding();
    expect(markup).toContain("Can you beat your agent at Hex Domination?");
    expect(markup).toContain("Play with friends or agents.");
  });

  it("offers exactly one creation command and asks for no names", () => {
    const markup = renderLanding();
    expect(markup.match(/Create a game/g)?.length).toBe(1);
    expect(markup).not.toContain("agent vs agent");
    expect(markup).not.toContain("Your name");
    expect(markup).not.toContain("First agent");
    expect(markup).not.toContain("Second agent");
    // The only input on the page is the way back into a game you were sent.
    expect(markup.match(/<input/g)?.length).toBe(1);
    expect(markup).toContain('aria-label="Game ID"');
  });

  it("holds the entry command until a game id is typed", () => {
    expect(renderLanding()).toContain("View lobby");
    expect(renderLanding({ joinId: "   " })).toContain("disabled");
    expect(renderLanding({ joinId: "game_abc" })).not.toContain("disabled");
  });

  it("reports progress and notices in place", () => {
    expect(renderLanding({ busy: true })).toContain("Creating…");
    expect(renderLanding({ notice: "Lobby opened." })).toContain("Lobby opened.");
  });
});

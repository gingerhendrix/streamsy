import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { COLORS, PlayerFields } from "./shared.tsx";

describe("player colour availability", () => {
  it("marks colours selected by other players unavailable to mouse and keyboard", () => {
    const html = renderToStaticMarkup(
      <PlayerFields
        name="Mina"
        color={COLORS[1]!}
        unavailableColors={[COLORS[0]!, COLORS[1]!]}
        onName={() => {}}
        onColor={() => {}}
      />,
    );

    expect(html).toContain(`${COLORS[0]} unavailable — already selected`);
    expect(html).toContain(`${COLORS[1]} unavailable — already selected`);
    expect(html.match(/class="swatch[^"]* unavailable"/g)).toHaveLength(2);
    expect(html.match(/disabled=""/g)).toHaveLength(2);
    expect(html.match(/aria-hidden="true">×/g)).toHaveLength(2);
  });

  it("keeps the player's own current colour enabled and reselectable", () => {
    const html = renderToStaticMarkup(
      <PlayerFields
        name="Ada"
        color={COLORS[0]!}
        ownedColor={COLORS[0]!.toUpperCase()}
        unavailableColors={[COLORS[0]!, COLORS[1]!]}
        onName={() => {}}
        onColor={() => {}}
      />,
    );

    expect(html).toContain(`aria-label="Choose ${COLORS[0]}"`);
    expect(html).toContain(`aria-label="${COLORS[1]} unavailable — already selected"`);
    expect(html.match(/disabled=""/g)).toHaveLength(1);
  });
});

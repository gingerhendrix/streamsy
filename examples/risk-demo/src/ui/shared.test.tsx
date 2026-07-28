import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { PlayerFields } from "./shared.tsx";

describe("player identity fields", () => {
  it("asks only for a name because the game assigns an available colour", () => {
    const html = renderToStaticMarkup(<PlayerFields name="Mina" onName={() => {}} />);

    expect(html).toContain("Your name");
    expect(html).toContain('value="Mina"');
    expect(html).not.toContain("Colour");
    expect(html).not.toContain("swatch");
  });
});

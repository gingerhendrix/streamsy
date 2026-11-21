import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { MapControls } from "./map-controls.tsx";

function render(): string {
  const action = vi.fn();
  return renderToStaticMarkup(
    <MapControls
      onZoomIn={action}
      onZoomOut={action}
      onReset={action}
      onPanUp={action}
      onPanLeft={action}
      onPanRight={action}
      onPanDown={action}
    />,
  );
}

describe("map controls", () => {
  it("groups the map, compass, and zoom controls with accessible names", () => {
    const html = render();

    expect(html).toContain('role="group" aria-label="Map view controls"');
    expect(html).toContain('role="group" aria-label="Pan the map"');
    expect(html).toContain('role="group" aria-label="Zoom the map"');
    for (const name of [
      "Pan up",
      "Pan left",
      "Pan right",
      "Pan down",
      "Zoom out",
      "Zoom in",
      "Reset the map view",
    ]) {
      expect(html).toContain(`aria-label="${name}"`);
    }
  });

  it("renders native buttons in compass order and recognizable zoom signs", () => {
    const html = render();
    const buttonNames = [...html.matchAll(/<button[^>]*aria-label="([^"]+)"[^>]*>/g)].map(
      (match) => match[1],
    );

    expect(buttonNames).toEqual([
      "Pan up",
      "Pan left",
      "Pan right",
      "Pan down",
      "Zoom out",
      "Zoom in",
      "Reset the map view",
    ]);
    expect(html).toContain('class="pan-up"');
    expect(html).toContain('class="pan-left"');
    expect(html).toContain('class="pan-right"');
    expect(html).toContain('class="pan-down"');
    expect(html).toContain('aria-label="Zoom out">−</button>');
    expect(html).toContain('aria-label="Zoom in">+</button>');
    expect(html).toContain('<svg viewBox="0 0 24 24" focusable="false">');
    expect(html.match(/type="button"/g)).toHaveLength(7);
  });
});

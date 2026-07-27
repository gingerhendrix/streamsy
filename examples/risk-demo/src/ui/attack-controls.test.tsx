import { Children, isValidElement, type ReactElement, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { LegalActionV2 } from "../application/legal-actions-v2.ts";
import { PhaseControls } from "./game-v2.tsx";
import type { NameLookup } from "./presentation-v2.ts";

const NAMES: NameLookup = {
  territory: (id) => ({ a: "Ashfell", b: "Birchgate" })[id] ?? id,
  player: (id) => id ?? "Nobody",
  continent: (id) => id,
};

const FORTIFY: Extract<LegalActionV2, { type: "fortify" }> = {
  type: "fortify",
  choices: [{ from: "a", reachable: [{ to: "b", maxArmies: 3 }] }],
};

function controls(
  overrides: Partial<Parameters<typeof PhaseControls>[0]> = {},
): ReturnType<typeof PhaseControls> {
  return PhaseControls({
    names: NAMES,
    busy: false,
    selection: null,
    attackPhase: true,
    setSelection: () => {},
    intent: "attack",
    setIntent: () => {},
    pendingReinforcements: new Map(),
    adjustReinforcement: () => {},
    finishReinforcements: () => {},
    occupyArmies: null,
    setOccupyArmies: () => {},
    submit: async () => true,
    fortifyChoiceFrom: (from) => FORTIFY.choices.find((choice) => choice.from === from),
    ...overrides,
  });
}

function textContent(node: ReactNode): string {
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (!isValidElement<{ children?: ReactNode }>(node)) return "";
  return Children.toArray(node.props.children).map(textContent).join("");
}

function buttonNamed(node: ReactNode, name: string): ReactElement<{ onClick(): void }> {
  if (
    isValidElement<{ children?: ReactNode; onClick?: () => void }>(node) &&
    node.type === "button" &&
    textContent(node) === name &&
    node.props.onClick
  ) {
    return node as ReactElement<{ onClick(): void }>;
  }
  if (isValidElement<{ children?: ReactNode }>(node)) {
    for (const child of Children.toArray(node.props.children)) {
      try {
        return buttonNamed(child, name);
      } catch {
        // Continue through the small static element tree.
      }
    }
  }
  throw new Error(`Button not found: ${name}`);
}

describe("attack controls", () => {
  it("replaces the Fortify toggle with one explicit End attack control", () => {
    let intent = "attack";
    const rendered = controls({
      fortifyAction: FORTIFY,
      setIntent: (next) => {
        intent = next;
      },
    });
    const html = renderToStaticMarkup(rendered);
    expect(html).toContain("End attack →");
    expect(html).not.toMatch(/>Fortify</);
    buttonNamed(rendered, "End attack →").props.onClick();
    expect(intent).toBe("fortify");
  });

  it("keeps End attack available when no fortification is legal", () => {
    const html = renderToStaticMarkup(controls());
    expect(html).toMatch(/class="end-attack">End attack →/);
  });

  it("names the existing fortify submission as the end of attacking", () => {
    let submitted: unknown;
    const rendered = controls({
      intent: "fortify",
      fortifyAction: FORTIFY,
      selection: { kind: "fortify", from: "a", to: "b", armies: 2 },
      submit: async (action) => {
        submitted = action;
        return true;
      },
    });
    const html = renderToStaticMarkup(rendered);
    expect(html).toContain("Attack ended · choose your fortification");
    expect(html).toContain("End attack · move 2");
    buttonNamed(rendered, "End attack · move 2").props.onClick();
    expect(submitted).toEqual({ type: "fortify", from: "a", to: "b", armies: 2 });
  });
});

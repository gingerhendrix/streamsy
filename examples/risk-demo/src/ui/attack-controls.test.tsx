/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
import { Children, isValidElement, type ReactElement, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { Option, Schema } from "effect";

import type { LegalAction } from "../application/legal-actions.ts";
import { PhaseControls } from "./game.tsx";
import type { NameLookup } from "./presentation.ts";

const NAMES: NameLookup = {
  territory: (id) => ({ a: "Ashfell", b: "Birchgate" })[id] ?? id,
  player: (id) => id ?? "Nobody",
  continent: (id) => id,
};

const FORTIFY: Extract<LegalAction, { type: "fortify" }> = {
  type: "fortify",
  choices: [{ from: "a", reachable: [{ to: "b", maxArmies: 3 }] }],
  submit: {
    type: "fortify",
    from: "<choice.from>",
    to: "<choice.reachable.to>",
    armies: "<1..choice.reachable.maxArmies>",
  },
};

const SKIP_FORTIFICATIONS: Extract<LegalAction, { type: "skip-fortifications" }> = {
  type: "skip-fortifications",
  submit: { type: "skip-fortifications" },
};

function controls(
  overrides: Partial<Parameters<typeof PhaseControls>[0]> = {},
): ReturnType<typeof PhaseControls> {
  return PhaseControls({
    names: NAMES,
    busy: false,
    selection: null,
    attackPhase: true,
    skipFortificationsAction: SKIP_FORTIFICATIONS,
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
  const text = Schema.decodeUnknownOption(Schema.String)(node);
  if (Option.isSome(text)) return text.value;
  const number = Schema.decodeUnknownOption(Schema.Finite)(node);
  if (Option.isSome(number)) return String(number.value);
  if (!isValidElement<{ children?: ReactNode }>(node)) return "";
  return Children.toArray(node.props.children).map(textContent).join("");
}

interface ClickableButtonProps {
  children?: ReactNode;
  onClick(): void;
}

function buttonNamed(node: ReactNode, name: string): ReactElement<ClickableButtonProps> {
  if (
    isValidElement<ClickableButtonProps>(node) &&
    node.type === "button" &&
    textContent(node) === name
  ) {
    return node;
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
  it("puts the Fortify transition after the attack controls", () => {
    let intent = "attack";
    const rendered = controls({
      fortifyAction: FORTIFY,
      setIntent: (next) => {
        intent = next;
      },
    });
    const html = renderToStaticMarkup(rendered);
    expect(html).not.toContain("End attack");
    expect(html.indexOf("Pick a highlighted country")).toBeLessThan(html.indexOf("Fortify →"));
    buttonNamed(rendered, "Fortify →").props.onClick();
    expect(intent).toBe("fortify");
  });

  it("keeps Fortify available when no fortification is legal", () => {
    const html = renderToStaticMarkup(controls());
    expect(html).toMatch(/class="fortify-next">Fortify →/);
  });

  it("submits the canonical fortification move", () => {
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
    expect(html).toContain("Fortify with 2");
    buttonNamed(rendered, "Fortify with 2").props.onClick();
    expect(submitted).toEqual({ type: "fortify", from: "a", to: "b", armies: 2 });
  });

  it("skips fortifications from the Fortify controls", () => {
    let submitted: unknown;
    const rendered = controls({
      intent: "fortify",
      fortifyAction: undefined,
      submit: async (action) => {
        submitted = action;
        return true;
      },
    });

    const html = renderToStaticMarkup(rendered);
    expect(html).toContain("No fortification is available.");
    expect(html).toContain("Skip fortifications");
    expect(html).not.toContain("End turn");
    buttonNamed(rendered, "Skip fortifications").props.onClick();
    expect(submitted).toEqual({ type: "skip-fortifications" });
  });
});

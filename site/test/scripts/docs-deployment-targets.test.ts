import { expect, test } from "bun:test";
import { docsDeploymentTarget } from "../../../scripts/docs-deployment-targets.ts";

test("production updates the existing Worker from a pinned remote-state stage", () => {
  expect(docsDeploymentTarget()).toEqual({
    appId: "streamsy-docs",
    resourceId: "streamsy-docs",
    name: "streamsy-docs-streamsy-docs-gareth",
    domains: ["streamsy.gandrew.com", "streamsy.dev"],
    stage: "production",
  });
  expect(docsDeploymentTarget("production")).toEqual(docsDeploymentTarget());
});

test("next deploys its own Worker to next.streamsy.dev", () => {
  expect(docsDeploymentTarget("next")).toEqual({
    appId: "streamsy-docs",
    resourceId: "streamsy-docs",
    name: "streamsy-docs-next",
    domains: ["next.streamsy.dev"],
    stage: "next",
  });
});

test("experimental and preview keep local state and the default stage", () => {
  const experimental = docsDeploymentTarget("experimental");
  expect(experimental).toEqual({
    appId: "streamsy-docs-experimental",
    resourceId: "streamsy-docs-experimental",
    name: "streamsy-docs-experimental",
    domains: ["experimental.streamsy.dev"],
    stage: undefined,
  });
  expect(docsDeploymentTarget("preview")).toEqual(experimental);
});

test("shared targets never share a Worker or a domain", () => {
  const production = docsDeploymentTarget("production");
  const next = docsDeploymentTarget("next");
  expect(next.name).not.toBe(production.name);
  expect(next.stage).not.toBe(production.stage);

  for (const domain of next.domains) expect(production.domains).not.toContain(domain);
});

test("an unknown selector fails", () => {
  expect(() => docsDeploymentTarget("staging")).toThrow(
    "Unknown STREAMSY_DOCS_DEPLOYMENT value: staging",
  );
});

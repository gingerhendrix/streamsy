/**
 * The one host-specific module of the host-neutral serve tests. It gives the
 * listener and the Effect main of the runtime under test. Today that is Bun,
 * because the package tests run under `bun test`. Another host replaces only
 * this module.
 */
export { listener, type ListenerOptions } from "../../src/bun.ts";
export { runMain } from "@effect/platform-bun/BunRuntime";

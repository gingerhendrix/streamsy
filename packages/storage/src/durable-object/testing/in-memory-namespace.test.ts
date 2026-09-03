/**
 * Contract tests for the in-memory namespace fake itself.
 *
 * The fake stands in for `DurableObjectNamespace` in every adapter test, so the
 * routing guarantees those tests rely on — one stub per name, ids that compare
 * by name, and a hard failure on the members that carry no meaning here — are
 * asserted directly rather than only implied by the suites that consume it.
 */
import { describe, expect, it } from "vitest";
import { createFakeNamespace } from "./in-memory-namespace.ts";

describe("createFakeNamespace", () => {
  it("routes idFromName, idFromString, and getByName to one stub per name", () => {
    const fake = createFakeNamespace();

    const byName = fake.namespace.get(fake.namespace.idFromName("alpha"));
    const byString = fake.namespace.get(fake.namespace.idFromString("alpha"));
    const direct = fake.namespace.getByName("alpha");

    expect(byName).toBe(byString);
    expect(byName).toBe(direct);
    expect(byName).toBe(fake.stubFor("alpha"));
    expect(fake.namespace.get(fake.namespace.idFromName("beta"))).not.toBe(byName);
  });

  it("reports which stubs exist without creating them", () => {
    const fake = createFakeNamespace();

    expect(fake.has("alpha")).toBe(false);
    fake.namespace.get(fake.namespace.idFromName("alpha"));
    expect(fake.has("alpha")).toBe(true);
    expect(fake.has("beta")).toBe(false);
  });

  it("gives ids that stringify to and compare by their name", () => {
    const fake = createFakeNamespace();

    const alpha = fake.namespace.idFromName("alpha");
    expect(alpha.toString()).toBe("alpha");
    expect(alpha.name).toBe("alpha");
    expect(alpha.equals(fake.namespace.idFromString("alpha"))).toBe(true);
    expect(alpha.equals(fake.namespace.idFromName("beta"))).toBe(false);
  });

  it("rejects the namespace members it cannot model instead of standing them in", () => {
    const fake = createFakeNamespace();

    expect(() => fake.namespace.newUniqueId()).toThrow(/newUniqueId is not supported/);
    expect(() => fake.namespace.jurisdiction("eu")).toThrow(/jurisdiction is not supported/);
  });

  it("self-initializes a stub on first use and rejects a second stream id", async () => {
    const fake = createFakeNamespace();
    const stub = fake.stubFor("alpha");

    expect(await stub.getRecord("alpha")).toBeNull();
    expect(stub.state.boundId).toBe("alpha");
    expect(stub.state.initCalls).toEqual(["alpha"]);

    await expect(stub.getRecord("beta")).rejects.toThrow(
      "Durable Object already initialized for stream alpha",
    );
  });

  it("reports an unattributable append failure as an offset failure, as the real class does", async () => {
    const fake = createFakeNamespace();
    const stub = fake.stubFor("alpha");

    // No record exists, so `applyMutation` fails without a reason. The seam
    // requires one on every `precondition-failed`.
    const result = await stub.append("alpha", {
      preconditions: {},
      messages: [],
      recordPatch: {},
    });

    expect(result).toEqual({ status: "precondition-failed", record: null, reason: "offset" });
  });
});

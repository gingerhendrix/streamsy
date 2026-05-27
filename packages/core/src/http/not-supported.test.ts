import { describe, expect, it } from "vitest";
import { notSupported } from "../types/factory.ts";
import { HttpResponseFactory } from "./responses.ts";
import { maybeNotSupportedResponse, notSupportedResponse } from "./not-supported.ts";

describe("notSupportedResponse", () => {
  it("returns 400 with the feature header and default body", async () => {
    const responses = new HttpResponseFactory();
    const response = notSupportedResponse(notSupported("fork"), responses);
    expect(response.status).toBe(400);
    expect(response.headers.get("stream-not-supported")).toBe("fork");
    expect(await response.text()).toBe("Feature not supported: fork");
  });

  it("uses the supplied message when present", async () => {
    const responses = new HttpResponseFactory();
    const response = notSupportedResponse(
      notSupported("live-read", "long-poll not supported by this adapter"),
      responses,
    );
    expect(response.status).toBe(400);
    expect(response.headers.get("stream-not-supported")).toBe("live-read");
    expect(await response.text()).toBe("long-poll not supported by this adapter");
  });

  it.each([
    ["producer-idempotency", "Feature not supported: producer-idempotency"],
    ["fork", "Feature not supported: fork"],
    ["live-read", "Feature not supported: live-read"],
    ["active-expiry", "Feature not supported: active-expiry"],
    ["mutation-lock", "Feature not supported: mutation-lock"],
  ])("maps feature %s to a 400 with the feature header and default body", async (feature, body) => {
    const responses = new HttpResponseFactory();
    const response = notSupportedResponse(notSupported(feature), responses);
    expect(response.status).toBe(400);
    expect(response.headers.get("stream-not-supported")).toBe(feature);
    expect(await response.text()).toBe(body);
  });

  it("preserves an empty supplied message when present", async () => {
    const responses = new HttpResponseFactory();
    const response = notSupportedResponse(notSupported("fork", ""), responses);
    expect(response.headers.get("stream-not-supported")).toBe("fork");
    expect(await response.text()).toBe("");
  });
});

describe("maybeNotSupportedResponse", () => {
  const responses = new HttpResponseFactory();

  it("returns null for non-not-supported results", () => {
    expect(maybeNotSupportedResponse({ status: "ok" as const }, responses)).toBeNull();
  });

  it("returns a 400 response when the union member is not-supported", async () => {
    const response = maybeNotSupportedResponse(notSupported("producer-idempotency"), responses);
    expect(response).not.toBeNull();
    expect(response!.status).toBe(400);
    expect(response!.headers.get("stream-not-supported")).toBe("producer-idempotency");
  });

  it("forwards the supplied message in the 400 body when present", async () => {
    const response = maybeNotSupportedResponse(
      notSupported("active-expiry", "memory adapter cannot schedule active expiry"),
      responses,
    );
    expect(response).not.toBeNull();
    expect(response!.headers.get("stream-not-supported")).toBe("active-expiry");
    expect(await response!.text()).toBe("memory adapter cannot schedule active expiry");
  });
});

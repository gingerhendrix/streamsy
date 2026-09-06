import { describe, expect, it } from "vitest";
import {
  MAX_PREVIEW_WORKER_NAME_LENGTH,
  deploymentOutputFromJson,
  uniqueConformanceStage,
  workerNameForStage,
  workerUrlFrom,
} from "./deployment-state.ts";

describe("deploymentOutputFromJson", () => {
  it("reads output.url from a well-formed state value", () => {
    expect(
      deploymentOutputFromJson(
        '{"output":{"name":"streamsy-conf-server-test","url":"https://server.workers.dev"}}',
      ),
    ).toEqual({ name: "streamsy-conf-server-test", url: "https://server.workers.dev" });
  });

  it("keeps unrelated state and output fields outside the consumed contract", () => {
    expect(
      deploymentOutputFromJson(
        '{"status":"created","props":{},"output":{"id":7,"name":"worker","url":"https://a.test"}}',
      ),
    ).toEqual({ name: "worker", url: "https://a.test" });
  });

  it.each([
    ["a non-object state", "42"],
    ["a null state", "null"],
    ["an array state", '[{"output":{"url":"https://a.test"}}]'],
    ["a state with no output", '{"status":"created"}'],
    ["a null output", '{"output":null}'],
    ["a non-object output", '{"output":"https://a.test"}'],
    ["an array output", '{"output":["https://a.test"]}'],
    ["an output with no url", '{"output":{"id":7}}'],
    ["an output with no name", '{"output":{"url":"https://a.test"}}'],
    ["a non-string name", '{"output":{"name":7,"url":"https://a.test"}}'],
    ["an empty name", '{"output":{"name":"","url":"https://a.test"}}'],
    ["a non-string url", '{"output":{"url":7}}'],
    ["a null url", '{"output":{"url":null}}'],
    ["an empty url", '{"output":{"url":""}}'],
  ])("rejects %s", (_label, json) => {
    expect(deploymentOutputFromJson(json)).toBeNull();
  });

  it("keeps malformed JSON syntax visible", () => {
    expect(() => deploymentOutputFromJson('{"output":')).toThrow(SyntaxError);
  });
});

describe("workerUrlFrom", () => {
  it("strips one trailing slash so the base URL joins cleanly", () => {
    expect(workerUrlFrom({ name: "worker", url: "https://server.workers.dev/" })).toBe(
      "https://server.workers.dev",
    );
  });
});

describe("uniqueConformanceStage", () => {
  it("keeps the final Alchemy Worker name within Cloudflare's preview limit", () => {
    const stage = uniqueConformanceStage(
      "conformance-step-zero-measurement-with-an-intentionally-long-label",
      "1788650843-an-intentionally-long-run-identifier",
    );
    const workerName = workerNameForStage(stage);

    expect(workerName).toHaveLength(MAX_PREVIEW_WORKER_NAME_LENGTH);
    expect(workerName.endsWith("identifier")).toBe(true);
  });
});

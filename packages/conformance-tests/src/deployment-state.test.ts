import { describe, expect, it } from "vitest";
import { deploymentOutputFromJson, workerUrlFrom } from "./deployment-state.ts";

describe("deploymentOutputFromJson", () => {
  it("reads output.url from a well-formed state value", () => {
    expect(deploymentOutputFromJson('{"output":{"url":"https://server.workers.dev"}}')).toEqual({
      url: "https://server.workers.dev",
    });
  });

  it("keeps unrelated state and output fields outside the consumed contract", () => {
    expect(
      deploymentOutputFromJson(
        '{"status":"created","props":{},"output":{"id":7,"url":"https://a.test"}}',
      ),
    ).toEqual({ url: "https://a.test" });
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
    expect(workerUrlFrom({ url: "https://server.workers.dev/" })).toBe(
      "https://server.workers.dev",
    );
  });
});

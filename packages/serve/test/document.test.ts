/* oxlint-disable anti-slop/no-unknown-parameters -- The test codec exercises the sink's external wire boundary. */
import { describe, expect, test } from "bun:test";
import {
  cacheControlOf,
  defineDocumentSink,
  DOCUMENT_SINK_ERROR_TAGS,
} from "@streamsy/serve/document";

interface Summary {
  readonly workspaceId: string;
}

const sink = defineDocumentSink({
  name: "test.workspace-summary",
  from: [{ name: "test.issues" }, { name: "test.projects" }],
  document: {
    decode: (value: unknown): Summary => {
      if (!(value instanceof Object) || !("workspaceId" in value)) throw new Error("no workspace");
      return { workspaceId: String(value.workspaceId) };
    },
  },
  route: "/document/:workspaceId/summary",
  params: { workspaceId: { decode: (value: string) => value } },
  cache: { visibility: "private", maxAgeSeconds: 0, mustRevalidate: true },
  errors: DOCUMENT_SINK_ERROR_TAGS,
});

describe("defineDocumentSink", () => {
  test("lowers the declared cache policy to one header value", () => {
    expect(sink.cacheControl).toBe("private, max-age=0, must-revalidate");
    expect(cacheControlOf({ visibility: "public", maxAgeSeconds: 30, mustRevalidate: false })).toBe(
      "public, max-age=30",
    );
  });

  test("the fingerprint covers the sources, route and cache policy", () => {
    expect(sink.fingerprint).toMatch(/^[0-9a-f]{8}$/);
    const cachedLonger = defineDocumentSink({
      name: "test.workspace-summary",
      from: [{ name: "test.issues" }, { name: "test.projects" }],
      document: sink.document,
      route: "/document/:workspaceId/summary",
      params: { workspaceId: { decode: (value: string) => value } },
      cache: { visibility: "private", maxAgeSeconds: 30, mustRevalidate: true },
      errors: DOCUMENT_SINK_ERROR_TAGS,
    });
    const fewerSources = defineDocumentSink({
      name: "test.workspace-summary",
      from: [{ name: "test.issues" }],
      document: sink.document,
      route: "/document/:workspaceId/summary",
      params: { workspaceId: { decode: (value: string) => value } },
      cache: { visibility: "private", maxAgeSeconds: 0, mustRevalidate: true },
      errors: DOCUMENT_SINK_ERROR_TAGS,
    });
    expect(cachedLonger.fingerprint).not.toBe(sink.fingerprint);
    expect(fewerSources.fingerprint).not.toBe(sink.fingerprint);
  });

  test("the compiled route builds and matches the declared path", () => {
    expect(sink.compiledRoute.build({ workspaceId: "main" })).toBe("/document/main/summary");
    expect(sink.compiledRoute.match("/document/main/summary")).toEqual({
      kind: "matched",
      params: { workspaceId: "main" },
    });
    expect(sink.kind).toBe("checked-document-sink");
  });
});

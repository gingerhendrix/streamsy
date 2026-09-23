/* oxlint-disable effecttsgo/async-function, anti-slop/no-unknown-parameters -- The drift test feeds intentionally invalid JSON to both decoders; the bundle test owns Bun.build. */
import { describe, expect, test } from "bun:test";
import { canonicalJson, contractFingerprint, documentEtag } from "@streamsy/serve/contract";

describe("canonical encoding", () => {
  test("sorts object keys and preserves array order", () => {
    expect(canonicalJson({ b: 1, a: [3, 1, 2] })).toBe('{"a":[3,1,2],"b":1}');
  });

  test("two values that differ only in property order encode identically", () => {
    expect(canonicalJson({ a: { y: 1, x: 2 }, b: null })).toBe(
      canonicalJson({ b: null, a: { x: 2, y: 1 } }),
    );
  });

  test("null is a value, not an absence", () => {
    expect(canonicalJson({ a: null })).toBe('{"a":null}');
    expect(canonicalJson(null)).toBe("null");
  });
});

describe("digests", () => {
  test("a fingerprint is eight lowercase hex characters and follows the value", () => {
    const one = contractFingerprint({ route: "/a", params: ["x"] });
    expect(one).toMatch(/^[0-9a-f]{8}$/);
    expect(contractFingerprint({ params: ["x"], route: "/a" })).toBe(one);
    expect(contractFingerprint({ route: "/b", params: ["x"] })).not.toBe(one);
  });

  test("an entity tag is a quoted sixteen-hex validator that changes with the bytes", () => {
    const tag = documentEtag('{"issues":1}');
    expect(tag).toMatch(/^"[0-9a-f]{16}"$/);
    expect(documentEtag('{"issues":1}')).toBe(tag);
    expect(documentEtag('{"issues":2}')).not.toBe(tag);
  });
});

import { Schema } from "effect";
import { PublicErrorSchema } from "../src/errors.ts";
import { decodePublicError, type PublicError } from "../src/contract.ts";

const errors: readonly PublicError[] = [
  { _tag: "InvalidParams", route: "/feed", parameter: "seat", detail: "invalid" },
  {
    _tag: "ProtocolVersionUnsupported",
    route: "/feed",
    supported: 1,
    received: "2",
    recovery: "replay-from-start",
  },
  {
    _tag: "ResumeRejected",
    route: "/feed",
    reason: "invalid-offset",
    recovery: "replay-from-start",
  },
  {
    _tag: "ResumeRejected",
    route: "/feed",
    reason: "history-unavailable",
    recovery: "replay-from-start",
  },
  {
    _tag: "ResumeRejected",
    route: "/feed",
    reason: "contract-changed",
    recovery: "replay-from-start",
  },
  { _tag: "ContractChanged", route: "/doc", recovery: "refetch" },
  { _tag: "TransportUnavailable", route: "/feed", detail: "unavailable" },
  { _tag: "DocumentUnavailable", route: "/doc", detail: "unavailable" },
  { _tag: "WireEncodeFailed", route: "/doc", detail: "invalid" },
];
test("S-4 server Schema and browser decoder agree on the wire JSON domain", () => {
  const server = Schema.decodeUnknownSync(PublicErrorSchema);
  const accepts = (decode: (value: unknown) => PublicError, value: unknown) => {
    try {
      return { ok: true, value: decode(value) };
    } catch {
      return { ok: false };
    }
  };
  const candidates: unknown[] = [null, [], {}, 1, "error"];
  for (const error of errors) {
    expect(server(error)).toEqual(decodePublicError(error));
    candidates.push(error, { ...error, extra: "ignored" });
    for (const key of Object.keys(error)) {
      const missing = Object.fromEntries(Object.entries(error).filter(([name]) => name !== key));
      candidates.push(missing);
      for (const wrong of [null, false, [], {}, 2, "unknown"])
        candidates.push({ ...error, [key]: wrong });
    }
  }
  for (const candidate of candidates)
    expect(accepts(decodePublicError, candidate)).toEqual(accepts(server, candidate));
});
test("browser decoder rejects accessor and boxed required fields", () => {
  let accessed = false;
  expect(() =>
    decodePublicError({
      ...errors[0],
      get route() {
        accessed = true;
        return "/feed";
      },
    }),
  ).toThrow();
  expect(accessed).toBe(false);
  expect(() => decodePublicError({ ...errors[0], route: Object("feed") })).toThrow();
  expect(() => canonicalJson({ number: Number.NaN })).toThrow();
});
test("contract browser bundle has no Effect or core dependency", async () => {
  const modules = new Set<string>();
  const result = await Bun.build({
    entrypoints: [new URL("../src/contract.ts", import.meta.url).pathname],
    target: "browser",
    format: "esm",
    plugins: [
      {
        name: "contract-perimeter",
        setup(build) {
          build.onLoad({ filter: /./ }, (args) => {
            modules.add(args.path);
            return undefined;
          });
        },
      },
    ],
  });
  expect(result.success).toBe(true);
  expect(
    [...modules].filter((path) =>
      /(?:\/effect(?:\/|@)|\/@streamsy\/core(?:\/|@)|\/packages\/core\/)/.test(path),
    ),
  ).toEqual([]);
  expect(modules.size).toBeGreaterThan(0);
});

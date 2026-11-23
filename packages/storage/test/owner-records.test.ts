/* oxlint-disable effecttsgo/strict-effect-provide -- Each test is its own host entry point. */
import { expect } from "vitest";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import { MemoryStorage, Streamsy } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import {
  checkOwnerRecordPrefixes,
  explainList,
  rejectsMalformedPrefixes,
} from "./support/owner-records.ts";

it.live("memory lists original owner keys that start with the exact prefix", () =>
  checkOwnerRecordPrefixes.pipe(
    Effect.provide(MemoryStorage.layer(Streamsy.group(Streamsy.Wildcard))),
  ),
);

it.live("sqlite lists original owner keys by a primary key range", () =>
  Effect.gen(function* () {
    yield* checkOwnerRecordPrefixes;
    yield* rejectsMalformedPrefixes;
    const plan = yield* explainList('["checkpoint","a",');
    expect(plan).toEqual([
      expect.objectContaining({
        detail: expect.stringMatching(
          /SEARCH .*USING INDEX sqlite_autoindex_streamsy_owner_records_1 \(owner_key>\? AND owner_key<\?\)/u,
        ),
      }),
    ]);
  }).pipe(Effect.provide(BunStorage.layerProtocol({ client: { filename: ":memory:" } }))),
);

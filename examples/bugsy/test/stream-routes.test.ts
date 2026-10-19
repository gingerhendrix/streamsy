import { BunServices } from "@effect/platform-bun";
import { layer as testLayer } from "@effect/vitest";
import { FetchHttpClient } from "effect/http";
import { Effect } from "effect";

import { expect } from "vitest";
import { HttpRouter } from "effect/http";
import { app } from "../server/app.ts";
import { applicationLayer, sqliteStorage } from "../server/host.ts";
import { Layer } from "effect";

testLayer(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer), { excludeTestServices: true })(
  (it) => {
    it.effect("tracker streams expose declared streams with host access rules", () =>
      Effect.gen(function* () {
        const host = applicationLayer(sqliteStorage(":memory:"), ["live"]);

        const web = HttpRouter.toWebHandler(
          app(["live"]).pipe(Layer.provide(host), HttpRouter.provideRequest(host)),
          { disableLogger: true },
        );

        yield* Effect.addFinalizer(() => Effect.promise(() => web.dispose()).pipe(Effect.asVoid));

        expect(
          (yield* Effect.promise(() =>
            web.handler(new Request("http://host/streams/bugsy/live/issue-events?offset=-1")),
          )).status,
        ).toBe(200);

        for (const method of ["GET", "PUT", "POST", "DELETE"]) {
          expect(
            (yield* Effect.promise(() =>
              web.handler(new Request("http://host/streams/private/secret", { method })),
            )).status,
          ).toBe(404);
        }

        for (const name of [
          "issue-events",
          "issue-label-events",
          "projects",
          "users",
          "labels",
          "outputs/board",
          "outputs/label-counts",
          "outputs/transitions",
        ]) {
          for (const method of ["PUT", "POST", "DELETE"]) {
            expect(
              (yield* Effect.promise(() =>
                web.handler(new Request(`http://host/streams/bugsy/live/${name}`, { method })),
              )).status,
            ).toBe(405);
          }
        }
      }),
    );
  },
);

import { expect, test } from "bun:test";
import { HttpRouter } from "effect/http";
import { app } from "../server/app.ts";
import { applicationLayer, sqliteStorage } from "../server/host.ts";
import { Layer } from "effect";

test("tracker streams expose declared streams with host access rules", async () => {
  const host = applicationLayer(sqliteStorage(":memory:"), ["live"]);

  const web = HttpRouter.toWebHandler(
    app(["live"]).pipe(Layer.provide(host), HttpRouter.provideRequest(host)),
    { disableLogger: true },
  );

  try {
    expect(
      (await web.handler(new Request("http://host/streams/bugsy/live/issue-events?offset=-1")))
        .status,
    ).toBe(200);

    for (const method of ["GET", "PUT", "POST", "DELETE"]) {
      expect(
        (await web.handler(new Request("http://host/streams/private/secret", { method }))).status,
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
          (await web.handler(new Request(`http://host/streams/bugsy/live/${name}`, { method })))
            .status,
        ).toBe(405);
      }
    }
  } finally {
    await web.dispose();
  }
});

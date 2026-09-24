import { expect, test } from "bun:test";
import { HttpRouter } from "effect/unstable/http";
import { app } from "../server/app.ts";
import { applicationLayer } from "../server/host.ts";
import { Layer } from "effect";

test("tracker streams expose declared families with host access rules", async () => {
  const host = applicationLayer(":memory:", ["live"]);
  const web = HttpRouter.toWebHandler(
    app(["live"]).pipe(Layer.provide(host), HttpRouter.provideRequest(host)),
    { disableLogger: true },
  );
  try {
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
          (
            await web.handler(
              new Request(`http://host/streams/issue-tracker/live/${name}`, { method }),
            )
          ).status,
        ).toBe(405);
      }
    }
  } finally {
    await web.dispose();
  }
});

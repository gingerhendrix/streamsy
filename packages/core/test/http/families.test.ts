import { expect, it } from "bun:test";
import { Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { Http, StreamFamily, Memory } from "../../src/index.ts";

class Inputs extends StreamFamily.bytes<Inputs>()("Inputs", "inputs/:id", {
  params: { id: Schema.String },
}) {}
class Outputs extends StreamFamily.bytes<Outputs>()("Outputs", "outputs/:id", {
  params: { id: Schema.String },
}) {}

it("HTTP chooses family ownership and access before entering the complete protocol program", async () => {
  const edge = HttpRouter.toWebHandler(
    Http.routes(
      [
        { family: Inputs, access: "full" },
        { family: Outputs, access: "read" },
      ],
      { prefix: "/streams" },
    ).pipe(HttpRouter.provideRequest(Memory.layer({}, Inputs, Outputs))),
    { disableLogger: true },
  );
  const request = (path: string, method = "GET", body?: string) =>
    edge.handler(
      new Request(`http://host/streams/${path}`, {
        method,
        // GET and HEAD callers pass no body; only PUT test requests carry data.
        // oxlint-disable-next-line unicorn/no-invalid-fetch-options -- The helper only supplies body for PUT.
        body,
        headers: { "content-type": "application/octet-stream" },
      }),
    );
  try {
    expect((await request("private/secret")).status).toBe(404);
    for (const method of ["PUT", "POST", "DELETE"])
      expect((await request("outputs/a", method)).status).toBe(405);
    expect((await request("inputs/a", "PUT", "hello")).status).toBe(201);
    expect((await request("outputs/a", "POST")).headers.get("allow")).toBe("GET, HEAD, OPTIONS");
    expect(
      (await request("outputs/a", "OPTIONS")).headers.get("access-control-allow-methods"),
    ).toBe("GET, HEAD, OPTIONS");
    for (const source of ["/streams/private/secret", "/streams/outputs/a"]) {
      const fork = await edge.handler(
        new Request("http://host/streams/inputs/fork", {
          method: "PUT",
          headers: { "stream-forked-from": source },
        }),
      );
      expect(fork.status).toBe(404);
      expect(await fork.text()).toContain("Source stream not found");
    }
    const head = await request("inputs/a", "HEAD");
    expect(head.status).toBe(200);
    expect(head.headers.get("stream-next-offset")).not.toBeNull();
    const read = await request("inputs/a?offset=-1");
    expect(read.status).toBe(200);
    expect(await read.text()).toBe("hello");
  } finally {
    await edge.dispose();
  }
});

it("append access allows POST but refuses creation and deletion", async () => {
  const families = Memory.layer({}, Inputs);
  const seeded = Layer.effectDiscard(
    Effect.flatMap(Inputs, (inputs) => inputs.create({ id: "a" })),
  ).pipe(Layer.provideMerge(families));
  const edge = HttpRouter.toWebHandler(
    Http.routes([{ family: Inputs, access: "append" }]).pipe(HttpRouter.provideRequest(seeded)),
    { disableLogger: true },
  );
  try {
    for (const method of ["PUT", "DELETE"])
      expect((await edge.handler(new Request("http://host/inputs/a", { method }))).status).toBe(
        405,
      );
    expect(
      (
        await edge.handler(
          new Request("http://host/inputs/a", {
            method: "POST",
            body: "hello",
            headers: { "content-type": "application/octet-stream" },
          }),
        )
      ).status,
    ).toBe(204);
    expect(await (await edge.handler(new Request("http://host/inputs/a?offset=-1"))).text()).toBe(
      "hello",
    );
  } finally {
    await edge.dispose();
  }
});

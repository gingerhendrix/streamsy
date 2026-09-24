import { expect, it } from "bun:test";
import { Schema } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { Http, StreamFamily, Streams, provides } from "../../src/index.ts";

class Inputs extends StreamFamily.bytes<Inputs>()("inputs/:id", {
  params: { id: Schema.String },
}) {}
class Outputs extends StreamFamily.bytes<Outputs>()("outputs/:id", {
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
    ).pipe(HttpRouter.provideRequest(provides(Streams.layerMemory(), Inputs, Outputs))),
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
  const edge = HttpRouter.toWebHandler(
    Http.routes([{ family: Inputs, access: "append" }]).pipe(
      HttpRouter.provideRequest(provides(Streams.layerMemory(), Inputs)),
    ),
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
    ).toBe(404);
  } finally {
    await edge.dispose();
  }
});

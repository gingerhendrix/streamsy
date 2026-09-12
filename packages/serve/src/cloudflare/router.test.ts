/* oxlint-disable effecttsgo/async-function, anti-slop/no-chained-type-assertions -- The fake namespace and Bun Fetch boundary intentionally narrow Cloudflare test doubles. */
import { expect, test } from "bun:test";
import type { DurableObjectNamespace, ExportedHandler } from "@cloudflare/workers-types";
import { Schema } from "effect";
import { StreamRoute } from "@streamsy/core";
import { Placement, rule } from "./placement.ts";
import { router } from "./router.ts";

type FakeNamespace = DurableObjectNamespace & {
  readonly names: Array<string>;
  readonly requests: Array<Request>;
};

const makeNamespace = (): FakeNamespace => {
  const names: Array<string> = [];
  const requests: Array<Request> = [];
  const namespace = {
    names,
    requests,
    idFromName(name: string) {
      names.push(name);
      return { name };
    },
    get(id: { readonly name: string }) {
      return {
        fetch(request: Request) {
          requests.push(request);
          return Promise.resolve(new Response(id.name, { status: 200 }));
        },
      };
    },
  };
  // SAFETY: The router only uses idFromName, get and the returned stub's fetch;
  // the fake deliberately leaves unrelated Cloudflare namespace methods out.
  return namespace as FakeNamespace;
};

const invoke = (handler: ExportedHandler<unknown>, request: Request) =>
  handler.fetch?.(request, {}, {});

test("router uses the raw stripped stream path and forwards the unchanged request", async () => {
  const namespace = makeNamespace();
  const handler = router({ namespace: () => namespace });
  const request = new Request("https://streams.test/a%2Fb?offset=-1", {
    method: "POST",
    body: "payload",
  });

  const response = await invoke(handler, request);
  expect(response?.status).toBe(200);
  expect(namespace.names).toEqual(["a%2Fb"]);
  expect(namespace.requests[0]).toBe(request);
});

test("byKey routes a fork request to the child object without consulting the source key", async () => {
  const namespace = makeNamespace();
  const handler = router({
    namespace: () => namespace,
    placement: Placement.byKey((streamPath) => streamPath.split("/", 1)[0] ?? ""),
  });

  const same = await invoke(
    handler,
    new Request("https://streams.test/t1/y", {
      method: "PUT",
      headers: { "stream-forked-from": "/t1/x" },
    }),
  );
  expect(same?.status).toBe(200);
  expect(namespace.names).toEqual(["t1"]);

  const cross = await invoke(
    handler,
    new Request("https://streams.test/t2/z", {
      method: "PUT",
      headers: { "stream-forked-from": "/t1/x" },
    }),
  );
  expect(cross?.status).toBe(200);
  expect(cross?.headers.get("stream-not-supported")).toBeNull();
  expect(await cross?.text()).toBe("t2");
  expect(namespace.names).toEqual(["t1", "t2"]);
});

test("router follows the core prefix grammar and reports every invalid placement path", async () => {
  const namespace = makeNamespace();
  const handler = router({ namespace: () => namespace, pathPrefix: "/streams" });
  for (const path of ["/streams", "/streams/", "/other/x"]) {
    const response = await invoke(handler, new Request(`https://streams.test${path}`));
    expect(response?.status).toBe(400);
    expect(await response?.text()).toBe("Stream path required: /streams/{path}");
  }

  const routed = await invoke(handler, new Request("https://streams.test/streams/a/b?offset=-1"));
  expect(routed?.status).toBe(200);
  expect(namespace.names).toEqual(["a/b"]);
});

test("placement defects are 500 and empty or non-string keys are 400", async () => {
  const throwing = router({
    namespace: () => makeNamespace(),
    placement: Placement.byKey(() => {
      throw new Error("defect");
    }),
  });
  const defect = await invoke(throwing, new Request("https://streams.test/a"));
  expect(defect?.status).toBe(500);

  const invalid = router({
    namespace: () => makeNamespace(),
    placement: Placement.byKey(() => ""),
  });
  const empty = await invoke(invalid, new Request("https://streams.test/a"));
  expect(empty?.status).toBe(400);
  expect(await empty?.text()).toBe("Invalid placement key");

  const nonString = router({
    namespace: () => makeNamespace(),
    // SAFETY: This intentionally violates Placement's type to exercise the runtime boundary.
    placement: Placement.byKey(
      () =>
        /* SAFETY: This intentionally violates Placement's type to exercise the runtime boundary. */ 7 as never,
    ),
  });
  const number = await invoke(nonString, new Request("https://streams.test/a"));
  expect(number?.status).toBe(400);
  expect(await number?.text()).toBe("Invalid placement key");
});

const Note = Schema.Struct({ text: Schema.String });
const journal = StreamRoute.json("journal/:user", {
  params: { user: Schema.String },
  schema: Note,
});
const totals = StreamRoute.json("totals/:user", {
  params: { user: Schema.String },
  schema: Note,
});
const orders = StreamRoute.json("orders/:orderId", {
  params: { orderId: Schema.NumberFromString.pipe(Schema.check(Schema.isGreaterThan(0))) },
  schema: Note,
});

test("byRoute names the object from the route parameters", async () => {
  const namespace = makeNamespace();
  const handler = router({
    namespace: () => namespace,
    placement: Placement.byRoute([
      rule({ route: journal, owner: ({ user }) => `user/${user}` }),
      rule({ route: totals, owner: ({ user }) => `user/${user}` }),
    ]),
  });

  const read = await invoke(handler, new Request("https://streams.test/journal/ann"));
  expect(read?.status).toBe(200);
  expect(namespace.names).toEqual(["user/ann"]);

  const totalsRead = await invoke(handler, new Request("https://streams.test/totals/ann/x"));
  expect(totalsRead?.status).toBe(400);
  expect(namespace.names).toEqual(["user/ann"]);

  const other = await invoke(handler, new Request("https://streams.test/totals/ann"));
  expect(other?.status).toBe(200);
  expect(namespace.names).toEqual(["user/ann", "user/ann"]);
});

test("a constrained codec reaches the owner as its decoded type", async () => {
  const namespace = makeNamespace();
  const handler = router({
    namespace: () => namespace,
    placement: Placement.byRoute([
      rule({ route: orders, owner: ({ orderId }) => `order/${orderId}` }),
    ]),
  });

  expect((await invoke(handler, new Request("https://streams.test/orders/7")))?.status).toBe(200);
  expect(namespace.names).toEqual(["order/7"]);

  // A parameter its codec rejects falls through, so no rule owns the path.
  const invalid = await invoke(handler, new Request("https://streams.test/orders/0"));
  expect(invalid?.status).toBe(400);
  expect(await invalid?.text()).toBe("Invalid placement key");
  expect(namespace.names).toEqual(["order/7"]);
});

test("an unmatched path is 400 and a throwing owner is 500", async () => {
  const namespace = makeNamespace();
  const unmatched = router({
    namespace: () => namespace,
    placement: Placement.byRoute([rule({ route: journal, owner: ({ user }) => user })]),
  });
  const missing = await invoke(unmatched, new Request("https://streams.test/drafts/ann"));
  expect(missing?.status).toBe(400);
  expect(await missing?.text()).toBe("Invalid placement key");
  expect(namespace.names).toEqual([]);

  const throwing = router({
    namespace: () => namespace,
    placement: Placement.byRoute([
      rule({
        route: journal,
        owner: () => {
          throw new Error("defect");
        },
      }),
    ]),
  });
  const defect = await invoke(throwing, new Request("https://streams.test/journal/ann"));
  expect(defect?.status).toBe(500);
});

test("byRoute routes a fork to the child's family object", async () => {
  const namespace = makeNamespace();
  const handler = router({
    namespace: () => namespace,
    placement: Placement.byRoute([rule({ route: journal, owner: ({ user }) => `user/${user}` })]),
  });

  const response = await invoke(
    handler,
    new Request("https://streams.test/journal/bo", {
      method: "PUT",
      headers: { "stream-forked-from": "/journal/ann" },
    }),
  );
  expect(response?.status).toBe(200);
  expect(await response?.text()).toBe("user/bo");
  expect(namespace.names).toEqual(["user/bo"]);
});

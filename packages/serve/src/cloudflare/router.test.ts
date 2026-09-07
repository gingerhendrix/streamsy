/* oxlint-disable effecttsgo/async-function -- Web router tests await the real Fetch boundary. */
import { expect, test } from "bun:test";
import { Placement } from "./placement.ts";
import { router } from "./router.ts";

const makeNamespace = () => {
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
  return namespace;
};

test("router uses the raw stripped stream path and forwards the unchanged request", async () => {
  const namespace = makeNamespace();
  const handler = router({ namespace: () => namespace });
  const request = new Request("https://streams.test/a%2Fb?offset=-1", {
    method: "POST",
    body: "payload",
  });

  const response = await handler.fetch?.(request, {});
  expect(response?.status).toBe(200);
  expect(namespace.names).toEqual(["a%2Fb"]);
  expect(namespace.requests[0]).toBe(request);
});

test("byKey co-locates same-family streams and refuses a cross-family fork", async () => {
  const namespace = makeNamespace();
  const handler = router({
    namespace: () => namespace,
    placement: Placement.byKey((streamPath) => streamPath.split("/", 1)[0] ?? ""),
  });

  const same = await handler.fetch?.(
    new Request("https://streams.test/t1/y", {
      method: "PUT",
      headers: { "stream-forked-from": "/t1/x" },
    }),
    {},
  );
  expect(same?.status).toBe(200);
  expect(namespace.names).toEqual(["t1"]);

  const cross = await handler.fetch?.(
    new Request("https://streams.test/t2/z", {
      method: "PUT",
      headers: { "stream-forked-from": "/t1/x" },
    }),
    {},
  );
  expect(cross?.status).toBe(400);
  expect(cross?.headers.get("stream-not-supported")).toBe("fork");
  expect(await cross?.text()).toBe("Feature not supported: fork");
  expect(namespace.names).toEqual(["t1"]);
});

test("router follows the core prefix grammar and reports every invalid placement path", async () => {
  const namespace = makeNamespace();
  const handler = router({ namespace: () => namespace, pathPrefix: "/streams" });
  for (const path of ["/streams", "/streams/", "/other/x"]) {
    const response = await handler.fetch?.(new Request(`https://streams.test${path}`), {});
    expect(response?.status).toBe(400);
    expect(await response?.text()).toBe("Stream path required: /streams/{path}");
  }

  const routed = await handler.fetch?.(
    new Request("https://streams.test/streams/a/b?offset=-1"),
    {},
  );
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
  const defect = await throwing.fetch?.(new Request("https://streams.test/a"), {});
  expect(defect?.status).toBe(500);

  const invalid = router({
    namespace: () => makeNamespace(),
    placement: Placement.byKey(() => ""),
  });
  const empty = await invalid.fetch?.(new Request("https://streams.test/a"), {});
  expect(empty?.status).toBe(400);
  expect(await empty?.text()).toBe("Invalid placement key");

  const nonString = router({
    namespace: () => makeNamespace(),
    // SAFETY: This intentionally violates Placement's type to exercise the runtime boundary.
    placement: Placement.byKey(() => 7 as never),
  });
  const number = await nonString.fetch?.(new Request("https://streams.test/a"), {});
  expect(number?.status).toBe(400);
  expect(await number?.text()).toBe("Invalid placement key");
});

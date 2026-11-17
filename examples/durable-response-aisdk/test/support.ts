/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/crypto-random-uuid -- Black-box integration harness owns native HTTP and framework adapters. */
import { Effect, ManagedRuntime, Schema } from "effect";
import { createDurableChatTransport } from "@durable-streams/aisdk-transport";
import { server } from "../src/server.ts";

export const boot = async (paceMs = 20, database = ":memory:") => {
  const reservation = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = reservation.port;

  if (port === undefined) throw Error("No port");
  await reservation.stop(true);
  const runtime = ManagedRuntime.make(server({ port, database, paceMs }));
  await runtime.runPromise(Effect.void);
  const base = `http://127.0.0.1:${port}`;

  const post = (path: string, data: Schema.Json) =>
    fetch(`${base}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(data),
    });

  const start = async (id: string = crypto.randomUUID()) => {
    const response = await post("/api/chat", {
      id,
      messages: [{ role: "user", parts: [{ type: "text", text: "Explain durable streams" }] }],
    });

    if (response.status !== 201)
      throw Error(`Start failed: ${response.status} ${await response.text()}`);

    return id;
  };

  const transport = (id: string) =>
    createDurableChatTransport({
      api: `${base}/api/chat`,
      reconnectApi: `${base}/api/chat/${id}/replay`,
    });

  const raw = async (id: string, offset = "-1") => {
    const response = await fetch(`${base}/responses/${id}?offset=${encodeURIComponent(offset)}`);

    return {
      status: response.status,
      body: await response.text(),
      offset: response.headers.get("stream-next-offset"),
      closed: response.headers.get("stream-closed"),
    };
  };

  return { base, post, start, transport, raw, dispose: () => runtime.dispose() };
};

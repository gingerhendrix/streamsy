import { Streams } from "@streamsy/core";
import * as BunHost from "@streamsy/serve/bun";

const host = await BunHost.serve({ layer: Streams.layerMemory(), port: 0 });
try {
  const response = await fetch(new URL("/streams/events", host.url), {
    method: "PUT",
    headers: { "content-type": "application/json" },
  });
  if (response.status !== 201) throw new Error(`Create failed: ${response.status}`);
} finally {
  await host.stop();
}

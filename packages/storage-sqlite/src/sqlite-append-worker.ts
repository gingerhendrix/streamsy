import { existsSync, writeFileSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";
import { StreamProtocol } from "@streamsy/core";
import { createSqliteStreamFactory } from "./index.ts";

const [dbPath, streamId, label, countRaw, readyPath, startPath] = process.argv.slice(2);

if (!dbPath || !streamId || !label || !countRaw || !readyPath || !startPath) {
  console.error("usage: sqlite-append-worker <db> <stream> <label> <count> <ready> <start>");
  process.exit(2);
}

const count = Number(countRaw);
const encoder = new TextEncoder();
const factory = createSqliteStreamFactory({ filename: dbPath, busyTimeoutMs: 10_000 });
const protocol = new StreamProtocol({ storage: { factory } });

writeFileSync(readyPath, "ready");
while (!existsSync(startPath)) await delay(5);

const lookup = await protocol.get(streamId);
if (lookup.status !== "ok") {
  console.error(`lookup failed: ${lookup.status}`);
  process.exit(1);
}

for (let i = 0; i < count; i++) {
  const result = await lookup.stream.append({
    contentType: "text/plain",
    data: encoder.encode(`${label}-${i}`),
  });
  if (result.status !== "appended") {
    console.error(`append failed: ${JSON.stringify(result)}`);
    process.exit(1);
  }
}

factory.close();

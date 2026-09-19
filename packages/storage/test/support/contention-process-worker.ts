/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console -- Executable Bun process boundary writes its readiness protocol to stdout. */
import { Database } from "bun:sqlite";

const mode = Bun.argv[2];
const filename = Bun.argv[3];
const holdMs = Number(Bun.argv[4]);
if (
  (mode !== "write" && mode !== "read" && mode !== "exclusive") ||
  filename === undefined ||
  !Number.isFinite(holdMs)
)
  throw new Error("Usage: contention-process-worker.ts <write|read|exclusive> <filename> <holdMs>");

const database = new Database(filename, { create: true, readwrite: true });
database.run("PRAGMA busy_timeout=0");
database.run("CREATE TABLE IF NOT EXISTS batch_c_lock(value INTEGER NOT NULL)");
if (
  database.query<{ count: number }, []>("SELECT COUNT(*) count FROM batch_c_lock").get()?.count ===
  0
)
  database.run("INSERT INTO batch_c_lock VALUES (0)");
database.run(
  mode === "exclusive" ? "BEGIN EXCLUSIVE" : mode === "write" ? "BEGIN IMMEDIATE" : "BEGIN",
);
if (mode !== "read") database.run("UPDATE batch_c_lock SET value=value+1");
else database.query("SELECT * FROM batch_c_lock").all();
console.log("locked");
await Bun.sleep(holdMs);
database.run("COMMIT");
database.close(false);

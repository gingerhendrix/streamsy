/**
 * Shared test fixtures.
 *
 * Every suite drives the same `createLocalHost` the executable edge uses, so
 * nothing here is a second implementation of the application.
 */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- Restart tests need real package-external temporary directories for on-disk SQLite hosts.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- This path join constructs a file-backed test fixture location, not application I/O.
import { join } from "node:path";
import { Schema } from "effect";
import { createLocalHost, type LocalHostOptions } from "../server/host/bun/local.ts";

export type Host = ReturnType<typeof createLocalHost>;

export function temporaryDirectory(prefix: string): string {
  return mkdtempSync(join(tmpdir(), `${prefix}-`));
}

export function host(options: LocalHostOptions = {}): Host {
  return createLocalHost(options);
}

export function call(
  target: Host,
  method: string,
  path: string,
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Tests intentionally send malformed external request bodies through this trust boundary.
  body?: unknown,
): Promise<Response> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { "content-type": "application/json" };
  }
  return target.fetch(new Request(`http://localhost${path}`, init));
}

/** Decode a response through the declared wire contract, as a client would. */
export function json<S extends Schema.ConstraintDecoder<unknown>>(
  response: Response,
  schema: S,
): Promise<S["Type"]> {
  return response.text().then((text) => {
    if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 500)}`);
    return Schema.decodePromise(Schema.fromJsonString(schema))(text);
  });
}

/** The create-issue body a test sends. `status` is optional, as the contract says. */
export interface CreateIssueBody {
  commandId: string;
  issueId: string;
  projectId: string;
  title: string;
  status?: string;
}

export const createIssueBody = (
  commandId: string,
  issueId: string,
  title: string,
  status?: string,
): CreateIssueBody => {
  const body: CreateIssueBody = { commandId, issueId, projectId: "streamsy", title };
  if (status !== undefined) body.status = status;
  return body;
};

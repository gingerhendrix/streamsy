/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- `bun:test` owns these fixtures' control flow, and the restart fixture needs real on-disk databases, so it uses the Node-compatible filesystem and path APIs. */
/* oxlint-disable anti-slop/no-unknown-parameters, anti-slop/require-safety-comment-for-type-assertion -- A request body is arbitrary JSON on purpose, so malformed-body rejection can be driven; `json()` decodes every response through a declared Schema and the assertion only names what that decode produced. */
/**
 * Shared test fixtures.
 *
 * Every suite drives the same `createLocalHost` the executable edge uses, so
 * nothing here is a second implementation of the application.
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Schema } from "effect";
import { createLocalHost, type LocalHostOptions } from "../server/local.ts";

export type Host = ReturnType<typeof createLocalHost>;

export function temporaryDirectory(prefix: string): string {
  return mkdtempSync(join(tmpdir(), `${prefix}-`));
}

export function host(options: LocalHostOptions = {}): Host {
  return createLocalHost(options);
}

export async function call(
  target: Host,
  method: string,
  path: string,
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
export async function json<S extends Schema.ConstraintDecoder<unknown>>(
  response: Response,
  schema: S,
): Promise<S["Type"]> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 500)}`);
  return Schema.decodeUnknownSync(schema)(JSON.parse(text)) as S["Type"];
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

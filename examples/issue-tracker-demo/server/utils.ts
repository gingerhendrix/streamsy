import type { ZodError } from "zod";
import type { JsonValue } from "@streamsy/core";
import {
  jsonObjectSchema,
  txIdSchema,
  type MutationBody,
  type TxId,
} from "../shared/state-schema.ts";

export type { TxId } from "../shared/state-schema.ts";

/** One accepted mutation request: its untrusted body and its optional txid. */
export interface Mutation {
  body: MutationBody;
  txid: TxId | undefined;
}

type JsonResponseData = JsonValue | Readonly<Record<string, JsonValue | undefined>>;

/**
 * Read the mutation envelope: a JSON object body plus, when the client sent
 * one, a well-formed txid. Malformed envelopes answer 400 rather than failing
 * the request as an unhandled 500 (invalid JSON) or travelling into an event
 * as a value the wire shape does not allow (bad txid).
 */
export async function readMutation(request: Request): Promise<Mutation | Response> {
  let payload: JsonValue;
  try {
    payload = await request.json();
  } catch {
    return badRequest("Invalid JSON body");
  }
  const body = jsonObjectSchema.safeParse(payload);
  if (!body.success) return badRequest("Body must be a JSON object");
  if (body.data.txid === undefined) return { body: body.data, txid: undefined };
  const txid = txIdSchema.safeParse(body.data.txid);
  if (!txid.success) return badRequest("Invalid txid");
  return { body: body.data, txid: txid.data };
}

/** 400 for a body whose fields do not match the entity's wire schema. */
export function invalidBody(entity: string, error: ZodError): Response {
  const detail = error.issues
    .map((issue) => `${issue.path.join(".") || "body"}: ${issue.message}`)
    .join("; ");
  return badRequest(`Invalid ${entity} body — ${detail}`);
}

export function json(data: JsonResponseData, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  if (!headers.has("content-type")) headers.set("content-type", "application/json");
  return new Response(JSON.stringify(data, null, 2), {
    ...init,
    headers,
  });
}

export function notFound(message = "Not found"): Response {
  return json({ error: message }, { status: 404 });
}

export function badRequest(message: string): Response {
  return json({ error: message }, { status: 400 });
}

export function conflict(message: string): Response {
  return json({ error: message }, { status: 409 });
}

export function now(): string {
  return new Date().toISOString();
}

export function id(prefix: string): string {
  return `${prefix}_${crypto.randomUUID().slice(0, 8)}`;
}

import type { ZodError } from "zod";

export type TxId = `${string}-${string}-${string}-${string}-${string}`;

/** The untrusted JSON object body of a mutation request. */
export type MutationBody = Readonly<Record<string, unknown>>;

/** A client-supplied transaction id: the `a-b-c-d-e` shape of {@link TxId}. */
export function isTxId(value: unknown): value is TxId {
  return typeof value === "string" && value.split("-").length === 5;
}

function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** One accepted mutation request: its untrusted body and its optional txid. */
export interface Mutation {
  body: MutationBody;
  txid: TxId | undefined;
}

/**
 * Read the mutation envelope: a JSON object body plus, when the client sent
 * one, a well-formed txid. Malformed envelopes answer 400 rather than failing
 * the request as an unhandled 500 (invalid JSON) or travelling into an event
 * as a value the wire shape does not allow (bad txid).
 */
export async function readMutation(request: Request): Promise<Mutation | Response> {
  let payload: unknown;
  try {
    payload = await request.json();
  } catch {
    return badRequest("Invalid JSON body");
  }
  if (!isJsonObject(payload)) return badRequest("Body must be a JSON object");
  if (payload.txid === undefined) return { body: payload, txid: undefined };
  if (!isTxId(payload.txid)) return badRequest("Invalid txid");
  return { body: payload, txid: payload.txid };
}

/** 400 for a body whose fields do not match the entity's wire schema. */
export function invalidBody(entity: string, error: ZodError): Response {
  const detail = error.issues
    .map((issue) => `${issue.path.join(".") || "body"}: ${issue.message}`)
    .join("; ");
  return badRequest(`Invalid ${entity} body — ${detail}`);
}

export function json(data: unknown, init: ResponseInit = {}): Response {
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

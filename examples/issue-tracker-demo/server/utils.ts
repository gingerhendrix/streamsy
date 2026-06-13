export type TxId = `${string}-${string}-${string}-${string}-${string}`;

/** JSON body accepted by mutation endpoints: a partial entity plus an optional client txid. */
export type MutationBody<T> = Partial<T> & { txid?: TxId };

export function json(data: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(data, null, 2), {
    ...init,
    headers: {
      "content-type": "application/json",
      ...init.headers,
    },
  });
}

export function notFound(message = "Not found"): Response {
  return json({ error: message }, { status: 404 });
}

export function badRequest(message: string): Response {
  return json({ error: message }, { status: 400 });
}

export function now(): string {
  return new Date().toISOString();
}

export function id(prefix: string): string {
  return `${prefix}_${crypto.randomUUID().slice(0, 8)}`;
}

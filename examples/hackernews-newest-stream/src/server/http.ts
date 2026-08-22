import { streamContentType } from "./config.ts";

export function json(value: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(value, null, 2), {
    ...init,
    headers: {
      "content-type": streamContentType,
      ...init.headers,
    },
  });
}

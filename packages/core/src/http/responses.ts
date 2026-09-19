/**
 * Response factory for the HTTP layer.
 *
 * Centralizes security headers and the small set of plain text/empty
 * responses that recur across handlers (400/404/410/413/etc.). Method-
 * specific result mapping (append producer-state shaping, create conflict
 * mapping, catch-up ETag responses) stays in the owning service.
 */

const CACHE_CONTROL_NO_STORE = "no-store";
const CACHE_CONTROL_PUBLIC = "public, max-age=60, stale-while-revalidate=300";
const CACHE_CONTROL_PRIVATE = "private, max-age=60, stale-while-revalidate=300";

export function cacheControlForVisibility(visibility: "private" | "public"): string {
  return visibility === "public" ? CACHE_CONTROL_PUBLIC : CACHE_CONTROL_PRIVATE;
}

export function noStore(response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set("cache-control", CACHE_CONTROL_NO_STORE);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export function text(
  message: string | null,
  status: number,
  headers?: ConstructorParameters<typeof Headers>[0],
): Response {
  return new Response(message, { status, headers });
}

export function empty(
  status: number,
  headers?: ConstructorParameters<typeof Headers>[0],
): Response {
  return new Response(null, { status, headers });
}

export function badRequest(message: string): Response {
  return new Response(message, { status: 400 });
}

export function notFound(message = "Stream not found"): Response {
  return new Response(message, { status: 404 });
}

export function gone(message = "Stream is soft-deleted"): Response {
  return new Response(message, { status: 410 });
}

export function conflict(
  message: string | null,
  headers?: ConstructorParameters<typeof Headers>[0],
): Response {
  return new Response(message, { status: 409, headers });
}

export function payloadTooLarge(): Response {
  return new Response("Payload too large", { status: 413 });
}

export function invalidJson(): Response {
  return new Response("Invalid JSON", { status: 400 });
}

export function methodNotAllowed(): Response {
  return new Response("Method not allowed", { status: 405 });
}

export function internalError(): Response {
  return new Response("Internal server error", { status: 500 });
}

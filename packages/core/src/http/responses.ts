/**
 * Response factory for the HTTP layer.
 *
 * Centralizes security headers and the small set of plain text/empty
 * responses that recur across handlers (400/404/410/413/etc.). Method-
 * specific result mapping (append producer-state shaping, create conflict
 * mapping, catch-up ETag responses) stays in the owning service.
 */

export const CACHE_CONTROL_NO_STORE = "no-store";
export const CACHE_CONTROL_PUBLIC = "public, max-age=60, stale-while-revalidate=300";
export const CACHE_NO_STORE = CACHE_CONTROL_NO_STORE;
export const CACHE_REVALIDATE = CACHE_CONTROL_PUBLIC;

export class HttpResponseFactory {
  secure(response: Response): Response {
    return this.withSecurityHeaders(response);
  }

  text(message: string | null, status: number, headers?: HeadersInit): Response {
    return new Response(message, { status, headers });
  }

  empty(status: number, headers?: HeadersInit): Response {
    return new Response(null, { status, headers });
  }

  badRequest(message: string): Response {
    return new Response(message, { status: 400 });
  }

  notFound(message = "Stream not found"): Response {
    return new Response(message, { status: 404 });
  }

  gone(message = "Stream is soft-deleted"): Response {
    return new Response(message, { status: 410 });
  }

  conflict(message: string | null, headers?: HeadersInit): Response {
    return new Response(message, { status: 409, headers });
  }

  payloadTooLarge(): Response {
    return new Response("Payload too large", { status: 413 });
  }

  invalidJson(): Response {
    return new Response("Invalid JSON", { status: 400 });
  }

  methodNotAllowed(): Response {
    return new Response("Method not allowed", { status: 405 });
  }

  internalError(): Response {
    return new Response("Internal server error", { status: 500 });
  }

  /**
   * Wrap a response with the standard security headers used across every
   * route. Existing values for the same header names are preserved.
   */
  withSecurityHeaders(response: Response): Response {
    const headers = new Headers(response.headers);
    if (!headers.has("x-content-type-options")) {
      headers.set("x-content-type-options", "nosniff");
    }
    if (!headers.has("cross-origin-resource-policy")) {
      headers.set("cross-origin-resource-policy", "cross-origin");
    }
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  }
}

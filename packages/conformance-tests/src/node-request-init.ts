/**
 * `RequestInit` for the Node fallback servers in the conformance suites.
 *
 * These suites compile against `@cloudflare/workers-types`, whose `RequestInit`
 * does not model undici's `duplex` member, but the fallback servers run on Node
 * and set it when forwarding a request body. Widening the type states that one
 * platform gap in a single place, so the request literal is checked against the
 * Cloudflare fields it does use instead of being asserted into a type it does
 * not satisfy.
 */
export type NodeRequestInit = RequestInit & { duplex?: "half" };

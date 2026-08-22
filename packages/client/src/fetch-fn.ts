/**
 * Helpers for producing values of the exact `typeof globalThis.fetch` type.
 *
 * A host may hang extra members on the `fetch` global — Bun declares
 * `fetch.preconnect` — so a bare `(input, init) => Promise<Response>` arrow is
 * not assignable to `typeof globalThis.fetch` under every type environment. The
 * helpers below copy the host global's own properties onto the wrapper, which
 * both satisfies the type at compile time and carries those members through at
 * runtime, rather than asserting the difference away.
 */

/**
 * Wraps `target` in a fresh function that forwards every call to it.
 *
 * The indirection keeps `globalThis` as the receiver, so a host whose `fetch`
 * rejects a detached `this` (browsers) still works when the wrapper is passed
 * to a retry decorator.
 */
export function wrapFetch(target: typeof globalThis.fetch): typeof globalThis.fetch {
  const forward = (...args: Parameters<typeof globalThis.fetch>) => target(...args);
  return Object.assign(forward, target);
}

/**
 * Completes a partial fetch implementation into a full `typeof globalThis.fetch`
 * by copying the host global's own properties onto it. Intended for test
 * doubles, which supply only the call signature.
 */
export function asFetch(
  impl: (...args: Parameters<typeof globalThis.fetch>) => Promise<Response>,
): typeof globalThis.fetch {
  return Object.assign(impl, globalThis.fetch);
}

---
packages:
  "group:streamsy": minor
---

## Create rejects invalid options and JSON with typed errors

`CreateError` gains `InvalidCreateRequest`, a minor change to the public error union. Code that exhaustively matches `CreateError` must handle the new member. The error carries `id` and `message`.

- A remote plain create HTTP 400 response now fails with `InvalidCreateRequest` and the response message instead of `InvalidForkRequest`. Responses with `stream-not-supported` still fail with `NotSupported`, and fork HTTP 400 responses still fail with `InvalidForkRequest`.
- Local and remote creates reject `ttlSeconds` combined with `expiresAt`, and reject an `expiresAt` value that `DateTime.make` cannot parse. Both fail with `InvalidCreateRequest` before storage access or an HTTP request. Local creates previously used the TTL when both were set and could create a stream that never expired for an invalid date. HTTP continues to return 400 with the same messages.
- Invalid JSON `initialData` on a plain create, including typed toolkit create, now fails with `InvalidCreateRequest` and `Invalid JSON` instead of dying with `SyntaxError`. Fork initial data fails with `InvalidForkRequest` and the same message. Append framing and its invalid JSON behavior are unchanged.

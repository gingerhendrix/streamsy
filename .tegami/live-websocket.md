---
packages:
  "@streamsy/core": minor
  "@streamsy/serve": minor
---

## Follow streams over hibernatable WebSockets on Cloudflare

A Durable Object made with `StreamsyObject.make` can now serve `live=websocket`, a Streamsy extension to Durable Streams HTTP. Set `live: { websocket: true }`, or `live: { websocket: { prefix: "/streams" } }` when the routes have a prefix. The object accepts the socket with the WebSocket Hibernation API, keeps each socket's offset in its attachment, and pushes the new messages in the event that committed them. An object whose readers all hold sockets can hibernate between commits.

Each `data` frame holds the exact body of a catch-up `GET`, and a `control` frame with the SSE control fields follows it. The option is off by default, and an object without it behaves as before.

`Http.read` and `Http.app` now answer `live=websocket` with `426 Upgrade Required` on every host that does not take the upgrade, so a client falls back to long-poll. `@streamsy/core/http` exports the frame helpers and close codes. See [Follow streams over WebSockets](https://streamsy.dev/docs/servers/cloudflare/live-sockets).

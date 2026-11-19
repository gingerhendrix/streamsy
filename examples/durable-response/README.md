# Durable response

A Bun server that runs each Effect AI model call once and records its output in a SQLite-backed Streamsy stream. A reader can disconnect, reload or join late and resume from its last offset without a second model call. Any reader can stop the response, and every reader then sees the same stopped finish record. The [Effect Atom](../durable-response-atom) and [Foldkit](../durable-response-foldkit) pages are browser clients for this server. See [streamsy.dev](https://streamsy.dev) for the Streamsy documentation.

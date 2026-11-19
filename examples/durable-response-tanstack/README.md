# Durable response with TanStack AI

A chat page built with TanStack AI `useChat` and the published `@durable-streams/tanstack-ai-transport`, served by a Bun API that records each response in a SQLite-backed Streamsy stream. A reload or a second tab replays the recorded answer without calling the model again, and a stop from any reader writes a stopped event that every reader sees. The repository root applies a Bun patch to the transport so that a late reader in raw replay mode receives the whole closed stream. See [streamsy.dev](https://streamsy.dev) for the Streamsy documentation.

# Durable response with AI SDK

A chat page built with the AI SDK `useChat` hook and the published `@durable-streams/aisdk-transport`, served by a Bun API that records each response in a SQLite-backed Streamsy stream. A reload or a second tab replays the recorded answer without calling the model again, and a stop from any reader ends the response for all of them. See [streamsy.dev](https://streamsy.dev) for the Streamsy documentation.

# Durable response with Effect Atom

A React page for the [durable response](../durable-response) server that keeps its state and effects in Effect Atom. It reads a response from the server's stream, saves its offset so a reload continues where it stopped, and sends stop requests that every open tab sees. See [streamsy.dev](https://streamsy.dev) for the Streamsy documentation.

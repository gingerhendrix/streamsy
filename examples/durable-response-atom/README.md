# Durable response with Effect Atom

React renders the view; `effect/reactivity` owns application state and effects. `readAtom` depends on response id and read mode. Drop invalidates and interrupts the read; reconnect starts a new fiber from the last persisted checkpoint. Function atoms send create and stop requests through the shared Effect HTTP client. No React effect owns the transport.

Start the [Bun API](../durable-response/README.md), then run `bun run dev` here and open <http://localhost:1341>. `DURABLE_RESPONSE_API` overrides the default `http://localhost:1340` proxy target. `bun run build` and `bun run preview` serve the production page with the same proxy.

Submit a prompt, drop after a few entries, and reconnect. Reload mid-answer to restore text and offset from sessionStorage. Open the same `/r/<id>` in another tab and stop in either tab. The visible offsets, entry count, stopped identity/time, and process model-call count show what happened.

`bun run test` exercises read interruption, checkpoint persistence, reconnect, and cleanup with an injected reader. `bun run typecheck` checks the package. The fold, transport, and session implementation are shared with Foldkit in `durable-response-contract`; HTTP proofs live in the host package.

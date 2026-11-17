# Durable response with Foldkit

A Foldkit Model/Message/update application using the same [Bun API and contract](../durable-response/README.md) as the Atom page. The update function folds batches, Save Commands persist checkpoints, and subscriptions own the scoped reader. Dropping disables the subscription; reconnecting starts it from the checkpoint. `resumedAt` changes only on explicit reconnect, so receiving a batch cannot restart its own subscription.

Start the Bun API, then run `bun run dev` here and open <http://localhost:1342>. `DURABLE_RESPONSE_API` overrides the default `http://localhost:1340` proxy target. Foldkit DevTools are available in development. `bun run build` and `bun run preview` serve the production page with the same proxy.

Submit a prompt, drop/reconnect, reload mid-answer, and open the same `/r/<id>` in another tab. Stop in either tab; a tab opened after closure sees the same partial text, stopped identity/time, and final offset.

`bun run test` exercises batch folding, drop/reconnect subscription dependencies, checkpoint Commands, reload initialization, stop/closure, and malformed parts. `bun run typecheck` checks the package. Shared contract tests and the host's HTTP proofs cover the underlying transport and server.

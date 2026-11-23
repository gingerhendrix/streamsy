# Issue tracker with Foldkit and Views

This example runs the issue tracker UI in Foldkit and maintains its derived rows and counts with a browser-local `View.live` circuit. It shares the issue tracker's JSON write API and State schemas.

## Run it

From the repository root, install the workspaces and build the Streamsy packages.

```sh
bun install
bun run build
```

From this example directory, start each command in its own terminal.

```sh
bun run dev:api
bun run dev
```

Open http://localhost:1338. Vite proxies `/api` and `/streams` to the base server on port 1339. Set `PORT` when running the base server directly and `ISSUE_TRACKER_API` before starting Vite to use a different API port. Set `ISSUE_TRACKER_DB` to a SQLite file on the API process to keep workspaces across API restarts.

## How the page reads

The server exposes one State stream per workspace at `/streams/workspace/:workspaceId`, with project, issue, and comment collections. A Foldkit subscription opens a single circuit with `RemoteStreams` and `ViewBackend.layerMemoryHost`. It replays the stream on each page load and shows the tracker after `caughtUp`. Project and status choices select from the current outputs. A different workspace loads a fresh page and circuit.

The project totals and status chips use committed view outputs. The page shows pending rows from the Foldkit Model while a separate scoped read of the same State stream waits for their txids. The write queue lives in the page, and leaving the page closes both readers.

Run `bun run smoke:issue-tracker-foldkit-views` from the repository root to check replay, live writes, failure display, and cancellation against a base-server child process.

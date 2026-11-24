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

The server exposes one State stream per workspace at `/streams/workspace/:workspaceId`, with project, issue, and comment collections. A Foldkit subscription supplies `RemoteStreams` and opens one `View.live` reader. Its private memory host replays the stream on each page load. The page shows the tracker once a snapshot has `caughtUp` set. Project and status choices select from the current outputs. A different workspace opens a fresh reader.

Each snapshot carries the project totals, status counts, rows, and workspace offset together. Pending writes overlay those committed rows until the workspace offset reaches the write response's `awaitOffset`. If the snapshot arrives first, the response confirms the write against the offset already in the model. Leaving the page closes the reader.

Run `bun run smoke:issue-tracker-foldkit-views` from the repository root to check replay, live writes, failure display, and cancellation against a base-server child process.

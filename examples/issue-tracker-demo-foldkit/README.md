# Issue tracker demo, Foldkit

The [issue tracker demo](../issue-tracker-demo) with a [Foldkit](https://foldkit.dev)
frontend. Foldkit is a TypeScript framework on Effect with the Elm
architecture. The server, the wire contract
([`@streamsy/issue-tracker-demo-contract`](../issue-tracker-demo-contract)) and
the stylesheet do not change. Only the browser code is new.

The [guide](https://streamsy.dev/docs/demos/issue-tracker/foldkit) shows only
what differs from the base app.

## Run it

The page is a Vite app with Foldkit's Vite plugin. The base app's server
answers `/api` and `/streams`, and Vite sends those paths to it. Run each in
its own terminal, from the repository root:

```bash
bun install --frozen-lockfile
bun run build
bun run --cwd examples/issue-tracker-demo-foldkit dev:api
bun run --cwd examples/issue-tracker-demo-foldkit dev
```

Open <http://localhost:1338>. `dev:api` starts the base server on port 1339.
The base server's settings still apply, for example `ISSUE_TRACKER_DB`.

| Variable            | Default                 | Meaning                                                           |
| ------------------- | ----------------------- | ----------------------------------------------------------------- |
| `ISSUE_TRACKER_API` | `http://localhost:1339` | The server that Vite sends `/api` and `/streams` to               |
| `ISSUE_TRACKER_DB`  | unset                   | For `dev:api`: SQLite file for the workspaces; unset keeps memory |

An edit reloads the page and keeps the Model, so a half-typed field or a
selected filter stays. The Foldkit DevTools badge shows in the corner. The
`.mcp.json` file starts the Foldkit DevTools MCP server, so an agent can read
the Model and the Message history of the running page.

`bun run build` in this folder writes the page to `dist/`. `bun run preview`
serves `dist/` on port 1338 with the same proxy.

## How it differs

| File                  | Role                                                                                 |
| --------------------- | ------------------------------------------------------------------------------------ |
| `src/model.ts`        | The Model: stream rows, pending writes, form fields, notices                         |
| `src/message.ts`      | Every Message, including `ReceivedStateBatch` for each stream read                   |
| `src/update.ts`       | `init` and `update`: fold the stream, add and confirm pending writes, roll them back |
| `src/subscription.ts` | One subscription that reads the State stream with `@durable-streams/client`          |
| `src/command.ts`      | Stamps, API writes, notice timers, the display name, copy link, new workspace        |
| `src/domain/`         | Pure code: apply changes, lay pending writes over rows, the derived lists and counts |
| `src/view/`           | The view, with the base app's markup and class names                                 |
| `src/resources.ts`    | The browser services that the runtime gives to every Command                         |
| `src/entry.ts`        | Reads the Flags and starts the Foldkit runtime                                       |
| `vite.config.ts`      | Foldkit's Vite plugin, and the proxy to the base app's server                        |

This variant has no server code. The base app's server is the server.

### Try it by hand

This is the base app's script. Every variant must pass it.

1. Open `main`. See two projects, five issues and one comment.
2. Set a display name. Add a comment. The comment shows the name.
3. Create a project. It becomes selected. Add an issue. It shows at once,
   faded, then solid.
4. Rename the issue. Change its status. Filter by status. The counts change.
5. Open the same URL in a second tab. Each change in one tab shows in the other.
6. In "Streamsy Demo", move a different issue to "In progress" in each tab at
   the same time. One change stays. The other rolls back with a message.
7. Create a shared workspace. Open its link in a new tab. It shows only its
   starter project.
8. With `ISSUE_TRACKER_DB` set, restart the server. All data is still there,
   and new changes still show.

## Verify

```bash
bun run --cwd examples/issue-tracker-demo-foldkit test:unit
bun run --cwd examples/issue-tracker-demo-foldkit typecheck
bun run --cwd examples/issue-tracker-demo-foldkit build
```

The tests use Foldkit's test tools in Vitest with `happy-dom`. Story tests
(`test/update.test.ts`) drive `update` with Messages and check the Model and
the Commands: the stream fold, a write that stays pending until its txid
arrives, a refused write that rolls back with a notice, the rename field, and
empty input. Scene tests (`test/scene.test.ts`) drive the page through the
view: they type and click, resolve the Commands, and check that a new issue
shows faded and then solid, that a refused change rolls back with the server's
message, and that the filter chips count each status. Other tests cover the
pure workspace functions, the batch-to-Message mapping, and the JSON codec
that keeps the Model across a reload.

The server is the base app's server, so the base app's `smoke:http` covers it.

## Limits

- `effect` is `4.0.5`. Foldkit `0.169.0` and its tools ask for exactly
  `4.0.0` as a peer. The root `overrides` entry keeps one copy of `effect`,
  and Bun warns about the `@effect/platform-browser` peer. The Vite plugin
  brings `@effect/platform-node-shared@4.0.0` for its Node side. Check all of
  these again when Foldkit or Effect moves.
- With two tabs open, a reload after an edit gives both tabs the Model of the
  tab that changed last.
- Form fields are plain elements, not `@foldkit/ui` components, so the page
  keeps the base app's markup and stylesheet. `@foldkit/ui` is installed only
  because the DevTools overlay needs it.
- The Model keeps every row of the workspace, and the view derives the lists
  on each render. This suits a demo workspace.
- The other limits of the base app still hold.

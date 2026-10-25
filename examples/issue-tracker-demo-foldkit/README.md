# Issue tracker demo, Foldkit

The [issue tracker demo](../issue-tracker-demo) with a [Foldkit](https://foldkit.dev)
frontend. Foldkit is a TypeScript framework on Effect with the Elm
architecture. The server, the wire contract
([`@streamsy/issue-tracker-demo-contract`](../issue-tracker-demo-contract)) and
the stylesheet do not change. Only the browser code is new.

The [guide](https://streamsy.dev/docs/demos/issue-tracker/foldkit) shows only
what differs from the base app.

## Run it

From the repository root:

```bash
bun install --frozen-lockfile
bun run build
bun run --cwd examples/issue-tracker-demo-foldkit dev
```

Open <http://localhost:1338>. The settings are the base app's settings.

| Variable           | Default       | Meaning                                            |
| ------------------ | ------------- | -------------------------------------------------- |
| `PORT`             | `1338`        | HTTP port                                          |
| `ISSUE_TRACKER_DB` | unset         | SQLite file for the workspaces; unset keeps memory |
| `NODE_ENV`         | `development` | `production` turns off console echoing             |

The dev server has no hot module reloading. Foldkit reads `import.meta.hot`
as Vite's HMR API, and Bun's `import.meta.hot` refuses those calls. Reload
the page after an edit.

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
| `server/index.ts`     | The base app's server layers, with this variant's page                               |

Nothing in `server/` except the entry is new. It imports the base app's API,
storage and seed layers.

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
bun run --cwd examples/issue-tracker-demo-foldkit smoke:http
```

The unit tests drive `update` with Foldkit's `story` helper: the stream fold,
a write that stays pending until its txid arrives, a refused write that rolls
back with a notice, the rename field, and empty input. Other tests cover the
pure workspace functions and the batch-to-Message mapping. The HTTP smoke is
the base app's smoke, run against this folder.

## Limits

- `effect` is `4.0.5`. Foldkit `0.169.0` asks for exactly `4.0.0` as a peer.
  The root `overrides` entry keeps one copy of `effect`, and Bun warns about
  the `@effect/platform-browser` peer. Check both again when Foldkit or Effect
  moves.
- No Foldkit Vite plugin. Its view identity and Model-preserving reload are
  off. The view keys its lists by row id, as Foldkit's builds without the
  plugin need.
- Form fields are plain elements, not `@foldkit/ui` components, so the page
  keeps the base app's markup and stylesheet.
- The Model keeps every row of the workspace, and the view derives the lists
  on each render. This suits a demo workspace.
- The other limits of the base app still hold.

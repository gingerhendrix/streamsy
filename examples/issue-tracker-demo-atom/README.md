# Issue tracker demo, Effect Atom

The [issue tracker demo](../issue-tracker-demo) with a React frontend on
[Effect Atom](https://effect.website) (`effect/reactivity` and
`@effect/atom-react`). The base app's TanStack DB collections and live queries
become atoms. The server, the wire contract
([`@streamsy/issue-tracker-demo-contract`](../issue-tracker-demo-contract)), the
stylesheet and every component except `IssueTrackerApp.tsx` do not change.

The [guide](https://streamsy.dev/docs/examples/issue-tracker/atom) shows only
what differs from the base app.

## Run it

The page is a Vite app. The base app's server answers `/api` and `/streams`, and
Vite sends those paths to it. Run each in its own terminal, from the repository
root:

```bash
bun install --frozen-lockfile
bun run build
bun run --cwd examples/issue-tracker-demo-atom dev:api
bun run --cwd examples/issue-tracker-demo-atom dev
```

Open <http://localhost:1338>. `dev:api` starts the base server on port 1339. The
base server's settings still apply, for example `ISSUE_TRACKER_DB`.

| Variable            | Default                 | Meaning                                                           |
| ------------------- | ----------------------- | ----------------------------------------------------------------- |
| `ISSUE_TRACKER_API` | `http://localhost:1339` | The server that Vite sends `/api` and `/streams` to               |
| `ISSUE_TRACKER_DB`  | unset                   | For `dev:api`: SQLite file for the workspaces; unset keeps memory |

`bun run build` in this folder writes the page to `dist/`. `bun run preview`
serves `dist/` on port 1338 with the same proxy.

## How it differs

| File                                 | Role                                                                               |
| ------------------------------------ | ---------------------------------------------------------------------------------- |
| `src/queries.ts`                     | The stream read as an atom, and every derived value as an atom                     |
| `src/writes.ts`                      | `writeAtom`: stamp a change, show it as pending, send it, roll back a refusal      |
| `src/state.ts`                       | The atoms that the page writes: workspace, selection, filter and local writes      |
| `src/runtime.ts`                     | The typed `IssueTrackerApi` client, the stream reader service and `atomRuntime`    |
| `src/stream.ts`                      | The stream reader over `stream()` from `@durable-streams/client`                   |
| `src/domain/`                        | Pure code: fold changes, lay pending writes over rows, derive the lists and counts |
| `src/components/IssueTrackerApp.tsx` | Reads the atoms and calls `writeAtom`                                              |
| `src/App.tsx`, `src/main.tsx`        | The loading state, and the `RegistryProvider`                                      |
| `src/utils/`                         | The display name and notices as atoms, and the typed "new workspace" call          |
| `src/connection.ts`                  | The base app's file, and a gateway status from the proxy now counts as offline     |
| `src/components/*.tsx`               | Copies of the base app's other components, byte for byte                           |
| `vite.config.ts`                     | The React plugin, and the proxy to the base app's server                           |

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
bun run --cwd examples/issue-tracker-demo-atom test:unit
bun run --cwd examples/issue-tracker-demo-atom typecheck
bun run --cwd examples/issue-tracker-demo-atom build
```

The tests run in Vitest with `happy-dom`. `test/domain/` covers the fold, the
overlay and the derived lists with literals. `test/queries.test.ts` reads the
derived atoms from a registry with a scripted stream.
`test/writes.test.ts` runs the optimistic lifecycle. The tests script the
`HttpClient` below the typed client, so a 422 goes through the contract's
decoding to `WipLimitReached`. They cover a write that stays pending after its
HTTP answer, a txid that arrives before the answer, a refusal, a dropped
connection, two concurrent writes, and rows from another tab during a write.

The server is the base app's server, so the base app's `smoke:http` covers it.

## Limits

- `Atom`, `@effect/atom-react` and the Effect HTTP API modules carry
  `@stability unstable` tags. A later Effect release can break this page.
- The page ships Effect's HTTP client and Schema to the browser. The production
  JavaScript is 705 kB, 217 kB with gzip. The base app built with the same Vite
  version is 914 kB, 270 kB with gzip, because TanStack DB goes away.
- A write that the server accepted stays faded until its txid arrives. The page
  has no timeout, so it stays faded for as long as the reader cannot reach the
  stream. The base app's `awaitTxId` gives up and rolls back.
- If a write's connection drops after the server stored the write, the page
  rolls it back with a notice. The row then comes back from the stream.
- After the server comes back, the "Offline" badge goes away when the first long
  poll answers: at the next change, or after about 30 seconds.
- `localWritesAtom` keeps confirmed writes until the page reloads.
  `pendingWritesAtom` filters them out on every change.
- The other limits of the base app still hold.

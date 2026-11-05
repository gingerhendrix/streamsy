import { Array, DateTime, Effect, Option } from "effect";
import { Atom, AtomRegistry } from "effect/reactivity";
import { Intent, toPendingWrite } from "./domain/intent.ts";
import { Write, type PendingWrite } from "./domain/workspace.ts";
import { atomRuntime, IssueTrackerClient } from "./runtime.ts";
import { localWritesAtom, selectedProjectIdAtom, workspaceIdAtom } from "./state.ts";
import { browserCrypto } from "./utils/crypto.ts";
import { writerAtom } from "./utils/display-name.ts";
import { showNotice } from "./utils/notices.ts";

/** Send one write. Bodies leave the times out: the server stamps them. */
const send = (workspaceId: string, { txid, write }: PendingWrite) =>
  IssueTrackerClient.use((client) => {
    const params = { ws: workspaceId };

    return Write.$match(write, {
      CreateProject: ({ project: { id, name, description } }) =>
        Effect.asVoid(client.projects.create({ params, payload: { id, name, description, txid } })),
      CreateIssue: ({ issue: { id, projectId, title, status, updatedBy } }) =>
        Effect.asVoid(
          client.issues.create({
            params,
            payload: {
              id,
              projectId,
              title,
              status,
              txid,
              ...(updatedBy === undefined ? undefined : { updatedBy }),
            },
          }),
        ),
      UpdateIssue: ({ issueId, patch, updatedBy }) =>
        Effect.asVoid(
          client.issues.update({
            params: { ...params, id: issueId },
            payload: Option.match(updatedBy, {
              onNone: () => ({ ...patch, txid }),
              onSome: (name) => ({ ...patch, txid, updatedBy: name }),
            }),
          }),
        ),
      CreateComment: ({ comment: { id, issueId, author, body } }) =>
        Effect.asVoid(
          client.comments.create({ params, payload: { id, issueId, author, body, txid } }),
        ),
    });
  });

/** Why the server refused a write, or nothing when it accepted the write. */
const refusal = (workspaceId: string, pending: PendingWrite) =>
  send(workspaceId, pending).pipe(
    Effect.as(Option.none<string>()),
    Effect.catchTags({
      // A business rule. A retry gives the same answer until the workspace changes.
      WipLimitReached: ({ error }) => Effect.succeedSome(error),
      Conflict: ({ error }) => Effect.succeedSome(error),
      NotFound: ({ error }) => Effect.succeedSome(error),
      BadRequest: ({ error }) => Effect.succeedSome(error),
      InternalServerError: ({ error }) => Effect.succeedSome(error),
    }),
    Effect.orElseSucceed(() => Option.some("The server did not answer")),
  );

/** The ids, txid and time for one write. A browser without Web Crypto cannot run the page. */
const makeStamp = Effect.gen(function* () {
  const entropy = yield* browserCrypto.uuidV4();
  const txid = yield* browserCrypto.uuidV4();
  const at = DateTime.formatIso(yield* DateTime.now);

  return { entropy, txid, at };
}).pipe(Effect.orDie);

/**
 * Run one change. The row shows at once, as a pending write. An accepted write
 * stays pending until its txid arrives on the stream, so this function does
 * nothing with a success. A refusal removes the write and shows a notice.
 *
 * `concurrent` matters: without it a second write interrupts the first and
 * aborts its request. The registry comes from the service because a later call
 * retires this run's `get`, and a `get.set` on a retired context does nothing.
 */
export const writeAtom = Atom.keepAlive(
  atomRuntime.fn<Intent>()(
    Effect.fnUntraced(function* (intent) {
      const registry = yield* AtomRegistry.AtomRegistry;
      const stamp = yield* makeStamp;
      const pending = toPendingWrite(intent, stamp, registry.get(writerAtom));

      registry.update(localWritesAtom, Array.append(pending));

      // A new project is selected at once. If the server refuses it, the
      // selection falls back to the first project.
      if (Write.$is("CreateProject")(pending.write)) {
        registry.set(selectedProjectIdAtom, Option.some(pending.write.project.id));
      }

      const refused = yield* refusal(registry.get(workspaceIdAtom), pending);

      if (Option.isNone(refused)) return;

      registry.update(
        localWritesAtom,
        Array.filter(({ txid }) => txid !== pending.txid),
      );

      yield* showNotice(refused.value);
    }),
    { concurrent: true },
  ),
);

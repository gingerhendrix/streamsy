import { Clipboard } from "@effect/platform-browser";
import { Crypto, DateTime, Duration, Effect, Option, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { KeyValueStore } from "effect/persistence";
import { Command, Dom, Navigation } from "foldkit";
import {
  WorkspaceCreated,
  type CommentInput,
  type IssueInput,
  type ProjectInput,
} from "@streamsy/issue-tracker-demo-contract/api-schema";
import { Intent } from "./domain/intent.ts";
import { PendingWrite, Write } from "./domain/workspace.ts";
import { Message } from "./message.ts";

// Every Command returns a Message and never fails. The services come from the
// application's `resources` Layer.

/** The always-present demo workspace, used when no `?w=` param is set. */
export const mainWorkspaceId = "main";

/** Where the display name is kept, shared with the base app. */
export const displayNameKey = "issue-tracker-demo:display-name";

/** How long a notice stays on the page. */
const noticeDuration = Duration.seconds(4);

const apiUrl = (workspaceId: string, path: string) =>
  `/api/w/${encodeURIComponent(workspaceId)}${path}`;

// WRITES

/**
 * Make the ids, txid and time for one write. A browser without Web Crypto
 * cannot run the page at all, so a crypto failure is a defect.
 */
export const GenerateStamp = Command.define("GenerateStamp", {
  args: { intent: Intent },
  messages: [Message.CompletedGenerateStamp],
  execute: ({ intent }) =>
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const entropy = yield* crypto.randomUUIDv4();
      const txid = yield* crypto.randomUUIDv4();
      const at = DateTime.formatIso(yield* DateTime.now);

      return Message.CompletedGenerateStamp({ intent, stamp: { entropy, txid, at } });
    }).pipe(Effect.orDie),
});

/** The API request for one pending write. Bodies leave the times out: the server stamps them. */
const writeRequest = (workspaceId: string, { txid, write }: PendingWrite) =>
  Write.match(write, {
    CreateProject: ({ project }) =>
      HttpClientRequest.post(apiUrl(workspaceId, "/projects")).pipe(
        HttpClientRequest.bodyJsonUnsafe({
          id: project.id,
          name: project.name,
          description: project.description,
          txid,
        } satisfies ProjectInput),
      ),
    CreateIssue: ({ issue }) =>
      HttpClientRequest.post(apiUrl(workspaceId, "/issues")).pipe(
        HttpClientRequest.bodyJsonUnsafe({
          id: issue.id,
          projectId: issue.projectId,
          title: issue.title,
          status: issue.status,
          updatedBy: issue.updatedBy,
          txid,
        } satisfies IssueInput),
      ),
    UpdateIssue: ({ issueId, patch, updatedBy }) =>
      HttpClientRequest.patch(apiUrl(workspaceId, `/issues/${encodeURIComponent(issueId)}`)).pipe(
        HttpClientRequest.bodyJsonUnsafe({ ...patch, updatedBy, txid } satisfies IssueInput),
      ),
    CreateComment: ({ comment }) =>
      HttpClientRequest.post(apiUrl(workspaceId, "/comments")).pipe(
        HttpClientRequest.bodyJsonUnsafe({
          id: comment.id,
          issueId: comment.issueId,
          author: comment.author,
          body: comment.body,
          txid,
        } satisfies CommentInput),
      ),
  });

const ErrorBody = Schema.Struct({ error: Schema.String });

/** The `error` field of a refusal, or the status when the body has none. */
const refusal = (response: HttpClientResponse.HttpClientResponse) =>
  response.json.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(ErrorBody)),
    Effect.map(({ error }) => error),
    Effect.orElseSucceed(() => `Request failed with status ${response.status}`),
  );

/**
 * Send one write. Success only means the server accepted it: the row stays
 * pending until its txid arrives on the stream.
 */
export const SendWrite = Command.define("SendWrite", {
  args: { workspaceId: Schema.String, pending: PendingWrite },
  messages: [Message.SucceededSendWrite, Message.FailedSendWrite],
  execute: ({ workspaceId, pending }) =>
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const response = yield* client.execute(writeRequest(workspaceId, pending));

      if (response.status < 300) return Message.SucceededSendWrite({ txid: pending.txid });

      return Message.FailedSendWrite({ txid: pending.txid, error: yield* refusal(response) });
    }).pipe(
      Effect.orElseSucceed(() =>
        Message.FailedSendWrite({ txid: pending.txid, error: "The server did not answer" }),
      ),
    ),
});

export const WaitBeforeNoticeDismissal = Command.define("WaitBeforeNoticeDismissal", {
  args: { noticeId: Schema.Int },
  messages: [Message.CompletedWaitBeforeNoticeDismissal],
  execute: ({ noticeId }) =>
    Effect.sleep(noticeDuration).pipe(
      Effect.as(Message.CompletedWaitBeforeNoticeDismissal({ noticeId })),
    ),
});

// TITLE FIELD

/** The DOM id of an issue's title field. */
export const titleDraftId = (issueId: string) => `title-draft-${issueId}`;

export const FocusTitleDraft = Command.define("FocusTitleDraft", {
  args: { issueId: Schema.String },
  messages: [Message.CompletedFocusTitleDraft],
  execute: ({ issueId }) =>
    Dom.focus(`#${titleDraftId(issueId)}`).pipe(
      Effect.ignore,
      Effect.as(Message.CompletedFocusTitleDraft()),
    ),
});

// DISPLAY NAME

/** The saved display name, for the Flags. A storage failure starts anonymous. */
export const readDisplayName = KeyValueStore.KeyValueStore.pipe(
  Effect.flatMap((store) => store.get(displayNameKey)),
  Effect.map((saved) => saved ?? ""),
  Effect.orElseSucceed(() => ""),
);

/** Keep the name in `localStorage`. A storage failure only loses the name for the next visit. */
export const SaveDisplayName = Command.define("SaveDisplayName", {
  args: { name: Schema.String },
  messages: [Message.CompletedSaveDisplayName],
  execute: ({ name }) =>
    KeyValueStore.KeyValueStore.pipe(
      Effect.flatMap((store) =>
        name.trim() === "" ? store.remove(displayNameKey) : store.set(displayNameKey, name.trim()),
      ),
      Effect.ignore,
      Effect.as(Message.CompletedSaveDisplayName()),
    ),
});

// SHARE CONTROLS

export const CopyLink = Command.define("CopyLink", {
  messages: [Message.SucceededCopyLink, Message.FailedCopyLink],
  execute: Effect.gen(function* () {
    const clipboard = yield* Clipboard.Clipboard;
    const href = yield* Effect.sync(() => window.location.href);

    yield* clipboard.writeString(href);

    return Message.SucceededCopyLink();
  }).pipe(
    Effect.catch((error) => Effect.succeed(Message.FailedCopyLink({ error: error.message }))),
  ),
});

export const WaitBeforeCopiedReset = Command.define("WaitBeforeCopiedReset", {
  messages: [Message.CompletedWaitBeforeCopiedReset],
  execute: Effect.sleep(Duration.millis(1500)).pipe(
    Effect.as(Message.CompletedWaitBeforeCopiedReset()),
  ),
});

/** Create a fresh shared workspace on the server. */
export const CreateWorkspace = Command.define("CreateWorkspace", {
  messages: [Message.SucceededCreateWorkspace, Message.FailedCreateWorkspace],
  execute: Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;

    const response = yield* client
      .execute(HttpClientRequest.post("/api/workspaces"))
      .pipe(Effect.flatMap(HttpClientResponse.filterStatusOk));

    const { id } = yield* HttpClientResponse.schemaBodyJson(WorkspaceCreated)(response);

    return Message.SucceededCreateWorkspace({ workspaceId: id });
  }).pipe(
    Effect.orElseSucceed(() =>
      Message.FailedCreateWorkspace({ error: "Unable to create a workspace" }),
    ),
  ),
});

/** Switching workspace is a full navigation: one workspace for each page load. */
export const NavigateToWorkspace = Command.define("NavigateToWorkspace", {
  args: { workspaceId: Schema.String },
  messages: [Message.CompletedNavigateToWorkspace],
  execute: ({ workspaceId }) =>
    Navigation.load(workspaceHref(workspaceId)).pipe(
      Effect.as(Message.CompletedNavigateToWorkspace()),
    ),
});

/** The page link for a workspace. `main` has no query. */
export const workspaceHref = (workspaceId: string) =>
  workspaceId === mainWorkspaceId ? "?" : `?w=${encodeURIComponent(workspaceId)}`;

/** The workspace that the link selects (`?w=<id>`), or `main`. */
export const workspaceFromSearch = (search: string) =>
  Option.fromNullishOr(new URLSearchParams(search).get("w")).pipe(
    Option.map((workspaceId) => workspaceId.trim()),
    Option.filter((workspaceId) => workspaceId !== ""),
    Option.getOrElse(() => mainWorkspaceId),
  );

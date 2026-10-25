import { Array } from "effect";
import type { Document, Html, HtmlBuilder } from "foldkit/html";
import { mainWorkspaceId } from "../command.ts";
import { workspaceView } from "../domain/workspace.ts";
import { Message } from "../message.ts";
import { Sync, type Model, type Notice } from "../model.ts";
import { issuesPanelView } from "./issues.ts";
import { projectsPanelView } from "./projects.ts";

// The markup and class names are the base app's React components, so the base
// app's stylesheet styles this page unchanged.

export const view = (model: Model, h: HtmlBuilder<Message>): Document => ({
  title: "Streamsy Issue Tracker Demo (Foldkit)",
  body: Sync.match(model.sync, {
    Loading: () => waitingView(model, "Loading durable stream database…", h),
    Failed: ({ error }) => waitingView(model, error, h),
    Live: () => trackerView(model, h),
  }),
});

const waitingView = (model: Model, text: string, h: HtmlBuilder<Message>): Html =>
  h.main([h.Class("shell")], [topbarView(model, [], h), h.div([h.Class("loading")], [text])]);

const trackerView = (model: Model, h: HtmlBuilder<Message>): Html => {
  const workspace = workspaceView(model.rows, model.pending);

  return h.main(
    [h.Class("shell")],
    [
      topbarView(
        model,
        [connectionBadgeView(model.isOnline, h), nameFieldView(model.displayName, h)],
        h,
      ),
      h.section(
        [h.Class("layout")],
        [projectsPanelView(model, workspace, h), issuesPanelView(model, workspace, h)],
      ),
      noticesView(model.notices, h),
    ],
  );
};

/** The page header. `tools` sit between the brand and the share controls. */
const topbarView = (model: Model, tools: ReadonlyArray<Html>, h: HtmlBuilder<Message>): Html =>
  h.header(
    [h.Class("topbar")],
    [
      h.div(
        [h.Class("brand")],
        [
          h.span([h.Class("brand-mark"), h.AriaHidden(true)], ["S"]),
          h.span([h.Class("brand-name")], ["Streamsy"]),
          h.span([h.Class("brand-divider")], ["/"]),
          h.span([h.Class("brand-app")], ["Issue Tracker"]),
        ],
      ),
      h.div([h.Class("topbar-tools")], [...tools, shareControlsView(model, h)]),
    ],
  );

/** Shows "Offline" while the stream reader cannot reach the server. */
const connectionBadgeView = (isOnline: boolean, h: HtmlBuilder<Message>): Html =>
  isOnline
    ? h.empty
    : h.span(
        [h.Class("offline-badge"), h.Title("Reconnecting to the workspace stream")],
        ["Offline"],
      );

/** The display name that this browser writes with. */
const nameFieldView = (displayName: string, h: HtmlBuilder<Message>): Html =>
  h.label(
    [h.Class("name-field")],
    [
      h.span([], ["Name"]),
      h.input([
        h.Value(displayName),
        h.OnInput((value) => Message.UpdatedDisplayName({ value })),
        h.Placeholder("anonymous"),
        h.AriaLabel("Your display name"),
        h.Maxlength(40),
      ]),
    ],
  );

/**
 * Create a fresh shared workspace and copy the current URL. Anyone who opens
 * the copied link syncs to the same workspace stream live.
 */
const shareControlsView = (model: Model, h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class("share-controls")],
    [
      ...(model.workspaceId === mainWorkspaceId
        ? []
        : [
            h.span(
              [h.Class("share-workspace"), h.Title("Shared workspace id")],
              [model.workspaceId],
            ),
            h.a([h.Class("share-back"), h.Href("?")], ["Demo workspace"]),
          ]),
      h.button(
        [h.Type("button"), h.Class("subtle"), h.OnClick(Message.ClickedCopyLink())],
        [model.isLinkCopied ? "Copied" : "Copy link"],
      ),
      h.button(
        [
          h.Type("button"),
          h.OnClick(Message.ClickedNewWorkspace()),
          h.Disabled(model.isCreatingWorkspace),
        ],
        [model.isCreatingWorkspace ? "Creating…" : "New shared workspace"],
      ),
    ],
  );

const noticesView = (notices: ReadonlyArray<Notice>, h: HtmlBuilder<Message>): Html =>
  h.div(
    [h.Class("notices"), h.Role("status"), h.AriaLive("polite")],
    Array.map(notices, (notice) =>
      h.keyed("p")(
        String(notice.id),
        [h.Class("notice")],
        [h.span([h.Class("notice-label")], ["Change rejected"]), notice.message],
      ),
    ),
  );

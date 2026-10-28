import { Effect } from "effect";
import { Runtime } from "foldkit";
// The base app's stylesheet. The view uses the base app's markup and class names.
import "issue-tracker-demo/src/styles/globals.css";
import { readDisplayName, workspaceFromSearch } from "./command.ts";
import { Message } from "./message.ts";
import { Flags, Model } from "./model.ts";
import { subscriptions } from "./subscription.ts";
import { resources } from "./resources.ts";
import { init, update } from "./update.ts";
import { view } from "./view/view.ts";

/** Read once per page load: switching workspace is a full navigation. */
const flags = Effect.gen(function* () {
  const workspaceId = yield* Effect.sync(() => workspaceFromSearch(window.location.search));
  const displayName = yield* readDisplayName;

  return { workspaceId, displayName };
});

const application = Runtime.makeApplication({
  Model,
  Flags,
  init,
  update,
  view,
  subscriptions,
  resources,
  container: document.getElementById("root"),
  devTools: { Message },
});

Runtime.run(application, { flags });

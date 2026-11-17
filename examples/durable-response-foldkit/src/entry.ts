import { Effect } from "effect";
import { Runtime } from "foldkit";
import { pageResponseId, readerIdentity, resumeState } from "@streamsy/durable-response-contract";
import { Flags, Model } from "./model.ts";
import { Message } from "./message.ts";
import { init, update } from "./update.ts";
import { view } from "./view.ts";
import { resources } from "./resources.ts";
import { subscriptions } from "./subscription.ts";
import "durable-response/src/styles.css";

const flags = Effect.sync(() => {
  const id = pageResponseId(window.location.pathname);

  const storage = {
    getItem: (key: string) => sessionStorage.getItem(key),
    setItem: (key: string, value: string) => sessionStorage.setItem(key, value),
  };

  return { id, reader: readerIdentity(storage), snapshot: resumeState(storage, id) };
});

Runtime.run(
  Runtime.makeApplication({
    Model,
    Flags,
    init,
    update,
    view,
    subscriptions,
    resources,
    container: document.getElementById("root"),
    devTools: { Message },
  }),
  { flags },
);

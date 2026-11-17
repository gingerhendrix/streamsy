import { RegistryProvider } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { createRoot } from "react-dom/client";
import { pageResponseId, readerIdentity, resumeState } from "@streamsy/durable-response-contract";
import { App } from "./App.tsx";
import { readerIdAtom, responseIdAtom, snapshotAtom, resumedAtAtom } from "./state.ts";
import "durable-response/src/styles.css";

const id = pageResponseId(window.location.pathname);

// The adapter is lazy: resume helpers can recover if sessionStorage throws.
const storage = {
  getItem: (key: string) => sessionStorage.getItem(key),
  setItem: (key: string, value: string) => sessionStorage.setItem(key, value),
};

const saved = resumeState(storage, id);

const root = document.getElementById("root");

if (!root) throw Error("Missing root element");

createRoot(root).render(
  <RegistryProvider
    initialValues={[
      Atom.initialValue(responseIdAtom, id),
      Atom.initialValue(readerIdAtom, readerIdentity(storage)),
      Atom.initialValue(snapshotAtom, saved),
      Atom.initialValue(resumedAtAtom, saved.offset),
    ]}
  >
    <App />
  </RegistryProvider>,
);

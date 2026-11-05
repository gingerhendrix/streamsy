import { RegistryProvider } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { createRoot } from "react-dom/client";
import { App } from "./App.tsx";
import { workspaceIdAtom } from "./state.ts";
import { currentWorkspaceId } from "./utils/workspace.ts";
// The base app's stylesheet. The components keep the base app's markup and class names.
import "issue-tracker-demo/src/styles/globals.css";

createRoot(document.getElementById("root")!).render(
  <RegistryProvider initialValues={[Atom.initialValue(workspaceIdAtom, currentWorkspaceId())]}>
    <App />
  </RegistryProvider>,
);

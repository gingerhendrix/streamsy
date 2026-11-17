import { createRoot } from "react-dom/client";
import { App } from "./App.tsx";
import "../../durable-response/src/styles.css";

const root = document.getElementById("root");

if (!root) throw Error("Missing root element");

createRoot(root).render(<App />);

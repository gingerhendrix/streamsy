import { sqlite } from "../support/backends.ts";
import { registerLifecycle } from "./lifecycle-suite.ts";

registerLifecycle(sqlite);

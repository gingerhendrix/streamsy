import { sqlite } from "../support/backends.ts";
import { registerViewConformance } from "./view-conformance-suite.ts";

registerViewConformance(sqlite);

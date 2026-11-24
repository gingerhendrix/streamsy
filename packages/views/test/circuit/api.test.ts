import { sqlite } from "../support/backends.ts";
import { registerApi } from "./api-suite.ts";

registerApi(sqlite);

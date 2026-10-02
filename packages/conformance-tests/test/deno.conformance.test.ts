/** The official server suite and live-read checks through the Deno listener. */
import { describeRuntimeHost } from "./support/runtime-host-suite.ts";

describeRuntimeHost("deno");

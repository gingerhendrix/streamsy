import type { NotSupportedResult } from "../protocol/outcomes.ts";
export const notSupported = (result: NotSupportedResult): Response =>
  new Response(result.message ?? `Feature not supported: ${result.feature}`, {
    status: 400,
    headers: { "stream-not-supported": result.feature },
  });

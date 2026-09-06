export const isDevelopment = process.env.NODE_ENV !== "production";
export const port = Number.parseInt(process.env.PORT ?? "1338", 10);
export const contentType = "application/json";

/** The always-present, seeded demo workspace (default when no `?w=` is set). */
export const mainWorkspaceId = "main";

/**
 * Cheap guard for `:ws` route params before any stream lookup. Accepts both
 * `main` and server-generated ids.
 */
const workspaceIdPattern = /^[a-z0-9][a-z0-9-]{0,63}$/;

export function isValidWorkspaceId(workspaceId: string): boolean {
  return workspaceIdPattern.test(workspaceId);
}

/** Stream id for a workspace, served at `/streams/workspace/<id>`. */
export function workspaceStreamId(workspaceId: string): string {
  return `workspace/${workspaceId}`;
}

/**
 * Server-generated shareable workspace id: 10 base36 chars
 * (`^[a-z0-9]{10}$`). Random ids make links unguessable-ish and remove any
 * need for duplicate checks — a property of the design, not a security
 * boundary.
 */
export function generateWorkspaceId(): string {
  const bytes = new Uint8Array(10);
  crypto.getRandomValues(bytes);
  let workspaceId = "";
  for (const byte of bytes) {
    workspaceId += (byte % 36).toString(36);
  }
  return workspaceId;
}

// Streamsy's in-memory storage long-polls for up to 30 seconds. Bun's default
// HTTP idle timeout is 10 seconds, which can terminate live reads before
// Streamsy returns its normal 204 timeout response.
export const serverIdleTimeoutSeconds = 60;

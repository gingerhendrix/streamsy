// Minimal shell for the backend slice. The next batch replaces this with the
// board, detail drawer, and projection inspector.
const workspaceId = new URLSearchParams(location.search).get("workspace") ?? "main";
const projectId = new URLSearchParams(location.search).get("project") ?? "launch";
const connection = document.getElementById("connection");
const board = document.getElementById("board");

try {
  const health = await fetch("/health").then((response) => response.json());
  connection.textContent = `${health.host} · ${health.schemaVersion}`;
  const response = await fetch(`/api/workspaces/${workspaceId}/projects/${projectId}/board`);
  board.textContent = response.ok
    ? JSON.stringify(await response.json(), null, 2)
    : `Board unavailable (${response.status}). Seed with POST /api/workspaces/${workspaceId}/seed.`;
} catch (error) {
  connection.textContent = "Offline";
  board.textContent = String(error);
}

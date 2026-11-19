export const roomIdFromHash = (hash: string): string | null =>
  /^#[A-Za-z0-9_-]{21}$/.test(hash) ? hash.slice(1) : null;

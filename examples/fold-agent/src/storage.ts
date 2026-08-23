/* oxlint-disable effecttsgo/node-builtin-import -- Substrate construction is synchronous host wiring before any Effect runtime exists; the SQLite adapter itself is Promise/sync-native. */
/**
 * Streamsy substrate construction for the example: a Bun SQLite database on
 * disk, or an in-memory database for tests. The store owns the storage adapter,
 * the protocol, and the client-seam handle over it, so a caller can close all of
 * them and reopen a genuinely fresh runtime over the same file — which is what
 * the restart proof needs.
 *
 * Everything above this file works through {@link StreamBinding} values, the
 * explicit composition of identity, client, and stream address that the
 * `@streamsy/experimental` Effect capabilities take as their method argument.
 */
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import {
  createMemoryStorageAdapter,
  createStreamProtocol,
  directProtocolClient,
  type StreamsyProtocolClient,
} from "@streamsy/core";
import { bindStream, type StreamBinding } from "@streamsy/experimental/binding";
import { streamIdentity } from "@streamsy/experimental/stream-identity";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";

export interface StreamsyStore {
  /** The transport-neutral client over the local protocol. */
  readonly client: StreamsyProtocolClient;
  /** Bind one durable stream address to this store's identity and client. */
  bind(streamId: string): StreamBinding;
  /** Cancel client sessions, then release the underlying storage. */
  close(): Promise<void>;
}

export interface StreamsyStoreOptions {
  /** SQLite file path, or `:memory:`. */
  readonly filename?: string;
  /** Long-poll budget for live reads. Kept short in tests. */
  readonly longPollTimeoutMs?: number;
}

export const DEFAULT_DATABASE_PATH = "examples/fold-agent/.data/agent.sqlite";

/** Resolve the database path from the environment, falling back to the example default. */
export const databasePathFromEnv = (env: Record<string, string | undefined>): string =>
  env["FOLD_AGENT_DB"] ?? DEFAULT_DATABASE_PATH;

const IDENTITY = streamIdentity("fold-agent-example");

const store = (client: StreamsyProtocolClient, release: () => void): StreamsyStore => ({
  client,
  bind: (streamId) => bindStream({ identity: IDENTITY, client, streamId }),
  close: () => client.close().then(release),
});

export const openStore = (options: StreamsyStoreOptions = {}): StreamsyStore => {
  const filename = options.filename ?? ":memory:";
  if (filename !== ":memory:") mkdirSync(dirname(filename), { recursive: true });

  const adapter = createSqliteStorageAdapter({ filename });
  const protocol = createStreamProtocol({
    storage: { adapter },
    ...(options.longPollTimeoutMs === undefined
      ? {}
      : { longPollTimeoutMs: options.longPollTimeoutMs }),
  });

  return store(directProtocolClient(protocol), () => adapter.close());
};

/** The stream that holds one Fold session's durable log. */
export const sessionStreamId = (sessionId: string): string => `fold/sessions/${sessionId}/events`;

/** An in-memory Streamsy substrate, for tests that only need the protocol semantics. */
export const openMemoryStore = (options: { longPollTimeoutMs?: number } = {}): StreamsyStore => {
  const protocol = createStreamProtocol({
    storage: { adapter: createMemoryStorageAdapter() },
    ...(options.longPollTimeoutMs === undefined
      ? {}
      : { longPollTimeoutMs: options.longPollTimeoutMs }),
  });

  return store(directProtocolClient(protocol), () => {});
};

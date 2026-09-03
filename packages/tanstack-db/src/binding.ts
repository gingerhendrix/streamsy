import { DurableStream } from "@durable-streams/client";
import type { CollectionDefinition } from "@durable-streams/state";
import {
  createStreamDB,
  type StreamDB,
  type StreamStateDefinition,
} from "@durable-streams/state/db";
import {
  decodeStateSinkPublicError,
  STATE_SINK_CONTRACT_HEADER,
  STATE_SINK_RESET_HEADER,
  STATE_SINK_RESET_VALUE,
  STATE_SINK_VERSION_HEADER,
  type StateSinkPublicError,
} from "@streamsy/sinks";
import type { ResumeStore, StateSinkResume } from "./resume-store.ts";

export type StateSinkStatus =
  | { readonly kind: "connecting" }
  | { readonly kind: "live"; readonly offset?: string }
  | { readonly kind: "resetting"; readonly reason: string }
  | { readonly kind: "failed"; readonly error: StateSinkPublicError | Error };

export interface StateSinkClientDescriptor<
  Row extends object,
  Params extends Readonly<Record<string, string>>,
  Definition extends Readonly<Record<string, CollectionDefinition<Row>>>,
> {
  readonly name: string;
  readonly route: { readonly build: (params: Params) => string };
  readonly protocolVersion: number;
  readonly contractFingerprint: string;
  readonly state: Definition;
  readonly collection: {
    readonly name: keyof Definition & string;
    readonly type: string;
    readonly primaryKey: keyof Row & string;
  };
}

export interface StateSinkTransport<Row extends object> {
  readonly stream: DurableStream;
  readonly attachRows: (rows: () => readonly Row[]) => void;
  readonly latestResume: () => StateSinkResume | undefined;
  readonly commitResume: (resume: StateSinkResume) => void;
}

export interface StateSinkConnection<Definition extends StreamStateDefinition> {
  readonly db: StreamDB<Definition>;
  readonly preload: () => Promise<void>;
  readonly dispose: () => void;
}

const NO_ROWS: readonly never[] = [];
const emptyRows = (): readonly never[] => NO_ROWS;

export function createStateSinkBinding<
  Row extends object,
  Params extends Readonly<Record<string, string>>,
  Definition extends Readonly<Record<string, CollectionDefinition<Row>>>,
>(descriptor: StateSinkClientDescriptor<Row, Params, Definition>) {
  const createTransport = (options: {
    readonly params: Params;
    readonly origin: string | URL;
    readonly resumeStore: ResumeStore;
    readonly onStatus: (status: StateSinkStatus) => void;
    readonly fetch?: typeof globalThis.fetch;
  }): StateSinkTransport<Row> => {
    let rows: () => readonly Row[] = emptyRows;
    let latest: StateSinkResume | undefined;
    const fetchImplementation = options.fetch ?? globalThis.fetch;

    const guardedFetch = async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ): Promise<Response> => {
      const request = new Request(input, init);
      const headers = new Headers(request.headers);
      headers.set(STATE_SINK_VERSION_HEADER, String(descriptor.protocolVersion));
      headers.set(STATE_SINK_CONTRACT_HEADER, descriptor.contractFingerprint);

      let response = await fetchImplementation(new Request(request, { headers }));
      if (response.status === 409) {
        const error = await decodeErrorResponse(response);
        const { _tag: tag } = error;
        if (tag === "ResumeRejected" || tag === "ProtocolVersionUnsupported") {
          if (tag === "ProtocolVersionUnsupported") {
            options.onStatus({ kind: "failed", error });
            throw new Error(`state-sink protocol ${error.received} is unsupported`);
          }
          options.onStatus({ kind: "resetting", reason: error.reason });
          await options.resumeStore.clear();
          latest = undefined;
          const resetHeaders = new Headers(headers);
          resetHeaders.set(STATE_SINK_RESET_HEADER, STATE_SINK_RESET_VALUE);
          const resetUrl = new URL(request.url);
          resetUrl.searchParams.set("offset", "-1");
          response = await fetchImplementation(
            new Request(resetUrl, { method: request.method, headers: resetHeaders }),
          );
        }
      }

      if (!response.ok) {
        const error = await decodeErrorResponse(response);
        options.onStatus({ kind: "failed", error });
        return response;
      }

      const offset = response.headers.get("stream-next-offset") ?? undefined;
      if (offset !== undefined) {
        latest = {
          offset,
          protocolVersion: descriptor.protocolVersion,
          contractFingerprint: descriptor.contractFingerprint,
        };
      }
      options.onStatus({ kind: "live", offset });
      return lowerResetResponse(response, rows(), descriptor.collection);
    };

    const compatibleFetch: typeof globalThis.fetch = Object.assign(guardedFetch, {
      preconnect: fetchImplementation.preconnect,
    });
    const stream = new DurableStream({
      url: new URL(descriptor.route.build(options.params), options.origin).toString(),
      contentType: "application/json",
      warnOnHttp: false,
      fetch: compatibleFetch,
    });

    return {
      stream,
      attachRows: (read) => {
        rows = read;
      },
      latestResume: () => latest,
      commitResume: (resume) => {
        latest = resume;
      },
    };
  };

  const connect = (options: {
    readonly transport: StateSinkTransport<Row>;
    readonly resumeStore: ResumeStore;
  }): StateSinkConnection<Definition> => {
    let disposed = false;
    const db = createStreamDB({
      stream: options.transport.stream,
      state: descriptor.state,
      onBatch: (batch) => {
        const resume: StateSinkResume = {
          offset: batch.offset,
          protocolVersion: descriptor.protocolVersion,
          contractFingerprint: descriptor.contractFingerprint,
        };
        options.transport.commitResume(resume);
        queueMicrotask(() => {
          if (!disposed) void options.resumeStore.save(resume);
        });
      },
    });
    const collection = db.collections[descriptor.collection.name];
    options.transport.attachRows(() => {
      return collection.toArray;
    });
    return {
      db,
      preload: () => db.preload(),
      dispose: () => {
        disposed = true;
        db.close();
      },
    };
  };

  return Object.freeze({ descriptor, createTransport, connect });
}

async function decodeErrorResponse(response: Response): Promise<StateSinkPublicError> {
  try {
    return decodeStateSinkPublicError(await response.clone().json());
  } catch (cause) {
    return {
      _tag: "WireDecodeFailed",
      sink: "unknown",
      detail: cause instanceof Error ? cause.message : String(cause),
    };
  }
}

export async function lowerResetResponse<Row extends object>(
  response: Response,
  rows: readonly Row[],
  collection: { readonly type: string; readonly primaryKey: keyof Row & string },
): Promise<Response> {
  if (response.headers.get(STATE_SINK_RESET_HEADER) !== STATE_SINK_RESET_VALUE) return response;
  const value: unknown = await response.clone().json();
  if (!Array.isArray(value)) throw new Error("reset snapshot is not a Durable State message array");
  const messages = value.filter((message) => !isReset(message));
  const deletes = rows.map((row) => ({
    type: collection.type,
    key: String(row[collection.primaryKey]),
    headers: { operation: "delete" },
  }));
  const snapshotStart = messages.findIndex(isSnapshotStart);
  messages.splice(snapshotStart < 0 ? 0 : snapshotStart + 1, 0, ...deletes);
  return new Response(JSON.stringify(messages), {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

/* oxlint-disable anti-slop/no-unknown-parameters -- These helpers parse the reset snapshot's external JSON message array. */
function isReset(value: unknown): boolean {
  return control(value) === "reset";
}

function isSnapshotStart(value: unknown): boolean {
  return control(value) === "snapshot-start";
}

function control(value: unknown): string | undefined {
  if (!(value instanceof Object) || !("headers" in value)) return undefined;
  const headers = value.headers;
  if (!(headers instanceof Object) || !("control" in headers)) return undefined;
  return String(headers.control);
}
/* oxlint-enable anti-slop/no-unknown-parameters */

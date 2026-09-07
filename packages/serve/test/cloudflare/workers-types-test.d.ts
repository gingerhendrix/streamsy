export interface AlarmInvocationInfo {
  readonly isRetry: boolean;
  readonly retryCount: number;
}

export interface DurableObjectStorage {
  readonly getAlarm: () => Promise<number | null>;
  readonly setAlarm: (scheduledTime: number) => Promise<void>;
  readonly deleteAlarm: () => Promise<void>;
}

export interface DurableObjectId {
  readonly name?: string;
  readonly toString: () => string;
  readonly equals: (other: DurableObjectId) => boolean;
}

export interface DurableObjectStub {
  readonly fetch: (request: Request) => Promise<Response> | Response;
}

export interface DurableObjectNamespace {
  readonly idFromName: (name: string) => DurableObjectId;
  readonly get: (id: DurableObjectId) => DurableObjectStub;
}

export interface ExportedHandler<Env = unknown> {
  readonly fetch?: (request: Request, env: Env, ctx: unknown) => Response | Promise<Response>;
}

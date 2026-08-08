/**
 * Runtime configuration as a service.
 *
 * Application code never reads `process.env` or an ambient `env` object. It
 * reads this service; a host decides which layer supplies it. `layerFromEnv`
 * uses Effect `Config`, so the Worker's `ConfigProvider` and the local host's
 * environment are the same code path, and a test overrides values with
 * `layer(...)` or a `ConfigProvider` rather than mutating globals.
 *
 * `ISSUE_TRACKER_HOST` and `ISSUE_TRACKER_DEPLOYMENT` are exactly the two env
 * bindings `alchemy.run.ts` declares on the Worker.
 */
import { Config, Context, Effect, Layer } from "effect";

/** Fixed identity of this application's durable value shapes. */
export const SCHEMA_VERSION = "issue-tracker-projections/1";

export type HostKind = "local" | "cloudflare";

export interface AppConfigShape {
  /** Which executable edge is running the application. */
  readonly host: HostKind;
  /** Deployment identity: the Alchemy stage on Cloudflare, `local` otherwise. */
  readonly deployment: string;
  readonly schemaVersion: string;
}

export class AppConfig extends Context.Service<AppConfig, AppConfigShape>()(
  "issue-tracker-projections/AppConfig",
) {}

const hostConfig = Config.literals(["local", "cloudflare"] as const, "ISSUE_TRACKER_HOST").pipe(
  Config.withDefault("local" as HostKind),
);

const deploymentConfig = Config.string("ISSUE_TRACKER_DEPLOYMENT").pipe(
  Config.withDefault("local"),
);

/**
 * Read the configuration from the active `ConfigProvider`.
 *
 * Both values have defaults, so absence is not a failure. A *malformed* value —
 * a host that is neither `local` nor `cloudflare` — is a deployment bug rather
 * than an operational outcome, so it becomes a defect instead of an error the
 * request path would have to model.
 */
export const layerFromEnv: Layer.Layer<AppConfig> = Layer.effect(
  AppConfig,
  Effect.gen(function* () {
    const host = yield* hostConfig;
    const deployment = yield* deploymentConfig;
    return AppConfig.of({ host, deployment, schemaVersion: SCHEMA_VERSION });
  }).pipe(Effect.orDie),
);

/** Supply a concrete configuration, for hosts and tests that already have one. */
export const layer = (config: Omit<AppConfigShape, "schemaVersion">): Layer.Layer<AppConfig> =>
  Layer.succeed(AppConfig, AppConfig.of({ ...config, schemaVersion: SCHEMA_VERSION }));

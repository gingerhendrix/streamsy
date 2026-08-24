/**
 * Runtime configuration as a service.
 *
 * Application code never reads `process.env`. It reads this service; a host
 * decides which layer supplies it. `layerFromEnv` goes through Effect `Config`
 * so a test overrides values with a `ConfigProvider` rather than mutating
 * globals.
 */
import { Config, Context, Effect, Layer } from "effect";
import { issues } from "../domain/declaration.ts";
import { planHash } from "../views/plan.ts";

/** Fixed identity of this application's durable value shapes. */
export const SCHEMA_VERSION = "issue-tracker/1";

/** Identity of the maintained relation, derived from the declaration itself. */
export const PLAN_HASH = planHash(issues.plan);

export interface AppConfigValues {
  readonly deployment: string;
  readonly schemaVersion: string;
  readonly planHash: string;
  /** How long a minted sink resume token stays valid. */
  readonly resumeTokenTtlSeconds: number;
  /** The signing secret for sink resume tokens. */
  readonly resumeTokenSecret: string;
}

export class AppConfig extends Context.Service<AppConfig, AppConfigValues>()(
  "issue-tracker/AppConfig",
) {}

const deploymentConfig = Config.string("ISSUE_TRACKER_DEPLOYMENT").pipe(
  Config.withDefault("local"),
);

const resumeTtlConfig = Config.int("ISSUE_TRACKER_RESUME_TTL_SECONDS").pipe(
  Config.withDefault(900),
);

const resumeSecretConfig = Config.string("ISSUE_TRACKER_RESUME_SECRET").pipe(
  Config.withDefault("issue-tracker-local-development-secret"),
);

/**
 * Read configuration from the active `ConfigProvider`.
 *
 * Every value has a default, so absence is not a failure. A malformed value is
 * a deployment bug rather than an operational outcome, so it becomes a defect
 * instead of an error the request path would have to model.
 */
export const layerFromEnv: Layer.Layer<AppConfig> = Layer.effect(
  AppConfig,
  Effect.gen(function* () {
    return AppConfig.of({
      deployment: yield* deploymentConfig,
      schemaVersion: SCHEMA_VERSION,
      planHash: PLAN_HASH,
      resumeTokenTtlSeconds: yield* resumeTtlConfig,
      resumeTokenSecret: yield* resumeSecretConfig,
    });
  }).pipe(Effect.orDie),
);

/** The values a host may choose; the rest is fixed by the declaration. */
export type AppConfigOverrides = Partial<Omit<AppConfigValues, "schemaVersion" | "planHash">>;

/** Supply a concrete configuration, for hosts and tests that already have one. */
export const layer = (values: AppConfigOverrides = {}): Layer.Layer<AppConfig> =>
  Layer.succeed(
    AppConfig,
    AppConfig.of({
      deployment: values.deployment ?? "local",
      schemaVersion: SCHEMA_VERSION,
      planHash: PLAN_HASH,
      resumeTokenTtlSeconds: values.resumeTokenTtlSeconds ?? 900,
      resumeTokenSecret: values.resumeTokenSecret ?? "issue-tracker-test-secret",
    }),
  );

/* oxlint-disable effecttsgo/async-function -- Vitest owns this file's control flow: every `test` and `afterEach` callback is a Promise the runner awaits, and the request helper is a Promise-native driver over the host's Web `fetch` handler. The application operations under test stay Effect descriptions; the tests run them with `ManagedRuntime.runPromise` on runtimes assembled from explicit layers. */
/**
 * The server is Effect-first, and these tests only pass if it really is.
 *
 * Each one exercises a property that a Promise-first implementation wrapped in
 * `Effect.succeed` could not satisfy:
 *
 *  - application operations are *descriptions* that declare their services, so
 *    a test can run them on a runtime built from arbitrary layers;
 *  - expected failures live in the typed error channel, not in a status field
 *    and not as thrown defects;
 *  - request bodies are decoded by Schema at the boundary, so a malformed body
 *    never reaches a workflow;
 *  - capabilities are injectable: swapping the `Wake` layer changes behaviour
 *    with no change to any application function;
 *  - configuration comes from `Config`, so a `ConfigProvider` decides it.
 */
import {
  createMemoryStorageAdapter,
  directProtocolClient,
  StreamProtocol,
  type StreamProtocolClient,
  type JsonValue,
} from "@streamsy/core";
import { ConfigProvider, Effect, Layer, ManagedRuntime, Ref } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import type { ApiError, BoardResponse } from "../shared/api.ts";
import {
  createIssue,
  createProject,
  health,
  listProjects,
  type ApplicationServices,
} from "../server/application.ts";
import * as AppConfigModule from "../server/config.ts";
import { AppConfig } from "../server/config.ts";
import { createLocalHost } from "../server/local.ts";
import { applicationLayer } from "../server/runtime.ts";
import { Wake, type WakeMessage } from "../server/wake.ts";

type Host = ReturnType<typeof createLocalHost>;

const hosts: Host[] = [];
const disposals: Array<() => Promise<void>> = [];

afterEach(async () => {
  await Promise.all(hosts.splice(0).map((host) => host.close()));
  await Promise.all(disposals.splice(0).map((dispose) => dispose()));
});

function newHost(): Host {
  const host = createLocalHost();
  hosts.push(host);
  return host;
}

/** A runtime assembled from explicit layers, exactly as a host assembles one. */
function testRuntime(overrides: {
  readonly config?: Layer.Layer<AppConfig>;
  readonly wake?: Layer.Layer<Wake>;
  readonly client?: StreamProtocolClient;
}) {
  const client =
    overrides.client ??
    directProtocolClient(
      new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } }),
    );
  const runtime: ManagedRuntime.ManagedRuntime<ApplicationServices, never> = ManagedRuntime.make(
    applicationLayer({
      client,
      config: overrides.config ?? AppConfigModule.layer({ host: "local", deployment: "test" }),
      wake: overrides.wake ?? Layer.succeed(Wake, Wake.of({ wake: () => Effect.void })),
    }),
  );
  disposals.push(async () => {
    await runtime.dispose();
    await client.close();
  });
  return runtime;
}

async function call(host: Host, method: string, path: string, body?: JsonValue): Promise<Response> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { "content-type": "application/json" };
  }
  return host.fetch(new Request(`http://localhost${path}`, init));
}

describe("application operations are Effect descriptions", () => {
  test("an operation runs on any runtime built from the application layer", async () => {
    const runtime = testRuntime({});
    const projects = await runtime.runPromise(listProjects("w1"));
    expect(projects).toEqual([]);

    await runtime.runPromise(
      createProject("w1", { projectId: "launch", projectKey: "SHIP", name: "Launch" }),
    );
    expect((await runtime.runPromise(listProjects("w1"))).map((p) => p.projectId)).toEqual([
      "launch",
    ]);
  });

  test("an expected failure is a typed error, not a defect and not a status field", async () => {
    const runtime = testRuntime({});
    const failure = await runtime.runPromise(
      Effect.flip(
        createIssue("w1", {
          commandId: "cmd-1",
          issueId: "issue-1",
          projectId: "absent",
          title: "Nope",
        }),
      ),
    );
    const { _tag: tag } = failure;
    expect(tag).toBe("UnknownProject");
    if (tag !== "UnknownProject") throw new Error("the failure must be UnknownProject");
    expect(failure.projectId).toBe("absent");
  });
});

describe("Schema decodes the HTTP boundary", () => {
  test("a body that is not the declared shape is a 400 before any workflow runs", async () => {
    const host = newHost();
    await call(host, "POST", "/api/workspaces/arch/projects", {
      projectId: "launch",
      projectKey: "SHIP",
      name: "Launch",
    });

    // An unknown status literal, an unknown team member, and a wrong-typed
    // field are all rejected by the request schema.
    const invalidBodies: readonly JsonValue[] = [
      { commandId: "c1", issueId: "issue-1", projectId: "launch", title: "x", status: "shipped" },
      { commandId: "c2", issueId: "issue-2", projectId: "launch", title: "x", creatorId: "nobody" },
      { commandId: "c3", issueId: "issue-3", projectId: "launch", title: 42 },
      { commandId: "c4", issueId: "issue-4", projectId: "launch" },
    ];
    for (const body of invalidBodies) {
      const response = await call(host, "POST", "/api/workspaces/arch/issues", body);
      expect([response.status, JSON.stringify(body)]).toEqual([400, JSON.stringify(body)]);
    }

    // A malformed command body is rejected for the same reason.
    const created = await call(host, "POST", "/api/workspaces/arch/issues", {
      commandId: "ok-1",
      issueId: "issue-ok",
      projectId: "launch",
      title: "Fine",
    });
    expect(created.status).toBe(201);
    const bad = await call(host, "POST", "/api/workspaces/arch/issues/issue-ok/commands", {
      commandId: "c5",
      type: "assign",
      assigneeId: "nobody",
    });
    expect(bad.status).toBe(400);

    // No rejected body created durable state.
    const board: BoardResponse = await (
      await call(host, "GET", "/api/workspaces/arch/projects/launch/board")
    ).json();
    expect(board.rows.map((row) => row.issueId)).toEqual(["issue-ok"]);
  });

  test("a body that is not JSON at all is a 400", async () => {
    const host = newHost();
    const response = await host.fetch(
      new Request("http://localhost/api/workspaces/arch/projects", {
        method: "POST",
        body: "{not json",
        headers: { "content-type": "application/json" },
      }),
    );
    expect(response.status).toBe(400);
    const body: ApiError = await response.json();
    expect(body.error).toBe("invalid-json");
  });
});

describe("capabilities are injectable layers", () => {
  test("a deferred command wakes through whichever Wake layer the host provides", async () => {
    const woken: WakeMessage[] = [];
    const recordingWake = Layer.effect(
      Wake,
      Effect.gen(function* () {
        const log = yield* Ref.make<readonly WakeMessage[]>([]);
        return Wake.of({
          wake: (message) =>
            Ref.update(log, (all) => [...all, message]).pipe(
              Effect.tap(() => Effect.sync(() => woken.push(message))),
            ),
        });
      }),
    );
    const runtime = testRuntime({ wake: recordingWake });

    await runtime.runPromise(
      createProject("wake", { projectId: "launch", projectKey: "SHIP", name: "Launch" }),
    );
    // A settled command proves its coverage, so it must not wake anything.
    await runtime.runPromise(
      createIssue("wake", {
        commandId: "cmd-1",
        issueId: "issue-1",
        projectId: "launch",
        title: "Settled",
      }),
    );
    expect(woken).toEqual([]);

    // A deferred command cannot be proven, so the host's lane is asked to
    // converge it. Nothing in `createIssue` knows which lane that is.
    await runtime.runPromise(
      createIssue(
        "wake",
        { commandId: "cmd-2", issueId: "issue-2", projectId: "launch", title: "Deferred" },
        { deferProjections: true },
      ),
    );
    expect(woken).toEqual([{ workspaceId: "wake", projectId: "launch", issueId: "issue-2" }]);
  });
});

describe("configuration comes from Config", () => {
  test("a ConfigProvider decides the reported host and deployment", async () => {
    const runtime = testRuntime({
      config: AppConfigModule.layerFromEnv.pipe(
        Layer.provide(
          ConfigProvider.layer(
            ConfigProvider.fromEnv({
              env: {
                ISSUE_TRACKER_HOST: "cloudflare",
                ISSUE_TRACKER_DEPLOYMENT: "stage-under-test",
              },
            }),
          ),
        ),
      ),
    });
    expect(await runtime.runPromise(health())).toEqual({
      status: "ok",
      host: "cloudflare",
      deployment: "stage-under-test",
      schemaVersion: AppConfigModule.SCHEMA_VERSION,
    });
  });

  test("an absent provider falls back to the local defaults", async () => {
    const runtime = testRuntime({
      config: AppConfigModule.layerFromEnv.pipe(
        Layer.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} }))),
      ),
    });
    const reported = await runtime.runPromise(health());
    expect(reported.host).toBe("local");
    expect(reported.deployment).toBe("local");
  });
});

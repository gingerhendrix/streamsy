import { Array, Effect } from "effect";

const modules = new Set<string>();

const result = await Bun.build({
  entrypoints: [new URL("../src/browser/default.ts", import.meta.url).pathname],
  target: "browser",
  format: "esm",
  minify: true,
  plugins: [
    {
      name: "browser-module-inventory",
      setup(build) {
        build.onLoad({ filter: /./ }, (args) => {
          modules.add(args.path);

          return undefined;
        });
      },
    },
  ],
});

if (!result.success) throw new AggregateError(result.logs, "Browser build failed");

const forbidden = Array.filter([...modules], (path) =>
  /(?:\/effect(?:\/|@)|\/@streamsy\/core(?:\/|@)|\/packages\/core\/)/.test(path),
);

if (forbidden.length)
  throw new Error(`Forbidden browser dependencies:\n${Array.join(forbidden, "\n")}`);

for (const name of ["client", "state"]) {
  if (!Array.some([...modules], (path) => path.includes(`/@durable-streams/${name}/`)))
    throw new Error(`Missing official ${name} package from proof`);
}

const bytes = Array.reduce(result.outputs, 0, (sum, output) => sum + output.size);

let gzip = 0;

for (const output of result.outputs)
  gzip += Bun.gzipSync(new Uint8Array(await output.arrayBuffer())).byteLength;

await Effect.runPromise(Effect.log({ modules: modules.size, bytes, gzipBytes: gzip, forbidden }));

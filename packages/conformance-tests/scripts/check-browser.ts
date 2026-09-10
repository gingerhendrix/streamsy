import { join } from "node:path";

const modules = new Set<string>();
const result = await Bun.build({
  entrypoints: [join(import.meta.dir, "../src/browser/default.ts")],
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
const forbidden = [...modules].filter((path) =>
  /(?:\/effect(?:\/|@)|\/@streamsy\/core(?:\/|@)|\/packages\/core\/)/.test(path),
);
if (forbidden.length) throw new Error(`Forbidden browser dependencies:\n${forbidden.join("\n")}`);
for (const name of ["client", "state"]) {
  if (![...modules].some((path) => path.includes(`/@durable-streams/${name}/`)))
    throw new Error(`Missing official ${name} package from proof`);
}
const bytes = result.outputs.reduce((sum, output) => sum + output.size, 0);
let gzip = 0;
for (const output of result.outputs)
  gzip += Bun.gzipSync(new Uint8Array(await output.arrayBuffer())).byteLength;
console.log(JSON.stringify({ modules: modules.size, bytes, gzipBytes: gzip, forbidden }, null, 2));

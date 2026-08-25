#!/usr/bin/env bun
/* oxlint-disable no-console -- This is a one-shot code-generation CLI. */
import { readFile, writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { generateStateSinkModule, type StateSinkCodegenInput } from "./codegen.ts";

const args = process.argv.slice(2);
const modulePath = required(args, "--module");
const exportName = required(args, "--export");
const outputPath = required(args, "--output");
const check = args.includes("--check");

const imported: unknown = await import(pathToFileURL(modulePath).href);
const sink = Object.getOwnPropertyDescriptor(Object(imported), exportName)?.value;
if (!isCheckedSink(sink)) throw new Error(`${exportName} is not a checked state sink`);

const generated = generateStateSinkModule({
  modulePath: relativeImport(outputPath, modulePath),
  exportName,
  sink,
});
if (check) {
  const current = await readFile(outputPath, "utf8").catch(() => "");
  if (current !== generated) {
    console.error(`${outputPath} is stale; run the state-sink generator`);
    process.exit(1);
  }
  console.log(`${outputPath} is current`);
} else {
  await writeFile(outputPath, generated);
  console.log(`generated ${outputPath}`);
}

function required(values: readonly string[], name: string): string {
  const index = values.indexOf(name);
  const value = index < 0 ? undefined : values[index + 1];
  if (value === undefined) throw new Error(`missing ${name}`);
  return value;
}

function isCheckedSink(value: unknown): value is StateSinkCodegenInput["sink"] {
  return (
    value instanceof Object &&
    "kind" in value &&
    value.kind === "checked-state-sink" &&
    "collection" in value &&
    value.collection instanceof Object &&
    "name" in value.collection &&
    "type" in value.collection &&
    "primaryKey" in value.collection
  );
}

function relativeImport(output: string, module: string): string {
  const outputParts = output.replaceAll("\\", "/").split("/");
  const moduleParts = module.replaceAll("\\", "/").split("/");
  outputParts.pop();
  while (outputParts[0] === moduleParts[0]) {
    outputParts.shift();
    moduleParts.shift();
  }
  const relative = `${"../".repeat(outputParts.length)}${moduleParts.join("/")}`;
  return relative.startsWith(".") ? relative : `./${relative}`;
}

import { HOSTED_STATUS } from "./contract.ts";

const help = process.argv.slice(2).includes("--help");
if (help) {
  console.log("streamsy hosted evidence preparation");
  console.log(HOSTED_STATUS);
  console.log(
    "This local range provides typechecked, fake-only workflow preparation; live adapters are deferred.",
  );
} else {
  console.error(HOSTED_STATUS);
  console.error("Live hosted adapters are not enabled in this local range");
  process.exitCode = 2;
}

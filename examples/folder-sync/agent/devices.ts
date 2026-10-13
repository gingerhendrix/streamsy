/* oxlint-disable effecttsgo/async-function -- just-bash in-memory device compatibility edge. */
import { InMemoryFs } from "just-bash";

/** MountableFs strips /dev. Keep the standard shell files at their mount-relative paths. */
export class ShellDevices extends InMemoryFs {
  constructor() {
    super({ "/zero": "", "/stdin": "", "/stdout": "", "/stderr": "" });
    super.writeFileSync("/null", "");
    this.mkdirSync("/fd");
  }

  override writeFileSync(...args: Parameters<InMemoryFs["writeFileSync"]>): void {
    if (this.resolvePath("/", args[0]) !== "/null") super.writeFileSync(...args);
  }

  override async appendFile(...args: Parameters<InMemoryFs["appendFile"]>): Promise<void> {
    if (this.resolvePath("/", args[0]) !== "/null") await super.appendFile(...args);
  }
}

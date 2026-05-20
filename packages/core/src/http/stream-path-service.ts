export class StreamPathService {
  constructor(private pathPrefix: string) {}

  requiredPathPattern(): string {
    return `${this.prefixWithSlash()}{path}`;
  }

  strip(pathname: string): string {
    const regex = new RegExp(`^${this.escapeRegex(this.prefixWithSlash())}`);
    return pathname.replace(regex, "");
  }

  canonicalizeForkSource(header: string): string {
    return this.strip(header);
  }

  private prefixWithSlash(): string {
    return this.pathPrefix.endsWith("/") ? this.pathPrefix : `${this.pathPrefix}/`;
  }

  private escapeRegex(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }
}

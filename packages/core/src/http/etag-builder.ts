export class EtagBuilder {
  forCatchUp(pathname: string, startOffset: string, nextOffset: string, closed: boolean): string {
    const closedSuffix = closed ? ":c" : "";
    return `"${btoa(pathname)}:${startOffset}:${nextOffset}${closedSuffix}"`;
  }
}

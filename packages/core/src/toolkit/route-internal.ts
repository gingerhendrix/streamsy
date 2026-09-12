/**
 * Compile output shared by `StreamRoute` and the binding table.
 *
 * The public route value carries an id grammar, a typed `parse`, and a ref
 * constructor. The binding table also needs the compiled grammar, so it can
 * reject two templates that can both match one id. This module owns that
 * compilation and the static overlap check.
 */

export interface LiteralSegment {
  readonly kind: "literal";
  readonly value: string;
}

export interface ParameterSegment {
  readonly kind: "parameter";
  readonly name: string;
}

export type Segment = LiteralSegment | ParameterSegment;

export interface TemplateGrammar {
  readonly segments: ReadonlyArray<Segment>;
  readonly names: ReadonlyArray<string>;
}

/** Ids are canonical relative paths, the same rule the fetch Layer enforces. */
export function isCanonicalIdSegment(value: string): boolean {
  if (value.length === 0 || value === "." || value === "..") return false;
  return !/[%?#/\\]/.test(value);
}

export function compileSegments(template: string): TemplateGrammar {
  if (template.length === 0)
    throw new RangeError("a stream route template must name at least one segment");
  if (template.startsWith("/"))
    throw new RangeError("a stream route template is relative and cannot start with a slash");
  if (/[?#*]/.test(template))
    throw new RangeError("a stream route template cannot contain a query, fragment, or wildcard");
  const names: Array<string> = [];
  const segments: Array<Segment> = [];
  for (const part of template.split("/")) {
    if (part.length === 0)
      throw new RangeError("a stream route template cannot contain an empty segment");
    if (!part.startsWith(":")) {
      if (!isCanonicalIdSegment(part))
        throw new RangeError(
          `a stream route literal segment is not a canonical id segment: ${part}`,
        );
      segments.push({ kind: "literal", value: part });
      continue;
    }
    const name = part.slice(1);
    if (name.length === 0) throw new RangeError("a stream route parameter needs a name");
    if (names.includes(name)) throw new RangeError(`duplicate stream route parameter: ${name}`);
    names.push(name);
    segments.push({ kind: "parameter", name });
  }
  return { segments: Object.freeze(segments), names: Object.freeze(names) };
}

/**
 * True when two template routes can both match one id.
 *
 * Lengths must agree, and every position must be either an equal literal pair
 * or a parameter on at least one side. A parameter is treated as matching any
 * segment, so a codec-constrained parameter can report an overlap it would
 * never actually match. A custom route carries no template and is never
 * checked; the binding table orders those and the first match wins.
 */
export function templatesOverlap(left: string, right: string): boolean {
  if (left.length === 0 || right.length === 0) return false;
  const a = compileSegments(left).segments;
  const b = compileSegments(right).segments;
  if (a.length !== b.length) return false;
  for (const [index, segment] of a.entries()) {
    const other = b[index];
    if (!other || segment.kind === "parameter" || other.kind === "parameter") continue;
    if (segment.value !== other.value) return false;
  }
  return true;
}

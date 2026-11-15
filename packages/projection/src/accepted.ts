import { Array, Record } from "effect";
import { compare, Offset, StreamId, Streamsy } from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import { Accumulated, Emitted, type Commit } from "./commit.ts";
import { supportsAccepted } from "./commit/range.ts";
import type { Result } from "./commit/internal.ts";
import type { InputMap } from "./batch.ts";
import type { Unit } from "./unit.ts";
import type { WakeOutput } from "./wake.ts";

/** Private output name, stable across producer retries. */
export const acceptedKey = "@streamsy/accepted";

export const validateAccepted = (
  accepted: WakeOutput | undefined,
  params: StreamRoute.ParamCodecs,
  inputTemplates: ReadonlyArray<string>,
  commit: Pick<Commit<never>, "name">,
  outputNames: ReadonlyArray<string> = [],
): void => {
  if (accepted === undefined) return;

  if (!supportsAccepted(commit))
    throw new RangeError("accepted requires Commit.transaction or Commit.producer");

  if (accepted.collections !== Streamsy.wakeCollections) {
    // State declarations with the same wake collection schema are valid too.
    if (
      accepted.collections.stream?.schema !== Streamsy.WakeRow ||
      accepted.collections.stream.key !== "stream"
    )
      throw new RangeError("accepted requires a wake stream declaration");
  }

  for (const name of Record.keys(accepted.paramCodecs)) {
    if (!Record.has(params, name))
      throw new RangeError(`accepted parameter ${name} must be a projection parameter`);
  }

  for (const template of inputTemplates) {
    if (template === accepted.template)
      throw new RangeError("An accepted stream may not name an input stream");
  }

  if (Array.contains(outputNames, acceptedKey))
    throw new RangeError(`Output name ${acceptedKey} is reserved for accepted`);
};

/** Only moved inputs contribute a row; terminal closes retain their final offset. */
export const acceptedOutput = <R>(
  address: Streamsy.Address<Streamsy.WakeEvent, R>,
  inputs: InputMap,
  unit: Unit,
  closed: Readonly<Record<string, boolean>> = {},
) => {
  const items: Streamsy.WakeEvent[] = [];

  for (const [name, range] of Record.toEntries(unit.ranges)) {
    const input = inputs[name];

    if (
      input === undefined ||
      range.gone ||
      (compare(range.nextOffset, range.from) <= 0 && (!range.closed || closed[name]))
    )
      continue;
    const stream = StreamId.make(input.id);
    const offset = Offset.make(range.nextOffset);
    const value = range.closed ? { stream, offset, closed: true as const } : { stream, offset };
    items.push({ type: "stream", key: stream, value, headers: { operation: "upsert" } });
  }

  return { address, items };
};

/** Adapt every handler kind to the existing output-writing Commit result. */
export const acceptedResult = (result: Result, output: ReturnType<typeof acceptedOutput>) => {
  const outputs =
    result !== undefined && "outputs" in result
      ? { ...result.outputs, [acceptedKey]: output }
      : { [acceptedKey]: output };

  if (result !== undefined && "encoded" in result)
    return Accumulated({ value: result.value, encoded: result.encoded, outputs });
  const emitted = Emitted({ outputs });

  return result !== undefined && "wakeAt" in result
    ? { ...emitted, wakeAt: result.wakeAt }
    : emitted;
};

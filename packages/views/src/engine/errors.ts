import type { Expression, RelationNode, RowKey } from "../ir/contracts.ts";

export type OperatorPhase =
  | "plan"
  | "predicate"
  | "project"
  | "key"
  | "join"
  | "aggregate"
  | "top"
  | "decode"
  | "restore";

/** A bounded, contextual fault from the pure graph runtime. */
export class OperatorFault extends TypeError {
  readonly planName: string;
  readonly nodeId: string;
  readonly operatorKind: RelationNode["kind"] | "graph";
  readonly phase: OperatorPhase;
  readonly rowKey?: RowKey;
  readonly expression?: Expression;
  override readonly cause?: unknown;

  constructor(input: {
    readonly planName: string;
    readonly nodeId: string;
    readonly operatorKind: RelationNode["kind"] | "graph";
    readonly phase: OperatorPhase;
    readonly detail: string;
    readonly rowKey?: RowKey;
    readonly expression?: Expression;
    readonly cause?: unknown;
  }) {
    const key = input.rowKey === undefined ? "" : ` for row ${bounded(input.rowKey)}`;
    super(
      `plan ${input.planName}, node ${input.nodeId} (${input.operatorKind}) ${input.phase}${key}: ${input.detail}`,
    );
    this.name = "OperatorFault";
    this.planName = input.planName;
    this.nodeId = input.nodeId;
    this.operatorKind = input.operatorKind;
    this.phase = input.phase;
    this.rowKey = input.rowKey;
    this.expression = input.expression;
    this.cause = input.cause;
  }
}

function bounded(value: RowKey): string {
  const text = JSON.stringify(value);
  return text.length <= 120 ? text : `${text.slice(0, 117)}...`;
}

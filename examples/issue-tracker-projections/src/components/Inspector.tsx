/**
 * Projection inspector.
 *
 * It labels the durable hops of the fixed path and reports the coverage the
 * server proved from lineage. A wake receipt or an elapsed delay can never make
 * this panel say `Proven`.
 */
import { useState } from "react";
import type { CoverageReport } from "../../shared/api.ts";
import { shortPosition, shortStream } from "../lib/format.ts";
import type { Mutation } from "../lib/pending.ts";

const COVERAGE_LABELS: Readonly<Record<CoverageReport["status"], string>> = {
  proven: "Proven",
  "not-yet": "Not yet",
  incomparable: "Incomparable",
};

const HOP_LABELS: Readonly<Record<string, string>> = {
  "issue-detail": "Issue detail projection",
  "project-board": "Project board fan-in",
};

export interface InspectorProps {
  readonly mutations: readonly Mutation[];
  readonly boardStream: string;
  readonly onClose: () => void;
  readonly onRepair: () => void;
}

export function Inspector(props: InspectorProps) {
  const [expanded, setExpanded] = useState<string | null>(props.mutations[0]?.commandId ?? null);

  return (
    <aside
      className="inspector"
      role="dialog"
      aria-modal="false"
      aria-label="Projection inspector"
      data-testid="inspector"
      onKeyDown={(event) => {
        if (event.key === "Escape") props.onClose();
      }}
    >
      <header className="inspector-head">
        <h2>Projections</h2>
        <button type="button" className="ghost" onClick={props.onRepair}>
          Repair project
        </button>
        <button
          type="button"
          className="ghost close"
          onClick={props.onClose}
          data-testid="close-inspector"
        >
          Close
        </button>
      </header>

      <p className="meta">
        Board State stream <code>{shortStream(props.boardStream)}</code>
      </p>

      {props.mutations.length === 0 ? (
        <p className="empty">
          No commands yet in this session. Create or edit an issue to see its durable path.
        </p>
      ) : (
        <ul className="mutation-list">
          {props.mutations.map((mutation) => (
            <li key={mutation.commandId} data-testid={`mutation-${mutation.commandId}`}>
              <button
                type="button"
                className="mutation-head"
                aria-expanded={expanded === mutation.commandId}
                onClick={() =>
                  setExpanded((current) =>
                    current === mutation.commandId ? null : mutation.commandId,
                  )
                }
              >
                <span className="mutation-label">{mutation.label}</span>
                <CoverageBadge mutation={mutation} />
              </button>
              {expanded === mutation.commandId && <HopList mutation={mutation} />}
            </li>
          ))}
        </ul>
      )}
    </aside>
  );
}

function CoverageBadge({ mutation }: { readonly mutation: Mutation }) {
  if (mutation.phase === "failed") return <span className="badge bad">Failed</span>;
  if (mutation.coverage === undefined) return <span className="badge">In flight</span>;
  const status = mutation.coverage.status;
  return (
    <span
      className={`badge ${status === "proven" ? "ok" : "warn"}`}
      data-testid={`coverage-${mutation.commandId}`}
    >
      {COVERAGE_LABELS[status]}
    </span>
  );
}

function HopList({ mutation }: { readonly mutation: Mutation }) {
  const coverage = mutation.coverage;
  if (coverage === undefined) {
    return (
      <p className="meta">
        {mutation.phase === "failed"
          ? (mutation.error ?? "The command did not reach a durable acknowledgement.")
          : "Waiting for the accepted acknowledgement…"}
      </p>
    );
  }
  return (
    <div className="hops">
      <Hop
        title="Accepted source append"
        stream={coverage.ack.stream}
        rows={[["position", coverage.ack.position]]}
      />
      {coverage.hops.map((hop) => (
        <Hop
          key={hop.label}
          title={HOP_LABELS[hop.label] ?? hop.label}
          stream={hop.source}
          blocked={coverage.blockedAt === hop.label}
          rows={[
            ["read through", hop.through],
            ["output at", hop.output],
          ]}
        />
      ))}
      {coverage.status !== "proven" && (
        <p className="meta">
          Blocked at <strong>{coverage.blockedAt ?? "an earlier hop"}</strong>. Repair or the wake
          consumer carries it forward.
        </p>
      )}
      <details>
        <summary>Raw coverage</summary>
        <pre>{JSON.stringify(coverage, null, 2)}</pre>
      </details>
    </div>
  );
}

function Hop(props: {
  readonly title: string;
  readonly stream: string;
  readonly blocked?: boolean;
  readonly rows: readonly (readonly [string, string | null])[];
}) {
  return (
    <div className={`hop${props.blocked === true ? " hop-blocked" : ""}`}>
      <h3>{props.title}</h3>
      <code title={props.stream}>{shortStream(props.stream)}</code>
      <dl>
        {props.rows.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd title={value ?? "not yet"}>{shortPosition(value)}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

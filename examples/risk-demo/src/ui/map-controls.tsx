interface MapControlsProps {
  onZoomIn: () => void;
  onZoomOut: () => void;
  onReset: () => void;
  onPanUp: () => void;
  onPanLeft: () => void;
  onPanRight: () => void;
  onPanDown: () => void;
}

/** A compact, spatial control cluster for the map's existing pan/zoom actions. */
export function MapControls(props: MapControlsProps) {
  return (
    <div className="map-controls" role="group" aria-label="Map view controls">
      <div className="pan-pad" role="group" aria-label="Pan the map">
        <button className="pan-up" type="button" onClick={props.onPanUp} aria-label="Pan up">
          ↑
        </button>
        <button className="pan-left" type="button" onClick={props.onPanLeft} aria-label="Pan left">
          ←
        </button>
        <button
          className="pan-right"
          type="button"
          onClick={props.onPanRight}
          aria-label="Pan right"
        >
          →
        </button>
        <button className="pan-down" type="button" onClick={props.onPanDown} aria-label="Pan down">
          ↓
        </button>
      </div>

      <div className="map-view-tools">
        <div className="zoom-label" aria-hidden="true">
          <svg viewBox="0 0 24 24" focusable="false">
            <circle cx="10.5" cy="10.5" r="6.5" />
            <path d="m15.5 15.5 5 5" />
          </svg>
          <span>Zoom</span>
        </div>
        <div className="zoom-controls" role="group" aria-label="Zoom the map">
          <button type="button" onClick={props.onZoomOut} aria-label="Zoom out">
            −
          </button>
          <button type="button" onClick={props.onZoomIn} aria-label="Zoom in">
            +
          </button>
        </div>
        <button
          className="map-reset"
          type="button"
          onClick={props.onReset}
          aria-label="Reset the map view"
        >
          Reset
        </button>
      </div>
    </div>
  );
}

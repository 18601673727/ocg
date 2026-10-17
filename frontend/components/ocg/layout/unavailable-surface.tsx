import { EmptyPanel, PageSurface } from "@/components/ocg/primitives";
import { VIEW_AVAILABILITY, VIEW_TITLE, type WorkspaceView } from "./view-domain";

/**
 * A 0.4.5 view the Control Plane cannot back yet. It states what is missing
 * and shows nothing else: no sample rows, no zero counts that could be read
 * as a measurement.
 */
export function UnavailableSurface({ view }: { view: WorkspaceView }) {
  const availability = VIEW_AVAILABILITY[view];
  return (
    <PageSurface className="mx-auto w-full max-w-3xl">
      <h1 className="mb-4 text-2xl font-semibold tracking-tight">{VIEW_TITLE[view]}</h1>
      <EmptyPanel
        title={`${VIEW_TITLE[view]} is not available in this build`}
        hint={
          availability.available
            ? undefined
            : `The 0.5 Control Plane has no contract for it yet: ${availability.missing}.`
        }
        className="[&_p:last-child]:max-w-[60ch]"
      />
    </PageSurface>
  );
}

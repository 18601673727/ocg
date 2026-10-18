"use client";

import { OperatorConsole } from "../session/operator-console";
import type { ScenarioId } from "../runtime/runtime-types";
import type { WorkspaceView } from "../layout/view-domain";
import type { ControlCenterView } from "../control-center/domain";

export function OcgEntryGate(_props: { scenario: ScenarioId; view?: WorkspaceView; controlCenterView?: ControlCenterView; initialProjectId?: string }) {
  void _props;
  return <OperatorConsole />;
}

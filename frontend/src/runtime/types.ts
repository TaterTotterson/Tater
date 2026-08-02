export type JsonRow = Record<string, any>;

export interface RuntimeStatusState {
  health: JsonRow | null;
  text: string;
  tone: "normal" | "degraded" | "offline";
}

export interface RuntimeStatusMountOptions {
  initialState?: Partial<RuntimeStatusState>;
  endpoints: {
    breakdown: string;
    unloadModel: string;
  };
  onBreakdownChange?: (payload: JsonRow) => void;
  onHealthRefresh?: () => void;
  onToast?: (message: string, tone?: string) => void;
}

export interface RuntimeStatusController {
  setHealth: (health: JsonRow, tone?: RuntimeStatusState["tone"]) => void;
  setStatus: (text: string, tone?: RuntimeStatusState["tone"]) => void;
  open: () => Promise<void>;
  unmount: () => void;
}

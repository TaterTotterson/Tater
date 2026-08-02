export type JsonRow = Record<string, any>;

export interface CoreTabSpec extends JsonRow {
  core_key: string;
  label?: string;
  running?: boolean;
  requires_running?: boolean;
}

export interface CoresPayload {
  runtime: JsonRow;
  shop: JsonRow;
  tabs: JsonRow;
}

export interface CoresMountOptions {
  initialPayload: CoresPayload;
  initialTab?: string;
  endpoints: {
    runtime: string;
    shop: string;
    tabs: string;
  };
  renderCorePanel?: (host: HTMLElement, payload: JsonRow, tab: CoreTabSpec) => void;
  clearCorePanel?: (host: HTMLElement) => void;
  onTabChange?: (tab: string) => void;
  onToast?: (message: string, tone?: string) => void;
  onHealthRefresh?: () => void;
}

export interface CoresController {
  update: (payload: CoresPayload) => void;
  refresh: () => Promise<void>;
  refreshTab: (key: string) => Promise<void>;
  unmount: () => void;
}

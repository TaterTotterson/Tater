export type JsonRow = Record<string, any>;

export interface DashboardPayload extends JsonRow {
  generated_at?: number;
  cards?: JsonRow[];
  updates?: JsonRow;
  sections?: JsonRow[];
  briefs?: JsonRow[];
  settings?: JsonRow;
  snapshot?: JsonRow;
  snapshot_refresh?: JsonRow;
  brief_refresh?: JsonRow;
}

export interface DashboardMountOptions {
  initialPayload: DashboardPayload;
  dashboardEndpoint: string;
  refreshBriefsEndpoint: string;
  settingsEndpoint: string;
  initialPreferences?: {
    showMetrics?: boolean;
    showMedia?: boolean;
  };
  onPreferencesChange?: (preferences: {
    showMetrics: boolean;
    showMedia: boolean;
  }) => void;
  onPayloadChange?: (payload: DashboardPayload) => void;
  onNavigate?: (target: string) => void;
  onToast?: (message: string, tone?: string) => void;
}

export interface DashboardController {
  update: (payload: DashboardPayload) => void;
  unmount: () => void;
}

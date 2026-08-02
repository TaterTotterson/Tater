export type JsonRow = Record<string, any>;

export interface SettingsSummary extends JsonRow {
  redisConnected?: boolean;
  adminGateCount?: number;
  integrationCount?: number;
}

export interface SettingsMountOptions {
  initialTab?: string;
  initialSummary: SettingsSummary;
  onTabChange?: (tab: string) => void;
}

export interface SettingsController {
  update: (summary: SettingsSummary) => void;
  select: (tab: string) => void;
  unmount: () => void;
}

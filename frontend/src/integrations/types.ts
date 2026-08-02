export type JsonRow = Record<string, any>;

export interface IntegrationSettingsPayload extends JsonRow {
  integrations?: JsonRow[];
  integration_shop?: JsonRow;
  integration_runtime?: JsonRow;
  integration_device_registry?: JsonRow;
}

export interface IntegrationsMountOptions {
  initialSettings: IntegrationSettingsPayload;
  initialTab?: string;
  endpoints: {
    settings: string;
    shop: string;
    integrationSettings: string;
    integrationActions: string;
    deviceRegistry: string;
    rooms: string;
    runtime: string;
    runtimeStates: string;
    runtimeEvents: string;
    systemTasks: string;
  };
  onTabChange?: (tab: string) => void;
  onToast?: (message: string, tone?: string) => void;
}

export interface IntegrationsController {
  update: (payload: IntegrationSettingsPayload) => void;
  unmount: () => void;
}

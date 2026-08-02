export type JsonRow = Record<string, any>;

export interface PortalPayload {
  runtime: JsonRow;
  shop: JsonRow;
}

export interface PortalsMountOptions {
  initialPayload: PortalPayload;
  initialTab?: string;
  endpoints: {
    runtime: string;
    shop: string;
  };
  onToast?: (message: string, tone?: string) => void;
  onHealthRefresh?: () => void;
}

export interface PortalsController {
  update: (payload: PortalPayload) => void;
  unmount: () => void;
}

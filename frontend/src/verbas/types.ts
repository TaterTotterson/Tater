export type JsonRow = Record<string, any>;

export interface VerbaPayload {
  runtime: JsonRow;
  shop: JsonRow;
}

export interface VerbasMountOptions {
  initialPayload: VerbaPayload;
  initialTab?: string;
  endpoints: {
    runtime: string;
    shop: string;
  };
  onToast?: (message: string, tone?: string) => void;
  onHealthRefresh?: () => void;
}

export interface VerbasController {
  update: (payload: VerbaPayload) => void;
  unmount: () => void;
}

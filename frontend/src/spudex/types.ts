import type { ChatProfile } from "../chat/types";

export type JsonRow = Record<string, any>;

export interface SpudexPayload extends JsonRow {
  settings?: JsonRow;
  sessions?: JsonRow[];
  model_processes?: JsonRow[];
  platform_options?: JsonRow[];
  active_count?: number;
  model_process_count?: number;
  agent_lab?: string;
  git?: JsonRow;
}

export interface SpudexMountOptions {
  initialPayload: SpudexPayload;
  initialTab?: string;
  initialSessionId?: string;
  initialManualSessionId?: string;
  profile?: ChatProfile;
  endpoints: {
    root: string;
    settings: string;
    run: string;
    chat: string;
    chatSession: string;
    sessions: string;
    chatFiles: string;
  };
  onTabChange?: (tab: string) => void;
  onSessionChange?: (sessionId: string) => void;
  onManualSessionChange?: (sessionId: string) => void;
  onToast?: (message: string, tone?: string) => void;
}

export interface SpudexController {
  update: (payload: SpudexPayload) => void;
  refresh: () => Promise<void>;
  unmount: () => void;
}

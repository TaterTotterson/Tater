export type ChatContent = string | Record<string, any> | null;

export interface ChatMessage {
  role?: string;
  username?: string;
  content?: ChatContent;
  id?: string;
}

export interface ChatProfile {
  username?: string;
  user_avatar?: string;
  tater_avatar?: string;
  tater_name?: string;
  tater_first_name?: string;
  tater_last_name?: string;
  tater_full_name?: string;
  attach_max_mb_each?: number;
  attach_max_mb_total?: number;
  show_speed_stats?: boolean;
}

export interface ChatStatsPayload {
  enabled?: boolean;
  stats?: Record<string, any> | null;
}

export interface ChatJobState {
  status?: string;
  current_tool?: string;
  task_name?: string;
  updated_at?: number;
}

export interface ChatMountOptions {
  initialProfile: ChatProfile;
  initialMessages: ChatMessage[];
  initialStats?: ChatStatsPayload;
  initialJobs?: Record<string, ChatJobState>;
  sessionId: string;
  isIngress?: boolean;
  endpoints: {
    history: string;
    profile: string;
    stats: string;
    jobs: string;
    files: string;
  };
  onSessionChange?: (sessionId: string) => void;
  onProfileChange?: (profile: ChatProfile) => void;
  onJobsChange?: (jobs: Record<string, ChatJobState>) => void;
  onToast?: (message: string, tone?: string) => void;
  onRequestError?: (message: string) => void;
  onHealthRefresh?: () => void;
}

export interface ChatController {
  update: (payload: { profile?: ChatProfile; messages?: ChatMessage[]; stats?: ChatStatsPayload }) => void;
  unmount: () => void;
}

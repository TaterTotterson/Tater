export type Primitive = string | number | boolean | null;

export interface SelectOption {
  value?: Primitive;
  id?: Primitive;
  key?: Primitive;
  label?: string;
  title?: string;
  name?: string;
  friendly_name?: string;
  description?: string;
  meta?: string;
  room?: string;
  area?: string;
  icon?: string;
}

export interface MusicField {
  key: string;
  label?: string;
  type?: string;
  value?: unknown;
  placeholder?: string;
  description?: string;
  required?: boolean;
  disabled?: boolean;
  read_only?: boolean;
  compact?: boolean;
  full_width?: boolean;
  presentation?: string;
  min?: number;
  max?: number;
  step?: number | string;
  suffix?: string;
  action?: string;
  options?: Array<SelectOption | Primitive>;
}

export interface MusicAction {
  action: string;
  label?: string;
  aria_label?: string;
  tooltip?: string;
  tone?: string;
  confirm?: string;
}

export interface MusicBadge {
  label?: string;
  tone?: string;
}

export interface MusicSummaryRow {
  label?: string;
  value?: Primitive;
}

export interface MusicTrack {
  id?: string;
  position?: number;
  title?: string;
  artist?: string;
  album?: string;
  duration?: string;
  active?: boolean;
  image_src?: string;
  image_alt?: string;
}

export interface MusicPlaybackState {
  status?: string;
  position_seconds?: number;
  duration_seconds?: number;
  position_updated_at?: number;
  seekable?: boolean;
  seek_action?: string;
  seek_relative_action?: string;
  seek_step_seconds?: number;
}

export interface MusicPlayerRow {
  target: string;
  label?: string;
  meta?: string;
  selected?: boolean;
  kind?: string;
  sync_quality?: "precise" | "automatic" | "bridge" | "best_effort" | string;
  volume_percent?: number;
  sync_offset_ms?: number;
  transport_mode?: "auto" | "native" | "airplay" | string;
  transport_options?: Array<{ value: string; label: string }>;
  airplay_bridge_target?: string;
}

export interface MusicRecommendationEntry {
  id?: string;
  type?: "album" | "song" | string;
  title?: string;
  artist?: string;
  album?: string;
  reason?: string;
  track_count?: number;
  image_src?: string;
  image_alt?: string;
}

export interface MusicItem {
  id: string;
  group?: string;
  card_variant?: string;
  assistant_name?: string;
  title?: string;
  subtitle?: string;
  detail?: string;
  hero_image_src?: string;
  hero_image_alt?: string;
  hero_badges?: MusicBadge[];
  summary_rows?: MusicSummaryRow[];
  fields?: MusicField[];
  popup_fields?: MusicField[];
  player_rows?: MusicPlayerRow[];
  test_sync_action?: string;
  settings_title?: string;
  settings_label?: string;
  settings_aria_label?: string;
  track_list?: MusicTrack[];
  track_list_label?: string;
  track_list_action?: string;
  track_list_shuffle?: boolean;
  track_list_shuffle_action?: string;
  save_action?: string;
  save_label?: string;
  run_action?: string;
  run_label?: string;
  actions?: MusicAction[];
  playback?: MusicPlaybackState;
  fields_dropdown?: boolean;
  recommendation_items?: MusicRecommendationEntry[];
  generated_at?: number;
  history_event_count?: number;
  recommendations_enabled?: boolean;
  refresh_available?: boolean;
  refresh_running?: boolean;
}

export interface ManagerGroup {
  key: string;
  label?: string;
  item_group?: string;
  page_size?: number;
  empty_message?: string;
}

export interface ManagerTab {
  key: string;
  label?: string;
  source?: string;
  item_group?: string;
  groups?: ManagerGroup[];
  empty_message?: string;
}

export interface MusicUi {
  kind?: string;
  title?: string;
  appearance?: string;
  default_tab?: string;
  manager_tabs?: ManagerTab[];
  item_forms?: MusicItem[];
  live_updates?: boolean;
}

export interface CoreTabPayload {
  summary?: string;
  stats?: Array<{ label?: string; value?: Primitive }>;
  empty_message?: string;
  error?: string;
  ui?: MusicUi;
  updated_at?: number;
}

export interface MusicCoreMountOptions {
  initialPayload: CoreTabPayload;
  coreKey: string;
  tabEndpoint: string;
  actionEndpoint: string;
  eventsEndpoint: string;
}

export interface MusicCoreController {
  update: (payload: CoreTabPayload) => void;
  unmount: () => void;
}

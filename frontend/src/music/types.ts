export type Primitive = string | number | boolean | null;

export interface SelectOption {
  value?: Primitive;
  id?: Primitive;
  key?: Primitive;
  label?: string;
  description?: string;
  meta?: string;
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
}

export interface MusicItem {
  id: string;
  group?: string;
  card_variant?: string;
  title?: string;
  subtitle?: string;
  detail?: string;
  hero_image_src?: string;
  hero_image_alt?: string;
  hero_badges?: MusicBadge[];
  summary_rows?: MusicSummaryRow[];
  fields?: MusicField[];
  popup_fields?: MusicField[];
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
  fields_dropdown?: boolean;
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

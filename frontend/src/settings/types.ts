export type JsonRow = Record<string, any>;

export interface SettingsSummary extends JsonRow {
  redisConnected?: boolean;
  adminGateCount?: number;
  integrationCount?: number;
}

export interface GeneralSettings extends JsonRow {
  username?: string;
  user_avatar?: string;
  tater_avatar?: string;
  show_speed_stats?: boolean;
  webui_theme?: string;
  tater_first_name?: string;
  tater_last_name?: string;
  tater_personality?: string;
  webui_password_set?: boolean;
}

export interface MiscSettings extends JsonRow {
  popup_effect_style?: string;
  emoji_enable_on_reaction_add?: boolean;
  emoji_enable_auto_reaction_on_reply?: boolean;
  emoji_reaction_chain_chance_percent?: number;
  emoji_reply_reaction_chance_percent?: number;
  emoji_reaction_chain_cooldown_seconds?: number;
  emoji_reply_reaction_cooldown_seconds?: number;
  emoji_min_message_length?: number;
}

export interface AdvancedSettings extends JsonRow {
  admin_plugin_options?: string[];
  admin_only_plugins?: string[];
  admin_only_plugins_defaults?: string[];
  tater_api_enabled?: boolean;
  tater_api_key_set?: boolean;
  tater_api_mode?: string;
  tater_api_hydra_tools_enabled?: boolean;
}

export interface HydraSettings extends JsonRow {
  max_display?: number;
  max_store?: number;
  max_llm?: number;
  hydra_max_ledger_items?: number;
  hydra_astraeus_plan_review_enabled?: boolean;
  hydra_auto_continue_incomplete_final_enabled?: boolean;
  defaults?: Partial<HydraSettings>;
}

export interface PeopleSettingsPayload extends JsonRow {
  settings?: JsonRow;
  summary_metrics?: JsonRow[];
  people?: JsonRow[];
  identities?: JsonRow[];
  faces?: JsonRow[];
  face_id?: JsonRow;
}

export interface SpudLinkSettings extends JsonRow {
  mode?: string;
  node_name?: string;
  home_url?: string;
  public_url?: string;
  pairing_enabled?: boolean;
  allow_spudlets?: boolean;
  allow_little_spuds?: boolean;
  little_spud_tools_enabled?: boolean;
  telemetry_enabled?: boolean;
  request_previews_enabled?: boolean;
  model_routing_enabled?: boolean;
  model_routing?: JsonRow;
  paired_hub?: JsonRow;
  linked_nodes?: JsonRow[];
}

export interface ModelsSettingsPayload extends JsonRow {
  local_llm_models?: JsonRow;
  speech_ui?: JsonRow;
  announcement_speech_ui?: JsonRow;
  voice_model_ui?: JsonRow;
  speech_model_warmup?: JsonRow;
  hf_llm_warmup?: JsonRow;
  face_id?: JsonRow;
  hydra_base_servers?: JsonRow[];
}

export interface RedisConnectionStatus extends JsonRow {
  mode?: string;
  internal?: boolean;
  configured?: boolean;
  connected?: boolean;
  host?: string;
  port?: number;
  db?: number;
  username?: string;
  use_tls?: boolean;
  verify_tls?: boolean;
  ca_cert_path?: string;
  password_set?: boolean;
  error?: string;
  fallback_reason?: string;
  source?: string;
  config_path?: string;
  data_path?: string;
  data_dir?: string;
  socket_path?: string;
  redis_pid?: number;
  redis_managed?: boolean;
  redis_server_source?: string;
}

export interface RedisEncryptionStatus extends JsonRow {
  encryption_available?: boolean;
  key_exists?: boolean;
  key_path?: string;
  key_fingerprint?: string;
  live_encryption_enabled?: boolean;
  live_encryption_state_path?: string;
  live_encryption_updated?: string;
  error?: string;
}

export interface SettingsMountOptions {
  initialTab?: string;
  initialSummary: SettingsSummary;
  initialGeneral: GeneralSettings;
  initialMisc: MiscSettings;
  initialAdvanced: AdvancedSettings;
  initialHydra: HydraSettings;
  initialPeople: PeopleSettingsPayload;
  initialSpudLink: SpudLinkSettings;
  initialModels: ModelsSettingsPayload;
  initialLogAutoScroll?: boolean;
  initialPeopleTab?: string;
  initialPeopleSort?: string;
  initialSpudLinkTab?: string;
  initialModelsTab?: string;
  initialVoiceTab?: string;
  initialRedisStatus?: RedisConnectionStatus;
  initialRedisEncryptionStatus?: RedisEncryptionStatus;
  endpoints: {
    general: string;
    misc: string;
    advanced: string;
    hydra: string;
    hydraMetrics: string;
    hydraData: string;
    hydraDataClear: string;
    systemTasks: string;
    coreTaskRun: string;
    logs: string;
    people: string;
    peopleAction: string;
    spudLink: string;
    spudLinkStatus: string;
    spudLinkPairingCode: string;
    spudLinkConnect: string;
    spudLinkRevoke: string;
    models: string;
    modelsVoiceRuntime: string;
    modelsVoiceAction: string;
    modelsLocalLlm: string;
    modelsLocalLlmDelete: string;
    modelsHuggingFace: string;
    modelsHuggingFaceDetail: string;
    modelsHuggingFaceDownload: string;
    modelsHfWarmup: string;
    modelsHfWarmupCancel: string;
    modelsRemoteLlm: string;
    modelsContextEstimate: string;
    modelsFaceStatus: string;
    modelsSpeechPreview: string;
    modelsWyomingVoices: string;
    modelsOpenAiVoices: string;
    modelsOpenAiModels: string;
    modelsChatterboxVoices: string;
    modelsSpeechWarmup: string;
    modelsCloneAudio: string;
    voiceRuntime: string;
    voiceAction: string;
    voicePresence: string;
    voicePresenceEvents: string;
    redisStatus: string;
    redisConfigure: string;
    redisMigrateInternal: string;
    redisEncryptionStatus: string;
    redisEncrypt: string;
    redisDecrypt: string;
    clearChat: string;
  };
  publicEndpoints: {
    chat: string;
    models: string;
    spudLinkLlm: string;
    spudLinkModels: string;
    spudLinkPair: string;
  };
  onTabChange?: (tab: string) => void;
  onGeneralChange?: (settings: GeneralSettings) => void;
  onThemePreview?: (theme: string) => void;
  onMiscChange?: (settings: MiscSettings) => void;
  onAdvancedChange?: (settings: AdvancedSettings) => void;
  onHydraChange?: (settings: HydraSettings) => void;
  onPeopleChange?: (payload: PeopleSettingsPayload) => void;
  onPeopleTabChange?: (tab: string) => void;
  onPeopleSortChange?: (sort: string) => void;
  onSpudLinkChange?: (settings: SpudLinkSettings) => void;
  onModelsChange?: (settings: ModelsSettingsPayload) => void;
  onSpudLinkTabChange?: (tab: string) => void;
  onModelsTabChange?: (tab: string) => void;
  onVoiceTabChange?: (tab: string) => void;
  onRedisStatusChange?: (status: RedisConnectionStatus) => void;
  onLogAutoScrollChange?: (enabled: boolean) => void;
  onToast?: (message: string, tone?: string) => void;
}

export interface SettingsController {
  update: (summary: SettingsSummary) => void;
  select: (tab: string) => void;
  unmount: () => void;
}

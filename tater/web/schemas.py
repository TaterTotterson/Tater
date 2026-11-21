"""Validated request models shared by Tater HTTP endpoints."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class PluginToggleRequest(BaseModel):
    enabled: bool


class SettingsUpdateRequest(BaseModel):
    values: Dict[str, Any] = Field(default_factory=dict)


class ChatAttachmentRequest(BaseModel):
    name: Optional[str] = None
    mimetype: Optional[str] = None
    data_url: Optional[str] = None
    data_b64: Optional[str] = None


class ChatRequest(BaseModel):
    message: Optional[str] = None
    username: Optional[str] = None
    session_id: Optional[str] = None
    attachments: List[ChatAttachmentRequest] = Field(default_factory=list)


class OpenAIChatCompletionRequest(BaseModel):
    model: Optional[str] = None
    messages: List[Dict[str, Any]] = Field(default_factory=list)
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    stop: Optional[Any] = None
    tools: List[Dict[str, Any]] = Field(default_factory=list)
    tool_choice: Optional[Any] = None
    parallel_tool_calls: Optional[bool] = None
    stream: Optional[bool] = None
    user: Optional[str] = None
    user_name: Optional[str] = None
    device_name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SpudLinkTaterChatRequest(BaseModel):
    message: Optional[Any] = None
    history: List[Dict[str, Any]] = Field(default_factory=list)
    attachments: List[Dict[str, Any]] = Field(default_factory=list)
    user: Optional[str] = None
    user_name: Optional[str] = None
    device_name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SpudLinkPairRequest(BaseModel):
    pairing_code: str = Field(min_length=1)
    role: Optional[str] = None
    node_name: Optional[str] = None
    public_url: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SpudLinkPairingCodeRequest(BaseModel):
    home_url: Optional[str] = None
    public_url: Optional[str] = None
    spud_link_home_url: Optional[str] = None
    spud_link_public_url: Optional[str] = None
    role: Optional[str] = None


class SpudLinkConnectRequest(BaseModel):
    hub_url: str = Field(min_length=1)
    pairing_code: str = Field(min_length=1)
    role: Optional[str] = None
    node_name: Optional[str] = None
    public_url: Optional[str] = None


class SpudLinkHeartbeatRequest(BaseModel):
    node_name: Optional[str] = None
    public_url: Optional[str] = None
    mode: Optional[str] = None
    version: Optional[str] = None
    stats: Dict[str, Any] = Field(default_factory=dict)
    activity: Dict[str, Any] = Field(default_factory=dict)


class SpudLinkPushRegistrationRequest(BaseModel):
    enabled: Optional[bool] = True
    provider: Optional[str] = None
    app: Optional[str] = None
    environment: Optional[str] = None
    push_device_id: Optional[str] = None
    push_secret: Optional[str] = None
    gateway_url: Optional[str] = None
    relay_url: Optional[str] = None
    registered_at: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SpudLinkNotificationReconcileRequest(BaseModel):
    event_ids: List[str] = Field(default_factory=list)
    notifications: List[Dict[str, Any]] = Field(default_factory=list)


class SpudLinkHomeActionRequest(BaseModel):
    room_id: str = Field(min_length=1, max_length=160)
    category_id: str = Field(min_length=1, max_length=80)
    action: str = Field(min_length=1, max_length=80)
    value: Optional[float] = None
    mode: Optional[str] = Field(default=None, max_length=40)
    temperature_unit: Optional[str] = Field(default=None, max_length=4)


class SpudLinkMusicActionRequest(BaseModel):
    action: str = Field(min_length=1, max_length=40)
    provider: Optional[str] = Field(default=None, max_length=40)
    query: Optional[str] = Field(default=None, max_length=300)
    track_id: Optional[str] = Field(default=None, max_length=300)
    track_ids: List[str] = Field(default_factory=list)
    recommendation_id: Optional[str] = Field(default=None, max_length=300)
    queue_index: Optional[int] = Field(default=None, ge=0)
    queue_session_id: Optional[str] = Field(default=None, max_length=200)
    target: Optional[str] = Field(default=None, max_length=500)
    targets: List[str] = Field(default_factory=list)
    shuffle: Optional[bool] = None
    volume_percent: Optional[int] = Field(default=None, ge=0, le=100)
    position_seconds: Optional[float] = Field(default=None, ge=0)


class SpudLinkPersonRequest(BaseModel):
    person_id: str = Field(min_length=1, max_length=160)


class SpudLinkTtsRequest(BaseModel):
    text: Optional[str] = None


class SpudLinkSttRequest(BaseModel):
    audio_base64: Optional[str] = None
    content_type: Optional[str] = None
    language: Optional[str] = None


class SpudLinkModelRequest(BaseModel):
    data_base64: Optional[str] = None
    filename: Optional[str] = None
    mimetype: Optional[str] = None
    operation: Optional[str] = Field(default=None, max_length=40)
    model_id: Optional[str] = Field(default=None, max_length=80)
    prompt: Optional[str] = None
    audio_format: Dict[str, Any] = Field(default_factory=dict)
    speech_s: Optional[float] = None
    event_id: Optional[str] = None
    seen_at: Optional[str] = None
    source: Dict[str, Any] = Field(default_factory=dict)
    record: Optional[bool] = None


class SpudLinkRevokeNodeRequest(BaseModel):
    node_id: Optional[str] = None


class ShopItemRequest(BaseModel):
    id: str = Field(min_length=1)


class ShopRemoveRequest(BaseModel):
    id: str = Field(min_length=1)
    purge_redis: bool = False


class ShopReposRequest(BaseModel):
    repos: List[Dict[str, Any]] = Field(default_factory=list)


class CoreTabActionRequest(BaseModel):
    action: str = Field(min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict)


class DashboardBriefRefreshRequest(BaseModel):
    brief_id: Optional[str] = None


class DashboardSettingsRequest(BaseModel):
    personal_person_id: Optional[str] = None
    refresh_interval_seconds: Optional[int] = None
    brief_refresh_interval_seconds: Optional[int] = None


class SpudexSettingsRequest(BaseModel):
    values: Dict[str, Any] = Field(default_factory=dict)


class SpudexRunRequest(BaseModel):
    project_id: Optional[str] = None
    command: Optional[str] = None
    argv: List[str] = Field(default_factory=list)
    cwd: Optional[str] = None
    label: Optional[str] = None
    background: bool = False


class SpudexChatRequest(BaseModel):
    message: str = ""
    session_id: Optional[str] = None
    project_id: Optional[str] = None
    mode: Optional[str] = None
    resume: bool = False


class SpudexChatSessionRequest(BaseModel):
    label: Optional[str] = None
    project_id: Optional[str] = None
    mode: str = "build"


class SpudexProjectRequest(BaseModel):
    path: str = Field(min_length=1)
    name: str = ""


class SpudexTerminalRequest(BaseModel):
    project_id: str = ""
    cols: int = Field(default=100, ge=20, le=500)
    rows: int = Field(default=26, ge=5, le=200)


class SpudexTerminalInputRequest(BaseModel):
    data: str = Field(default="", max_length=16384)
    cols: int = Field(default=0, ge=0, le=500)
    rows: int = Field(default=0, ge=0, le=200)


class SpudexFileChangeRequest(BaseModel):
    change_id: str = Field(min_length=1)


class PeopleActionRequest(BaseModel):
    action: str = Field(min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict)


class HfModelDownloadItemRequest(BaseModel):
    provider: Optional[str] = None
    repo_id: Optional[str] = None
    model_id: Optional[str] = None
    filename: Optional[str] = None
    task: Optional[str] = None


class LocalLlmModelDeleteRequest(BaseModel):
    provider: Optional[str] = None
    model: Optional[str] = None


class LlamaCppChatTemplateRequest(BaseModel):
    provider: Optional[str] = None
    model: Optional[str] = None
    template: Optional[str] = None
    reset: Optional[bool] = None


class LlamaCppRemoteModelsRequest(BaseModel):
    host: Optional[str] = None
    port: Optional[str] = None
    api_key: Optional[str] = None


class DecisionRouterBenchmarkRequest(BaseModel):
    model: Optional[str] = None
    threshold: Optional[float] = None


class HfLlmWarmupCancelRequest(BaseModel):
    key: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    cancel_all: Optional[bool] = None


class LocalLlmUnloadRequest(BaseModel):
    provider: Optional[str] = None
    model: Optional[str] = None
    cache_key: Optional[str] = None
    unload_all: Optional[bool] = None


class AppSettingsRequest(BaseModel):
    username: Optional[str] = None
    user_avatar: Optional[str] = None
    tater_avatar: Optional[str] = None
    clear_user_avatar: Optional[bool] = None
    clear_tater_avatar: Optional[bool] = None
    max_display: Optional[int] = None
    show_speed_stats: Optional[bool] = None
    tater_first_name: Optional[str] = None
    tater_last_name: Optional[str] = None
    tater_personality: Optional[str] = None
    webui_theme: Optional[str] = None
    max_store: Optional[int] = None
    max_llm: Optional[int] = None
    homeassistant_base_url: Optional[str] = None
    homeassistant_token: Optional[str] = None
    hue_bridge_host: Optional[str] = None
    hue_app_key: Optional[str] = None
    hue_device_type: Optional[str] = None
    hue_timeout_seconds: Optional[int] = None
    aladdin_username: Optional[str] = None
    aladdin_password: Optional[str] = None
    aladdin_timeout_seconds: Optional[int] = None
    sonos_enabled: Optional[bool] = None
    sonos_discovery_timeout_seconds: Optional[int] = None
    sonos_speaker_hosts: Optional[str] = None
    unifi_network_base_url: Optional[str] = None
    unifi_network_api_key: Optional[str] = None
    unifi_protect_base_url: Optional[str] = None
    unifi_protect_api_key: Optional[str] = None
    vision_api_base: Optional[str] = None
    vision_mode: Optional[str] = None
    vision_provider: Optional[str] = None
    vision_model: Optional[str] = None
    vision_api_key: Optional[str] = None
    audio_understanding_mode: Optional[str] = None
    audio_understanding_provider: Optional[str] = None
    audio_understanding_api_base: Optional[str] = None
    audio_understanding_model: Optional[str] = None
    audio_understanding_api_key: Optional[str] = None
    audio_understanding_max_seconds: Optional[int] = None
    video_understanding_mode: Optional[str] = None
    video_understanding_provider: Optional[str] = None
    video_understanding_api_base: Optional[str] = None
    video_understanding_model: Optional[str] = None
    video_understanding_api_key: Optional[str] = None
    video_understanding_max_seconds: Optional[int] = None
    face_id_enabled: Optional[bool] = None
    speech_stt_backend: Optional[str] = None
    speech_qwen3_asr_model: Optional[str] = None
    speech_acceleration: Optional[str] = None
    speech_wyoming_stt_host: Optional[str] = None
    speech_wyoming_stt_port: Optional[str] = None
    speech_tts_backend: Optional[str] = None
    speech_tts_model: Optional[str] = None
    speech_tts_voice: Optional[str] = None
    speech_pocket_tts_output_gain: Optional[float] = None
    speech_tts_clone_text: Optional[str] = None
    speech_qwen_tts_language: Optional[str] = None
    speech_qwen_tts_instruct: Optional[str] = None
    speech_omnivoice_tts_language: Optional[str] = None
    speech_omnivoice_tts_instruct: Optional[str] = None
    speech_wyoming_tts_host: Optional[str] = None
    speech_wyoming_tts_port: Optional[str] = None
    speech_wyoming_tts_voice: Optional[str] = None
    speech_openai_tts_base_url: Optional[str] = None
    speech_openai_tts_api_key: Optional[str] = None
    speech_announcement_tts_backend: Optional[str] = None
    speech_announcement_tts_model: Optional[str] = None
    speech_announcement_tts_voice: Optional[str] = None
    speech_announcement_pocket_tts_output_gain: Optional[float] = None
    speech_announcement_tts_clone_text: Optional[str] = None
    speech_announcement_qwen_tts_language: Optional[str] = None
    speech_announcement_qwen_tts_instruct: Optional[str] = None
    speech_announcement_omnivoice_tts_language: Optional[str] = None
    speech_announcement_omnivoice_tts_instruct: Optional[str] = None
    speech_satellite_ducking_target_percent: Optional[int] = None
    speech_satellite_ducking_attack_ms: Optional[int] = None
    speech_satellite_ducking_release_ms: Optional[int] = None
    speech_announcement_wyoming_tts_host: Optional[str] = None
    speech_announcement_wyoming_tts_port: Optional[str] = None
    speech_announcement_wyoming_tts_voice: Optional[str] = None
    speech_announcement_openai_tts_base_url: Optional[str] = None
    speech_announcement_openai_tts_api_key: Optional[str] = None
    esphome_settings: Optional[Dict[str, Any]] = None
    emoji_enable_on_reaction_add: Optional[bool] = None
    emoji_enable_auto_reaction_on_reply: Optional[bool] = None
    emoji_reaction_chain_chance_percent: Optional[int] = None
    emoji_reply_reaction_chance_percent: Optional[int] = None
    emoji_reaction_chain_cooldown_seconds: Optional[int] = None
    emoji_reply_reaction_cooldown_seconds: Optional[int] = None
    emoji_min_message_length: Optional[int] = None
    hydra_llm_host: Optional[str] = None
    hydra_llm_port: Optional[str] = None
    hydra_llm_model: Optional[str] = None
    hydra_llm_api_key: Optional[str] = None
    hydra_llm_provider: Optional[str] = None
    hydra_llm_thinking_enabled: Optional[bool] = None
    hydra_llama_cpp_context_tokens: Optional[Any] = None
    hydra_llama_cpp_vision_context_tokens: Optional[Any] = None
    hydra_llama_cpp_mtp_enabled: Optional[bool] = None
    hydra_llama_cpp_speculative_method: Optional[str] = None
    hydra_llama_cpp_mtp_draft_tokens: Optional[Any] = None
    hydra_llama_cpp_mtp_draft_model: Optional[str] = None
    hydra_llama_cpp_n_batch: Optional[Any] = None
    hydra_llama_cpp_n_ubatch: Optional[Any] = None
    hydra_llama_cpp_flash_attn: Optional[bool] = None
    hydra_llama_cpp_offload_kqv: Optional[bool] = None
    hydra_llama_cpp_slot_count: Optional[Any] = None
    hydra_llama_cpp_base_slot: Optional[Any] = None
    hydra_llama_cpp_vision_slot: Optional[Any] = None
    spudex_llm_provider: Optional[str] = None
    spudex_llm_host: Optional[str] = None
    spudex_llm_model: Optional[str] = None
    tater_open_webui_llm_provider: Optional[str] = None
    tater_open_webui_llm_host: Optional[str] = None
    tater_open_webui_llm_port: Optional[str] = None
    tater_open_webui_llm_model: Optional[str] = None
    tater_open_webui_llm_api_key: Optional[str] = None
    tater_open_webui_llm_llama_cpp_slot: Optional[Any] = None
    hydra_base_servers: Optional[List[Dict[str, Any]]] = None
    hydra_local_model_load_targets: Optional[List[Dict[str, Any]]] = None
    hydra_beast_mode_enabled: Optional[bool] = None
    hydra_decision_router_mode: Optional[str] = None
    hydra_decision_router_model: Optional[str] = None
    hydra_decision_router_confidence: Optional[float] = None
    hydra_llm_astraeus_provider: Optional[str] = None
    hydra_llm_astraeus_host: Optional[str] = None
    hydra_llm_astraeus_port: Optional[str] = None
    hydra_llm_astraeus_model: Optional[str] = None
    hydra_llm_astraeus_api_key: Optional[str] = None
    hydra_llm_astraeus_llama_cpp_slot: Optional[Any] = None
    hydra_llm_thanatos_provider: Optional[str] = None
    hydra_llm_thanatos_host: Optional[str] = None
    hydra_llm_thanatos_port: Optional[str] = None
    hydra_llm_thanatos_model: Optional[str] = None
    hydra_llm_thanatos_api_key: Optional[str] = None
    hydra_llm_thanatos_llama_cpp_slot: Optional[Any] = None
    hydra_llm_hermes_provider: Optional[str] = None
    hydra_llm_hermes_host: Optional[str] = None
    hydra_llm_hermes_port: Optional[str] = None
    hydra_llm_hermes_model: Optional[str] = None
    hydra_llm_hermes_api_key: Optional[str] = None
    hydra_llm_hermes_llama_cpp_slot: Optional[Any] = None
    hydra_max_ledger_items: Optional[int] = None
    hydra_astraeus_plan_review_enabled: Optional[bool] = None
    hydra_auto_continue_incomplete_final_enabled: Optional[bool] = None
    popup_effect_style: Optional[str] = None
    admin_only_plugins: Optional[List[str]] = None
    tater_api_enabled: Optional[bool] = None
    tater_api_key: Optional[str] = None
    clear_tater_api_key: Optional[bool] = None
    tater_api_mode: Optional[str] = None
    tater_api_hydra_tools_enabled: Optional[bool] = None
    spud_link_mode: Optional[str] = None
    spud_link_node_name: Optional[str] = None
    spud_link_home_url: Optional[str] = None
    spud_link_public_url: Optional[str] = None
    spud_link_pairing_enabled: Optional[bool] = None
    spud_link_allow_spudlets: Optional[bool] = None
    spud_link_allow_tater_open_webui: Optional[bool] = None
    spud_link_allow_little_spuds: Optional[bool] = None
    spud_link_little_spud_tools_enabled: Optional[bool] = None
    spud_link_telemetry_enabled: Optional[bool] = None
    spud_link_request_previews_enabled: Optional[bool] = None
    spud_link_model_routing_enabled: Optional[bool] = None
    spud_link_model_route_llm: Optional[str] = None
    spud_link_model_route_vad: Optional[str] = None
    spud_link_model_route_stt: Optional[str] = None
    spud_link_model_route_tts: Optional[str] = None
    spud_link_model_route_vision: Optional[str] = None
    spud_link_model_route_audio: Optional[str] = None
    spud_link_model_route_video: Optional[str] = None
    spud_link_model_route_speaker_id: Optional[str] = None
    spud_link_model_route_emotion_id: Optional[str] = None
    spud_link_model_route_face_id: Optional[str] = None
    spud_link_hub_url: Optional[str] = None
    spud_link_node_token: Optional[str] = None
    clear_spud_link_node_token: Optional[bool] = None
    webui_password: Optional[str] = None
    webui_password_confirm: Optional[str] = None
    clear_webui_password: Optional[bool] = None

    @field_validator(
        "hydra_llm_port",
        "tater_open_webui_llm_port",
        "hydra_llm_astraeus_port",
        "hydra_llm_thanatos_port",
        "hydra_llm_hermes_port",
        mode="before",
    )
    @classmethod
    def _coerce_llm_port_to_text(cls, value: Any) -> Any:
        if value is None:
            return None
        return str(value).strip()


class GeneralSettingsRequest(BaseModel):
    username: Optional[str] = None
    user_avatar: Optional[str] = None
    tater_avatar: Optional[str] = None
    clear_user_avatar: Optional[bool] = None
    clear_tater_avatar: Optional[bool] = None
    show_speed_stats: Optional[bool] = None
    tater_first_name: Optional[str] = None
    tater_last_name: Optional[str] = None
    tater_personality: Optional[str] = None
    webui_theme: Optional[str] = None
    webui_password: Optional[str] = None
    webui_password_confirm: Optional[str] = None
    clear_webui_password: Optional[bool] = None


class MiscSettingsRequest(BaseModel):
    popup_effect_style: Optional[str] = None
    emoji_enable_on_reaction_add: Optional[bool] = None
    emoji_enable_auto_reaction_on_reply: Optional[bool] = None
    emoji_reaction_chain_chance_percent: Optional[int] = None
    emoji_reply_reaction_chance_percent: Optional[int] = None
    emoji_reaction_chain_cooldown_seconds: Optional[int] = None
    emoji_reply_reaction_cooldown_seconds: Optional[int] = None
    emoji_min_message_length: Optional[int] = None


class AdvancedSettingsRequest(BaseModel):
    admin_only_plugins: Optional[List[str]] = None
    tater_api_enabled: Optional[bool] = None
    tater_api_key: Optional[str] = None
    clear_tater_api_key: Optional[bool] = None
    tater_api_mode: Optional[str] = None
    tater_api_hydra_tools_enabled: Optional[bool] = None


class HydraSettingsRequest(BaseModel):
    max_display: Optional[int] = None
    max_store: Optional[int] = None
    max_llm: Optional[int] = None
    hydra_max_ledger_items: Optional[int] = None
    hydra_astraeus_plan_review_enabled: Optional[bool] = None
    hydra_auto_continue_incomplete_final_enabled: Optional[bool] = None


class SpudLinkSettingsRequest(BaseModel):
    spud_link_mode: Optional[str] = None
    spud_link_node_name: Optional[str] = None
    spud_link_home_url: Optional[str] = None
    spud_link_public_url: Optional[str] = None
    spud_link_pairing_enabled: Optional[bool] = None
    spud_link_allow_spudlets: Optional[bool] = None
    spud_link_allow_tater_open_webui: Optional[bool] = None
    spud_link_allow_little_spuds: Optional[bool] = None
    spud_link_little_spud_tools_enabled: Optional[bool] = None
    spud_link_telemetry_enabled: Optional[bool] = None
    spud_link_request_previews_enabled: Optional[bool] = None
    spud_link_model_routing_enabled: Optional[bool] = None
    spud_link_model_route_llm: Optional[str] = None
    spud_link_model_route_vad: Optional[str] = None
    spud_link_model_route_stt: Optional[str] = None
    spud_link_model_route_tts: Optional[str] = None
    spud_link_model_route_vision: Optional[str] = None
    spud_link_model_route_audio: Optional[str] = None
    spud_link_model_route_video: Optional[str] = None
    spud_link_model_route_speaker_id: Optional[str] = None
    spud_link_model_route_emotion_id: Optional[str] = None
    spud_link_model_route_face_id: Optional[str] = None
    spud_link_hub_url: Optional[str] = None
    spud_link_node_token: Optional[str] = None
    clear_spud_link_node_token: Optional[bool] = None


class HueLinkRequest(BaseModel):
    hue_bridge_host: Optional[str] = None
    hue_device_type: Optional[str] = None
    hue_timeout_seconds: Optional[int] = None


class AladdinTestRequest(BaseModel):
    aladdin_username: Optional[str] = None
    aladdin_password: Optional[str] = None
    aladdin_timeout_seconds: Optional[int] = None


class IntegrationSettingsRequest(BaseModel):
    settings: Dict[str, Any] = Field(default_factory=dict)


class IntegrationActionRequest(BaseModel):
    payload: Dict[str, Any] = Field(default_factory=dict)


class IntegrationRoomActionRequest(BaseModel):
    action: str = Field(min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict)


class SpeechTtsPreviewRequest(BaseModel):
    backend: Optional[str] = None
    model: Optional[str] = None
    voice: Optional[str] = None
    pocket_tts_output_gain: Optional[float] = None
    clone_text: Optional[str] = None
    managed_language: Optional[str] = None
    managed_instruct: Optional[str] = None
    acceleration: Optional[str] = None
    wyoming_host: Optional[str] = None
    wyoming_port: Optional[str] = None
    wyoming_voice: Optional[str] = None
    openai_base_url: Optional[str] = None
    openai_api_key: Optional[str] = None
    text: Optional[str] = None


class WyomingTtsVoicesRequest(BaseModel):
    host: Optional[str] = None
    port: Optional[str] = None
    current_voice: Optional[str] = None


class OpenAiCompatibleTtsVoicesRequest(BaseModel):
    base_url: Optional[str] = None
    api_key: Optional[str] = None


class OpenAiCompatibleTtsModelsRequest(BaseModel):
    base_url: Optional[str] = None
    api_key: Optional[str] = None


class RedisSetupRequest(BaseModel):
    mode: Optional[str] = "internal"
    host: Optional[str] = ""
    port: Optional[int] = 6379
    db: Optional[int] = 0
    username: Optional[str] = ""
    password: Optional[str] = ""
    use_tls: Optional[bool] = False
    verify_tls: Optional[bool] = True
    ca_cert_path: Optional[str] = ""
    data_path: Optional[str] = ""
    keep_existing_password: Optional[bool] = False
    test_only: Optional[bool] = False


class RedisMigrateInternalRequest(BaseModel):
    data_path: Optional[str] = ""
    flush_internal: Optional[bool] = True


class HydraDataClearRequest(BaseModel):
    platform: str = "all"
    mode: str = "all"


class WebUiAuthSetupRequest(BaseModel):
    password: Optional[str] = ""
    confirm_password: Optional[str] = ""


class WebUiAuthLoginRequest(BaseModel):
    password: Optional[str] = ""


class HfModelDownloadRequest(HfModelDownloadItemRequest):
    items: List[HfModelDownloadItemRequest] = Field(default_factory=list)

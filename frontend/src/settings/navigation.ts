/** Presentation routes only. Panel IDs and child IDs retain the existing settings contract. */
export const settingsCategories = [
  { id: "general", label: "General", icon: "sliders", description: "Appearance, personality, avatars, and everyday behavior." },
  { id: "people", label: "People & recognition", icon: "users", description: "People, linked identities, and voice and face recognition." },
  { id: "intelligence", label: "Models & intelligence", icon: "brain", description: "Local models, chat routing, vision, media, and Hydra behavior." },
  { id: "voice", label: "Voice & devices", icon: "mic", description: "How Tater hears you, responds, and connects to your rooms." },
  { id: "connections", label: "Connections", icon: "plug", description: "Pair a Spud Hub, Spudlet, Little Spud, or coding workspace." },
  { id: "access", label: "Privacy & access", icon: "shield", description: "WebUI login, application access, and administrator permissions." },
  { id: "system", label: "System", icon: "server", description: "Storage, encryption, background tasks, and diagnostics." },
] as const;

export type SettingsCategory = typeof settingsCategories[number]["id"];
export type SettingsPanel = "general" | "people" | "models" | "hydra" | "esphome" | "redis" | "spudhub" | "misc" | "advanced" | "system" | "logs";
export interface SettingsPage {
  id: string;
  category: SettingsCategory;
  section: string;
  sectionLabel: string;
  panel: SettingsPanel;
  child?: string;
  label: string;
  description: string;
  keywords: string;
}

export const settingsPages: SettingsPage[] = [
  { id: "general", category: "general", section: "general", sectionLabel: "Appearance & identity", panel: "general", label: "Appearance & identity", description: "Color theme, names, personality, chat avatars, and speed statistics.", keywords: "theme color colour blueberry mint grape strawberry username tokens second stats tater first last name style avatar upload" },
  { id: "misc", category: "general", section: "misc", sectionLabel: "Motion & reactions", panel: "misc", label: "Motion & reactions", description: "Popup animations and Discord reaction behavior.", keywords: "misc compotato popup effects animation reduced motion emoji automatic reaction chain cooldown chance minimum message length" },
  { id: "people", category: "people", section: "people", sectionLabel: "People", panel: "people", label: "People & identities", description: "People, administrator status, instructions, linked identities, and the face gallery.", keywords: "directory face gallery enrollment camera photo capture merge person portal user identity admin instructions" },
  { id: "speakerid", category: "people", section: "speakerid", sectionLabel: "Voice profiles", panel: "models", child: "speakerid", label: "Voice profiles", description: "Speaker ID enrollment, recognition, and runtime settings.", keywords: "speaker id voice identity profile enroll threshold recognition model" },
  { id: "emotionid", category: "people", section: "emotionid", sectionLabel: "Emotion ID", panel: "models", child: "emotionid", label: "Emotion ID", description: "Voice-tone classification and prompt hints.", keywords: "emotion id voice tone model classifier confidence runtime" },
  { id: "faceid", category: "people", section: "faceid", sectionLabel: "Face model", panel: "models", child: "faceid", label: "Face recognition model", description: "Face ID model selection and processing status.", keywords: "face id facenet adaface model enabled recognition" },
  { id: "models", category: "intelligence", section: "models", sectionLabel: "Model library", panel: "models", child: "huggingface", label: "Model library", description: "Download and manage local llama.cpp GGUF models.", keywords: "huggingface hugging face gguf download remove delete weights quantization local installed library" },
  { id: "routing", category: "intelligence", section: "routing", sectionLabel: "Chat models", panel: "models", child: "routing", label: "Chat models & routing", description: "Base servers, WebUI, Terminal, Beast roles, and local runtime tuning.", keywords: "llm base provider server host port api key openai llama cpp context token batch ubatch flash attention gpu offload speculative mtp draft slot thinking decision router benchmark astraeus thanatos hermes beast fallback" },
  { id: "vision", category: "intelligence", section: "vision", sectionLabel: "Vision", panel: "models", child: "vision", label: "Vision", description: "Still-image understanding and multimodal runtime.", keywords: "vision image model mmproj multimodal provider runtime" },
  { id: "audio-understanding", category: "intelligence", section: "media", sectionLabel: "Audio & video", panel: "models", child: "audio-understanding", label: "Audio & video", description: "Media understanding for clips and recordings.", keywords: "audio video understanding recording clip model provider" },
  { id: "hydra", category: "intelligence", section: "hydra", sectionLabel: "Hydra", panel: "hydra", label: "Hydra behavior & diagnostics", description: "Conversation history, planning, recovery, metrics, and stored execution data.", keywords: "hydra history context maximum messages shown stored sent llm ledger failure handling plan review astraeus continue incomplete final answers metrics data counters trace clear portal" },
  { id: "listening", category: "voice", section: "voice", sectionLabel: "Voice", panel: "models", child: "listening", label: "Listening", description: "Speech recognition, processing hardware, VAD, and listening controls.", keywords: "listening speech recognition stt voxtral whisper vad voice activity detection silence microphone hardware cpu gpu language transcription" },
  { id: "speech", category: "voice", section: "voice", sectionLabel: "Voice", panel: "models", child: "speech", label: "Speaking", description: "Reply voices, announcements, voice cloning, and playback.", keywords: "speech speaking reply voice announcement tts pocket wyoming openai clone audio preview rate speed volume pronunciation" },
  { id: "wake", category: "voice", section: "voice", sectionLabel: "Voice", panel: "models", child: "wake", label: "Wake word", description: "Wake model, trainer, and speech verification for all satellites.", keywords: "wake word phrase hey tater okay verification verifier stt trainer train threshold confidence sensitivity" },
  { id: "esphome", category: "voice", section: "satellites", sectionLabel: "Satellites", panel: "esphome", child: "satellites", label: "Satellite devices", description: "Pair devices, set rooms and volume, and manage individual satellite settings.", keywords: "esphome satellite pair device room volume led screen display theme clock weather climate brightness s3box voicepe sat1 respeaker echo audio microphone" },
  { id: "firmware", category: "voice", section: "satellites", sectionLabel: "Satellites", panel: "esphome", child: "firmware", label: "Firmware", description: "Official firmware, OTA updates, USB installation, and recovery.", keywords: "firmware update install ota usb recovery version flash linux" },
  { id: "platform", category: "voice", section: "satellites", sectionLabel: "Satellites", panel: "esphome", child: "platform", label: "Shared device settings", description: "Shared satellite behavior and the native voice pipeline.", keywords: "platform settings shared satellite runtime pipeline output chain aec echo cancellation microphone gain noise suppression wake tuning" },
  { id: "stereo", category: "voice", section: "playback", sectionLabel: "Playback", panel: "esphome", child: "stereo", label: "Stereo pairs", description: "Synchronized left and right playback destinations.", keywords: "stereo pair left right speaker playback synchronization group" },
  { id: "airplay", category: "voice", section: "playback", sectionLabel: "Playback", panel: "esphome", child: "airplay", label: "AirPlay input", description: "Send audio from Apple devices to satellites and speaker groups.", keywords: "airplay apple audio input receiver speaker music group" },
  { id: "stats", category: "voice", section: "diagnostics", sectionLabel: "Diagnostics", panel: "esphome", child: "stats", label: "Voice quality", description: "Live voice quality, latency, fallbacks, and per-satellite outcomes.", keywords: "stats statistics diagnostics latency quality fallback outcomes performance" },
  { id: "bluetooth", category: "voice", section: "devices", sectionLabel: "Devices", panel: "esphome", child: "bluetooth", label: "Bluetooth", description: "Discover and pair Bluetooth devices through your satellites.", keywords: "bluetooth ble proxy pairing meshtastic devices" },
  { id: "spudhub", category: "connections", section: "spudhub", sectionLabel: "Spud Link", panel: "spudhub", label: "Spud Link", description: "Hub and companion pairing, linked nodes, model routes, and connection policy.", keywords: "spud link hub spudlet little spud qr pair token node coding workspace webui telemetry request preview technical endpoint model routing policy" },
  { id: "login", category: "access", section: "login", sectionLabel: "WebUI login", panel: "general", child: "login", label: "WebUI login", description: "Set, replace, or remove the WebUI password.", keywords: "password login authentication security access repeat confirm" },
  { id: "advanced", category: "access", section: "advanced", sectionLabel: "Applications & tools", panel: "advanced", label: "Application access & tool permissions", description: "Local API access, administrator tool gating, and chat history cleanup.", keywords: "advanced api key generate local application enable default mode direct base hydra tools admin plugin verba gate permission clear delete chat history attachments" },
  { id: "redis", category: "system", section: "redis", sectionLabel: "Storage", panel: "redis", label: "Storage & encryption", description: "Redis connection, internal migration, encryption, recovery, and storage health.", keywords: "redis database storage server host port username password ssl tls internal external migrate encryption encrypt decrypt recovery health" },
  { id: "system", category: "system", section: "system", sectionLabel: "Background tasks", panel: "system", label: "Background tasks", description: "Live snapshots, scheduled maintenance, process health, and run history.", keywords: "system tasks schedule maintenance process health background run history core snapshots" },
  { id: "logs", category: "system", section: "logs", sectionLabel: "Logs", panel: "logs", label: "Application logs", description: "Live logs with filters, pause, copy, and tail controls.", keywords: "logs debug diagnostic error warning level filter pause copy tail auto scroll" },
];

export function settingsPageForLegacy(panel: unknown, child: unknown = ""): SettingsPage {
  const key = String(panel || "").trim().toLowerCase();
  const sub = String(child || "").trim().toLowerCase();
  return settingsPages.find(page => page.panel === key && (page.child || "") === sub)
    || settingsPages.find(page => page.panel === key && page.id === key)
    || settingsPages.find(page => page.panel === key)
    || settingsPages[0];
}

export function searchSettings(query: string): SettingsPage[] {
  const terms = query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  if (!terms.length) return [];
  return settingsPages.filter(page => {
    const category = settingsCategories.find(item => item.id === page.category)!;
    const haystack = `${category.label} ${page.label} ${page.description} ${page.keywords}`.toLowerCase();
    return terms.every(term => haystack.includes(term));
  });
}

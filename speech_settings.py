from __future__ import annotations

import contextlib
import importlib
import importlib.util
from typing import Any, Dict, List, Optional

from helpers import redis_client
from tater_runtime_profile import remote_only_enabled

SPEECH_SETTINGS_KEY = "verba_settings:Speech"
DEFAULT_STT_BACKEND = "wyoming" if remote_only_enabled() else "faster_whisper"
DEFAULT_TTS_BACKEND = "wyoming"
DEFAULT_WYOMING_STT_HOST = "127.0.0.1"
DEFAULT_WYOMING_STT_PORT = 10300
DEFAULT_WYOMING_TTS_HOST = "127.0.0.1"
DEFAULT_WYOMING_TTS_PORT = 10200
DEFAULT_WYOMING_TTS_VOICE = ""
DEFAULT_OPENAI_COMPATIBLE_TTS_BASE_URL = ""
DEFAULT_OPENAI_COMPATIBLE_TTS_API_KEY = ""
DEFAULT_OPENAI_COMPATIBLE_TTS_MODEL = "tts-1"
DEFAULT_OPENAI_COMPATIBLE_TTS_VOICE = "alloy"
DEFAULT_SPEECH_ACCELERATION = "auto"
DEFAULT_POCKET_TTS_MODEL = "b6369a24"
DEFAULT_POCKET_TTS_OUTPUT_GAIN = 1.5
DEFAULT_QWEN_TTS_MODEL = "Qwen/Qwen3-TTS-12Hz-0.6B-Base"
QWEN_TTS_LARGE_MODEL = "Qwen/Qwen3-TTS-12Hz-1.7B-Base"
DEFAULT_OMNIVOICE_TTS_MODEL = "k2-fsa/OmniVoice"
DEFAULT_QWEN_TTS_LANGUAGE = "Auto"
DEFAULT_OMNIVOICE_TTS_LANGUAGE = "Auto"
QWEN_TTS_LANGUAGES = (
    "Auto",
    "Chinese",
    "English",
    "Japanese",
    "Korean",
    "German",
    "French",
    "Russian",
    "Portuguese",
    "Spanish",
    "Italian",
)
OMNIVOICE_TTS_LANGUAGES = (
    "Auto",
    "Abadi",
    "Abkhazian",
    "Abron",
    "Abua",
    "Adamawa Fulfulde",
    "Adyghe",
    "Afade",
    "Afrikaans",
    "Agwagwune",
    "Aja (Benin)",
    "Akebu",
    "Alago",
    "Albanian",
    "Algerian Arabic",
    "Algerian Saharan Arabic",
    "Ambo-Pasco Quechua",
    "Ambonese Malay",
    "Amdo Tibetan",
    "Amharic",
    "Anaang",
    "Angika",
    "Antankarana Malagasy",
    "Aragonese",
    "Arbëreshë Albanian",
    "Arequipa-La Unión Quechua",
    "Armenian",
    "Ashe",
    "Ashéninka Perené",
    "Askopan",
    "Assamese",
    "Asturian",
    "Atayal",
    "Awak",
    "Ayacucho Quechua",
    "Azerbaijani",
    "Baatonum",
    "Bacama",
    "Bade",
    "Bafia",
    "Bafut",
    "Bagirmi Fulfulde",
    "Bago-Kusuntu",
    "Baharna Arabic",
    "Bakoko",
    "Balanta-Ganja",
    "Balti",
    "Bamenyam",
    "Bamun",
    "Bangwinji",
    "Banjar",
    "Bankon",
    "Baoulé",
    "Bara Malagasy",
    "Barok",
    "Basa (Cameroon)",
    "Basa (Nigeria)",
    "Bashkir",
    "Basque",
    "Batak Mandailing",
    "Batanga",
    "Bateri",
    "Bats",
    "Bayot",
    "Bebele",
    "Belarusian",
    "Bengali",
    "Betawi",
    "Bhili",
    "Bhojpuri",
    "Bilur",
    "Bima",
    "Bodo",
    "Boghom",
    "Bokyi",
    "Bomu",
    "Bondei",
    "Borgu Fulfulde",
    "Bosnian",
    "Brahui",
    "Braj",
    "Breton",
    "Buduma",
    "Buginese",
    "Bukharic",
    "Bulgarian",
    "Bulu (Cameroon)",
    "Bundeli",
    "Bunun",
    "Bura-Pabir",
    "Burak",
    "Burmese",
    "Burushaski",
    "Cacaloxtepec Mixtec",
    "Cajatambo North Lima Quechua",
    "Cakfem-Mushere",
    "Cameroon Pidgin",
    "Campidanese Sardinian",
    "Cantonese",
    "Catalan",
    "Cebuano",
    "Cen",
    "Central Kurdish",
    "Central Nahuatl",
    "Central Pame",
    "Central Pashto",
    "Central Puebla Nahuatl",
    "Central Tarahumara",
    "Central Yupik",
    "Central-Eastern Niger Fulfulde",
    "Chadian Arabic",
    "Chichewa",
    "Chichicapan Zapotec",
    "Chiga",
    "Chimalapa Zoque",
    "Chimborazo Highland Quichua",
    "Chinese",
    "Chiquián Ancash Quechua",
    "Chitwania Tharu",
    "Chokwe",
    "Chuvash",
    "Cibak",
    "Coastal Konjo",
    "Copainalá Zoque",
    "Cornish",
    "Corongo Ancash Quechua",
    "Croatian",
    "Cross River Mbembe",
    "Cuyamecalco Mixtec",
    "Czech",
    "Dadiya",
    "Dagbani",
    "Dameli",
    "Danish",
    "Dargwa",
    "Dazaga",
    "Deccan",
    "Degema",
    "Dera (Nigeria)",
    "Dghwede",
    "Dhatki",
    "Dhivehi",
    "Dhofari Arabic",
    "Dijim-Bwilim",
    "Dogri",
    "Domaaki",
    "Dotyali",
    "Duala",
    "Dutch",
    "DũYa",
    "Dyula",
    "Eastern Balochi",
    "Eastern Bolivian Guaraní",
    "Eastern Egyptian Bedawi Arabic",
    "Eastern Krahn",
    "Eastern Mari",
    "Eastern Yiddish",
    "Ebrié",
    "Eggon",
    "Egyptian Arabic",
    "Ejagham",
    "Eleme",
    "Eloyi",
    "Embu",
    "English",
    "Erzya",
    "Esan",
    "Esperanto",
    "Estonian",
    "Eton (Cameroon)",
    "Ewondo",
    "Extremaduran",
    "Fang (Equatorial Guinea)",
    "Fanti",
    "Farefare",
    "Fe'fe'",
    "Filipino",
    "Filomena Mata-Coahuitlán Totonac",
    "Finnish",
    "Fipa",
    "French",
    "Fulah",
    "Galician",
    "Gambian Wolof",
    "Ganda",
    "Garhwali",
    "Gawar-Bati",
    "Gawri",
    "Gbagyi",
    "Gbari",
    "Geji",
    "Gen",
    "Georgian",
    "German",
    "Geser-Gorom",
    "Gheg Albanian",
    "Ghomálá'",
    "Gidar",
    "Glavda",
    "Goan Konkani",
    "Goaria",
    "Goemai",
    "Gola",
    "Greek",
    "Guarani",
    "Guduf-Gava",
    "Guerrero Amuzgo",
    "Güilá Zapotec",
    "Gujarati",
    "Gujari",
    "Gulf Arabic",
    "Gurgula",
    "Gusii",
    "Gusilay",
    "Gweno",
    "Hadothi",
    "Hahon",
    "Haitian",
    "Hakha Chin",
    "Hakö",
    "Halia",
    "Hausa",
    "Hawaiian",
    "Hazaragi",
    "Hebrew",
    "Hemba",
    "Herero",
    "Highland Konjo",
    "Hijazi Arabic",
    "Hindi",
    "Huarijio",
    "Huautla Mazatec",
    "Huaxcaleca Nahuatl",
    "Huba",
    "Huitepec Mixtec",
    "Hula",
    "Hungarian",
    "Hunjara-Kaina Ke",
    "Hwana",
    "Ibibio",
    "Icelandic",
    "Idakho-Isukha-Tiriki",
    "Idoma",
    "Igbo",
    "Igo",
    "Ikposo",
    "Ikwere",
    "Imbabura Highland Quichua",
    "Indonesian",
    "Indus Kohistani",
    "Interlingua (International Auxiliary Language Association)",
    "Inupiaq",
    "Irish",
    "Iron Ossetic",
    "Isekiri",
    "Isoko",
    "Italian",
    "Ito",
    "Itzá",
    "Ixtayutla Mixtec",
    "Izon",
    "Jambi Malay",
    "Japanese",
    "Jaqaru",
    "Jauja Wanca Quechua",
    "Jaunsari",
    "Javanese",
    "Jiba",
    "Jju",
    "Judeo-Moroccan Arabic",
    "Juxtlahuaca Mixtec",
    "Kabardian",
    "Kabras",
    "Kabuverdianu",
    "Kabyle",
    "Kachi Koli",
    "Kairak",
    "Kalabari",
    "Kalasha",
    "Kalenjin",
    "Kalkoti",
    "Kamba",
    "Kamo",
    "Kanauji",
    "Kanembu",
    "Kannada",
    "Karekare",
    "Kashmiri",
    "Kathoriya Tharu",
    "Kati",
    "Kazakh",
    "Keiyo",
    "Khams Tibetan",
    "Khana",
    "Khetrani",
    "Khmer",
    "Khowar",
    "Kinga",
    "Kinnauri",
    "Kinyarwanda",
    "Kirghiz",
    "Kirya-Konzəl",
    "Kochila Tharu",
    "Kohistani Shina",
    "Kohumono",
    "Kok Borok",
    "Kol (Papua New Guinea)",
    "Kom (Cameroon)",
    "Koma",
    "Konkani",
    "Konzo",
    "Korean",
    "Korwa",
    "Kota (India)",
    "Koti",
    "Kuanua",
    "Kuanyama",
    "Kui (India)",
    "Kulung (Nigeria)",
    "Kuot",
    "Kushi",
    "Kwambi",
    "Kwasio",
    "Lala-Roba",
    "Lamang",
    "Lao",
    "Larike-Wakasihu",
    "Lasi",
    "Latgalian",
    "Latvian",
    "Levantine Arabic",
    "Liana-Seti",
    "Liberia Kpelle",
    "Liberian English",
    "Libyan Arabic",
    "Ligurian",
    "Lijili",
    "Lingala",
    "Lithuanian",
    "Loarki",
    "Logooli",
    "Logudorese Sardinian",
    "Loja Highland Quichua",
    "Loloda",
    "Longuda",
    "Loxicha Zapotec",
    "Luba-Lulua",
    "Luo",
    "Lushai",
    "Luxembourgish",
    "Maasina Fulfulde",
    "Maba (Chad)",
    "Macedo-Romanian",
    "Macedonian",
    "Mada (Cameroon)",
    "Mafa",
    "Maithili",
    "Malay",
    "Malayalam",
    "Mali",
    "Malinaltepec Me'phaa",
    "Maltese",
    "Mandara",
    "Mandjak",
    "Manggarai",
    "Manipuri",
    "Mansoanka",
    "Manx",
    "Maori",
    "Marathi",
    "Marghi Central",
    "Marghi South",
    "Maria (India)",
    "Marwari (Pakistan)",
    "Masana",
    "Masikoro Malagasy",
    "Matsés",
    "Mazaltepec Zapotec",
    "Mazatlán Mazatec",
    "Mazatlán Mixe",
    "Mbe",
    "Mbo (Cameroon)",
    "Mbum",
    "Medumba",
    "Mekeo",
    "Meru",
    "Mesopotamian Arabic",
    "Mewari",
    "Min Nan Chinese",
    "Mingrelian",
    "Mitlatongo Mixtec",
    "Miya",
    "Mokpwe",
    "Moksha",
    "Mom Jango",
    "Mongolian",
    "Moroccan Arabic",
    "Motu",
    "Mpiemo",
    "Mpumpong",
    "Mundang",
    "Mungaka",
    "Musey",
    "Musgu",
    "Musi",
    "Naba",
    "Najdi Arabic",
    "Nalik",
    "Nawdm",
    "Ndonga",
    "Neapolitan",
    "Nepali",
    "Ngamo",
    "Ngas",
    "Ngiemboon",
    "Ngizim",
    "Ngomba",
    "Ngombale",
    "Nigerian Fulfulde",
    "Nigerian Pidgin",
    "Nimadi",
    "Nobiin",
    "North Mesopotamian Arabic",
    "North Moluccan Malay",
    "Northern Betsimisaraka Malagasy",
    "Northern Hindko",
    "Northern Kurdish",
    "Northern Pame",
    "Northern Pashto",
    "Northern Uzbek",
    "Northwest Gbaya",
    "Norwegian",
    "Norwegian Bokmål",
    "Norwegian Nynorsk",
    "Notsi",
    "Nüpode Huitoto",
    "Nyankpa",
    "Nyungwe",
    "Nzanyi",
    "Occitan",
    "Od",
    "Odia",
    "Odual",
    "Omani Arabic",
    "Ömie",
    "Orizaba Nahuatl",
    "Orma",
    "Ormuri",
    "Oromo",
    "Pahari-Potwari",
    "Paiwan",
    "Panjabi",
    "Papuan Malay",
    "Parkari Koli",
    "Pedi",
    "Pero",
    "Persian",
    "Petats",
    "Phalura",
    "Piemontese",
    "Piya-Kwonci",
    "Plateau Malagasy",
    "Pökoot",
    "Polish",
    "Poqomam",
    "Portuguese",
    "Pulaar",
    "Pular",
    "Puno Quechua",
    "Pushto",
    "Qaqet",
    "Quiotepec Chinantec",
    "Rana Tharu",
    "Rangi",
    "Rapoisi",
    "Ratahan",
    "Rayón Zoque",
    "Romanian",
    "Romansh",
    "Rombo",
    "Rotokas",
    "Rukai",
    "Russian",
    "Sacapulteco",
    "Saidi Arabic",
    "Sakalava Malagasy",
    "Sakizaya",
    "Saleman",
    "Samba Daka",
    "Samba Leko",
    "San Felipe Otlaltepec Popoloca",
    "San Francisco Del Mar Huave",
    "San Juan Atzingo Popoloca",
    "San Martín Itunyoso Triqui",
    "San Miguel El Grande Mixtec",
    "Sansi",
    "Sanskrit",
    "Santa Ana de Tusi Pasco Quechua",
    "Santa Catarina Albarradas Zapotec",
    "Santali",
    "Santiago del Estero Quichua",
    "Saposa",
    "Saraiki",
    "Sardinian",
    "Saya",
    "Sediq",
    "Serbian",
    "Seri",
    "Shina",
    "Shona",
    "Siar-Lak",
    "Sibe",
    "Sicilian",
    "Sihuas Ancash Quechua",
    "Sikkimese",
    "Sinaugoro",
    "Sindhi",
    "Sindhi Bhil",
    "Sinhala",
    "Sinicahua Mixtec",
    "Sipacapense",
    "Siwai",
    "Slovak",
    "Slovenian",
    "Solos",
    "Somali",
    "Soninke",
    "South Giziga",
    "South Ucayali Ashéninka",
    "Southeastern Nochixtlán Mixtec",
    "Southern Betsimisaraka Malagasy",
    "Southern Pashto",
    "Southern Pastaza Quechua",
    "Soyaltepec Mazatec",
    "Spanish",
    "Standard Arabic",
    "Standard Moroccan Tamazight",
    "Sudanese Arabic",
    "Sulka",
    "Svan",
    "Swahili",
    "Swedish",
    "Tae'",
    "Tahaggart Tamahaq",
    "Taita",
    "Tajik",
    "Tamil",
    "Tandroy-Mahafaly Malagasy",
    "Tangale",
    "Tanosy Malagasy",
    "Tarok",
    "Tatar",
    "Tedaga",
    "Telugu",
    "Tem",
    "Teop",
    "Tepeuxila Cuicatec",
    "Tepinapa Chinantec",
    "Tera",
    "Terei",
    "Termanu",
    "Tesaka Malagasy",
    "Tetelcingo Nahuatl",
    "Teutila Cuicatec",
    "Thai",
    "Tibetan",
    "Tidaá Mixtec",
    "Tidore",
    "Tigak",
    "Tigre",
    "Tigrinya",
    "Tilquiapan Zapotec",
    "Tinputz",
    "Tlacoapa Me'phaa",
    "Tlacoatzintepec Chinantec",
    "Tlingit",
    "Toki Pona",
    "Tomoip",
    "Tondano",
    "Tonsea",
    "Tooro",
    "Torau",
    "Torwali",
    "Tsimihety Malagasy",
    "Tsotso",
    "Tswana",
    "Tugen",
    "Tuki",
    "Tula",
    "Tulu",
    "Tunen",
    "Tungag",
    "Tunisian Arabic",
    "Tupuri",
    "Turkana",
    "Turkish",
    "Turkmen",
    "Tututepec Mixtec",
    "Twi",
    "Ubaghara",
    "Uighur",
    "Ukrainian",
    "Umbundu",
    "Upper Sorbian",
    "Urdu",
    "Ushojo",
    "Uzbek",
    "Vai",
    "Vietnamese",
    "Võro",
    "Votic",
    "Waci Gbe",
    "Wadiyara Koli",
    "Waja",
    "Wakhi",
    "Wanga",
    "Wapan",
    "Warji",
    "Welsh",
    "Wemale",
    "Western Frisian",
    "Western Highland Purepecha",
    "Western Juxtlahuaca Mixtec",
    "Western Maninkakan",
    "Western Mari",
    "Western Niger Fulfulde",
    "Western Panjabi",
    "Wolof",
    "Wuzlam",
    "Xanaguía Zapotec",
    "Xhosa",
    "Yace",
    "Yakut",
    "Yalahatan",
    "Yanahuanca Pasco Quechua",
    "Yangben",
    "Yaqui",
    "Yauyos Quechua",
    "Yekhee",
    "Yiddish",
    "Yidgha",
    "Yoruba",
    "Yutanduchi Mixtec",
    "Zacatlán-Ahuacatlán-Tepetzintla Nahuatl",
    "Zarma",
    "Zaza",
    "Zulu",
)
DEFAULT_ANNOUNCEMENT_TTS_BACKEND = DEFAULT_TTS_BACKEND
ANNOUNCEMENT_TTS_DIRECT_BACKEND = "same_as_direct"
DEFAULT_SATELLITE_DUCKING_TARGET_PERCENT = 20
DEFAULT_SATELLITE_DUCKING_ATTACK_MS = 150
DEFAULT_SATELLITE_DUCKING_RELEASE_MS = 350

def _clean(value: Any) -> str:
    return str(value or "").strip()


def _as_int(
    value: Any,
    default: int,
    *,
    minimum: int = 1,
    maximum: int = 65535,
) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    if parsed < int(minimum) or parsed > int(maximum):
        return int(default)
    return int(parsed)


def normalize_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = _clean(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def normalize_tts_output_gain(value: Any, default: float = 1.5) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    return min(4.0, max(0.1, parsed))


def _normalize_stt_backend(value: Any) -> str:
    token = _clean(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"", "default"}:
        return DEFAULT_STT_BACKEND
    if token in {"faster_whisper", "fasterwhisper", "whisper"}:
        return "faster_whisper"
    if token in {"parakeet", "parakeet_onnx", "onnx_parakeet"}:
        return "parakeet_onnx"
    if token in {
        "qwen3_asr",
        "qwen3_asr_llama",
        "qwen3_asr_llama_cpp",
        "qwen_asr",
        "qwen_asr_llama_cpp",
    }:
        return "qwen3_asr_llama_cpp"
    if token == "vosk":
        return "vosk"
    if token == "wyoming":
        return "wyoming"
    return DEFAULT_STT_BACKEND


def _normalize_tts_backend(value: Any) -> str:
    token = _clean(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"", "default"}:
        return DEFAULT_TTS_BACKEND
    if token == "wyoming":
        return "wyoming"
    if token in {"openai_compatible", "openai", "openai_api"}:
        return "openai_compatible"
    if token in {"kokoro", "piper"}:
        return "pocket_tts"
    if token in {"pocket_tts", "pockettts", "pocket"}:
        return "pocket_tts"
    if token in {"qwen", "qwen_tts", "qwen3", "qwen3tts", "qwen3_tts"}:
        return "qwen3_tts"
    if token in {"omni", "omni_voice", "omnivoice", "omnivoice_tts"}:
        return "omnivoice"
    return DEFAULT_TTS_BACKEND


def normalize_announcement_tts_backend(value: Any, *, default: str = DEFAULT_ANNOUNCEMENT_TTS_BACKEND) -> str:
    token = _clean(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"same_as_direct", "same_as_direct_tts", "direct", "direct_tts", "shared"}:
        return ANNOUNCEMENT_TTS_DIRECT_BACKEND
    return _normalize_tts_backend(_clean(value) or default)


def _normalize_tts_model_for_backend(backend: Any, value: Any) -> str:
    token = _normalize_tts_backend(backend)
    if token == "pocket_tts":
        return DEFAULT_POCKET_TTS_MODEL
    if token == "qwen3_tts":
        selected = _clean(value)
        if selected.endswith("-VoiceDesign"):
            return DEFAULT_QWEN_TTS_MODEL
        return selected or DEFAULT_QWEN_TTS_MODEL
    if token == "omnivoice":
        return DEFAULT_OMNIVOICE_TTS_MODEL
    return _clean(value)


def normalize_speech_acceleration(value: Any, *, default: str = DEFAULT_SPEECH_ACCELERATION) -> str:
    token = _clean(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"", "default", "auto"}:
        return default
    if token in {"cpu", "none", "off"}:
        return "cpu"
    if token in {"mps", "metal", "apple", "apple_mps", "apple_metal"}:
        return "mps"
    if token in {"rocm", "amd", "amd_gpu", "amd_rocm", "radeon", "strix", "strix_halo"}:
        return "rocm"
    if token in {"cuda", "gpu", "nvidia", "nvidia_cuda"}:
        return "cuda"
    return default


def _torch_has_rocm(torch_mod: Any) -> bool:
    version = getattr(torch_mod, "version", None)
    return bool(getattr(version, "hip", None))


def _option_rows_from_values(
    values: List[str],
    *,
    current_value: Any = "",
    labels: Optional[Dict[str, str]] = None,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen = set()
    for raw in values:
        value = _clean(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        label = _clean((labels or {}).get(value)) or value
        rows.append({"value": value, "label": label})
    current = _clean(current_value)
    if current and current not in seen:
        rows.insert(0, {"value": current, "label": _clean((labels or {}).get(current)) or current})
    return rows


def _cuda_runtime_available() -> bool:
    with contextlib.suppress(Exception):
        ctranslate2_mod = importlib.import_module("ctranslate2")
        if int(getattr(ctranslate2_mod, "get_cuda_device_count")()) > 0:
            return True
    with contextlib.suppress(Exception):
        torch_mod = importlib.import_module("torch")
        cuda_mod = getattr(torch_mod, "cuda", None)
        if cuda_mod is not None and bool(cuda_mod.is_available()) and not _torch_has_rocm(torch_mod):
            return True
    return False


def _ctranslate2_cuda_available() -> bool:
    with contextlib.suppress(Exception):
        ctranslate2_mod = importlib.import_module("ctranslate2")
        return int(getattr(ctranslate2_mod, "get_cuda_device_count")()) > 0
    return False


def _onnx_cuda_available() -> bool:
    with contextlib.suppress(Exception):
        ort_mod = importlib.import_module("onnxruntime")
        providers = set(getattr(ort_mod, "get_available_providers")() or [])
        return "CUDAExecutionProvider" in providers
    return False


def _onnx_rocm_available() -> bool:
    with contextlib.suppress(Exception):
        ort_mod = importlib.import_module("onnxruntime")
        providers = set(getattr(ort_mod, "get_available_providers")() or [])
        return "ROCMExecutionProvider" in providers or "MIGraphXExecutionProvider" in providers
    return False


def _torch_cuda_available() -> bool:
    with contextlib.suppress(Exception):
        torch_mod = importlib.import_module("torch")
        cuda_mod = getattr(torch_mod, "cuda", None)
        return cuda_mod is not None and bool(cuda_mod.is_available()) and not _torch_has_rocm(torch_mod)
    return False


def _torch_rocm_available() -> bool:
    with contextlib.suppress(Exception):
        torch_mod = importlib.import_module("torch")
        cuda_mod = getattr(torch_mod, "cuda", None)
        return cuda_mod is not None and bool(cuda_mod.is_available()) and _torch_has_rocm(torch_mod)
    return False


def _mps_runtime_available() -> bool:
    with contextlib.suppress(Exception):
        torch_mod = importlib.import_module("torch")
        backends = getattr(torch_mod, "backends", None)
        mps = getattr(backends, "mps", None)
        if mps is not None and bool(mps.is_available()):
            return True
    return False


def _stt_backend_option_rows() -> List[Dict[str, Any]]:
    return [
        {"value": "faster_whisper", "label": "Faster Whisper"},
        {"value": "parakeet_onnx", "label": "Parakeet ONNX"},
        {
            "value": "qwen3_asr_llama_cpp",
            "label": "Qwen3-ASR (llama.cpp, Experimental)",
        },
        {"value": "wyoming", "label": "Wyoming"},
        {"value": "vosk", "label": "Vosk"},
    ]


def _prefer_value_first(values: List[str], preferred_value: Any) -> List[str]:
    preferred = _clean(preferred_value)
    ordered = [_clean(value) for value in values if _clean(value)]
    if preferred and preferred in ordered:
        ordered = [value for value in ordered if value != preferred]
        ordered.insert(0, preferred)
    return ordered


def _module_available(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except Exception:
        return False


def _pocket_tts_model_option_rows(*, current_value: Any = "") -> List[Dict[str, str]]:
    labels = {DEFAULT_POCKET_TTS_MODEL: "Pocket TTS (CPU Voice Clone)"}
    return _option_rows_from_values([DEFAULT_POCKET_TTS_MODEL], current_value=current_value, labels=labels)


def get_speech_settings() -> Dict[str, Any]:
    shared = redis_client.hgetall(SPEECH_SETTINGS_KEY) or {}
    current_tts_backend = _normalize_tts_backend(shared.get("tts_backend"))
    announcement_backend = normalize_announcement_tts_backend(
        shared.get("announcement_tts_backend"),
        default=DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
    )

    def legacy_clone_value(field: str, backend: str, *, announcement: bool = False) -> str:
        prefix = "announcement_" if announcement else ""
        preferred = (
            f"{prefix}omnivoice_tts_{field}"
            if backend == "omnivoice"
            else f"{prefix}qwen_tts_{field}"
        )
        alternate = (
            f"{prefix}qwen_tts_{field}"
            if backend == "omnivoice"
            else f"{prefix}omnivoice_tts_{field}"
        )
        return _clean(shared.get(preferred)) or _clean(shared.get(alternate))

    direct_clone_audio = _clean(shared.get("tts_clone_audio")) or legacy_clone_value(
        "clone_audio", current_tts_backend
    )
    direct_clone_text = _clean(shared.get("tts_clone_text")) or legacy_clone_value(
        "clone_text", current_tts_backend
    )
    announcement_clone_audio = (
        _clean(shared.get("announcement_tts_clone_audio"))
        if "announcement_tts_clone_audio" in shared
        else legacy_clone_value("clone_audio", announcement_backend, announcement=True) or direct_clone_audio
    )
    announcement_clone_text = (
        _clean(shared.get("announcement_tts_clone_text"))
        if "announcement_tts_clone_text" in shared
        else legacy_clone_value("clone_text", announcement_backend, announcement=True) or direct_clone_text
    )

    def announcement_value(field: str, direct_field: str, default: Any = "") -> str:
        value = shared.get(field) if field in shared else shared.get(direct_field)
        return _clean(value) or _clean(default)

    has_announcement_wyoming_host = "announcement_wyoming_tts_host" in shared
    has_announcement_wyoming_port = "announcement_wyoming_tts_port" in shared
    has_announcement_wyoming_voice = "announcement_wyoming_tts_voice" in shared
    has_announcement_openai_base_url = "announcement_openai_tts_base_url" in shared
    has_announcement_openai_api_key = "announcement_openai_tts_api_key" in shared
    return {
        "stt_backend": _normalize_stt_backend(shared.get("stt_backend")),
        "acceleration": normalize_speech_acceleration(shared.get("acceleration")),
        "wyoming_stt_host": _clean(shared.get("wyoming_stt_host")) or DEFAULT_WYOMING_STT_HOST,
        "wyoming_stt_port": _as_int(shared.get("wyoming_stt_port"), DEFAULT_WYOMING_STT_PORT),
        "tts_backend": current_tts_backend,
        "tts_model": _normalize_tts_model_for_backend(current_tts_backend, shared.get("tts_model")),
        "tts_voice": _clean(shared.get("tts_voice")),
        "pocket_tts_output_gain": normalize_tts_output_gain(
            shared.get("pocket_tts_output_gain"),
            DEFAULT_POCKET_TTS_OUTPUT_GAIN,
        ),
        "tts_clone_audio": direct_clone_audio,
        "tts_clone_text": direct_clone_text,
        "qwen_tts_clone_audio": direct_clone_audio,
        "qwen_tts_clone_text": direct_clone_text,
        "qwen_tts_language": _clean(shared.get("qwen_tts_language")) or DEFAULT_QWEN_TTS_LANGUAGE,
        "qwen_tts_instruct": _clean(shared.get("qwen_tts_instruct")),
        "omnivoice_tts_clone_audio": direct_clone_audio,
        "omnivoice_tts_clone_text": direct_clone_text,
        "omnivoice_tts_language": _clean(shared.get("omnivoice_tts_language")) or DEFAULT_OMNIVOICE_TTS_LANGUAGE,
        "omnivoice_tts_instruct": _clean(shared.get("omnivoice_tts_instruct")),
        "wyoming_tts_host": _clean(shared.get("wyoming_tts_host")) or DEFAULT_WYOMING_TTS_HOST,
        "wyoming_tts_port": _as_int(shared.get("wyoming_tts_port"), DEFAULT_WYOMING_TTS_PORT),
        "wyoming_tts_voice": _clean(shared.get("wyoming_tts_voice")) or DEFAULT_WYOMING_TTS_VOICE,
        "openai_tts_base_url": _clean(shared.get("openai_tts_base_url")) or DEFAULT_OPENAI_COMPATIBLE_TTS_BASE_URL,
        "openai_tts_api_key": _clean(shared.get("openai_tts_api_key")) or DEFAULT_OPENAI_COMPATIBLE_TTS_API_KEY,
        "announcement_tts_backend": announcement_backend,
        "announcement_tts_model": _normalize_tts_model_for_backend(
            announcement_backend,
            shared.get("announcement_tts_model"),
        ),
        "announcement_tts_voice": _clean(shared.get("announcement_tts_voice")),
        "announcement_pocket_tts_output_gain": normalize_tts_output_gain(
            shared.get("announcement_pocket_tts_output_gain")
            if "announcement_pocket_tts_output_gain" in shared
            else shared.get("pocket_tts_output_gain"),
            DEFAULT_POCKET_TTS_OUTPUT_GAIN,
        ),
        "announcement_tts_clone_audio": announcement_clone_audio,
        "announcement_tts_clone_text": announcement_clone_text,
        "announcement_qwen_tts_clone_audio": announcement_clone_audio,
        "announcement_qwen_tts_clone_text": announcement_clone_text,
        "announcement_qwen_tts_language": announcement_value(
            "announcement_qwen_tts_language", "qwen_tts_language", DEFAULT_QWEN_TTS_LANGUAGE
        ),
        "announcement_qwen_tts_instruct": announcement_value(
            "announcement_qwen_tts_instruct", "qwen_tts_instruct"
        ),
        "announcement_omnivoice_tts_clone_audio": announcement_clone_audio,
        "announcement_omnivoice_tts_clone_text": announcement_clone_text,
        "announcement_omnivoice_tts_language": announcement_value(
            "announcement_omnivoice_tts_language", "omnivoice_tts_language", DEFAULT_OMNIVOICE_TTS_LANGUAGE
        ),
        "announcement_omnivoice_tts_instruct": announcement_value(
            "announcement_omnivoice_tts_instruct", "omnivoice_tts_instruct"
        ),
        "satellite_ducking_target_percent": _as_int(
            shared.get("satellite_ducking_target_percent"),
            DEFAULT_SATELLITE_DUCKING_TARGET_PERCENT,
            minimum=0,
            maximum=100,
        ),
        "satellite_ducking_attack_ms": _as_int(
            shared.get("satellite_ducking_attack_ms"),
            DEFAULT_SATELLITE_DUCKING_ATTACK_MS,
            minimum=0,
            maximum=10000,
        ),
        "satellite_ducking_release_ms": _as_int(
            shared.get("satellite_ducking_release_ms"),
            DEFAULT_SATELLITE_DUCKING_RELEASE_MS,
            minimum=0,
            maximum=10000,
        ),
        "announcement_wyoming_tts_host": (
            _clean(shared.get("announcement_wyoming_tts_host"))
            if has_announcement_wyoming_host
            else _clean(shared.get("wyoming_tts_host"))
        ) or DEFAULT_WYOMING_TTS_HOST,
        "announcement_wyoming_tts_port": _as_int(
            shared.get("announcement_wyoming_tts_port")
            if has_announcement_wyoming_port
            else shared.get("wyoming_tts_port"),
            DEFAULT_WYOMING_TTS_PORT,
        ),
        "announcement_wyoming_tts_voice": (
            _clean(shared.get("announcement_wyoming_tts_voice"))
            if has_announcement_wyoming_voice
            else _clean(shared.get("wyoming_tts_voice"))
        ),
        "announcement_openai_tts_base_url": (
            _clean(shared.get("announcement_openai_tts_base_url"))
            if has_announcement_openai_base_url
            else _clean(shared.get("openai_tts_base_url"))
        ) or DEFAULT_OPENAI_COMPATIBLE_TTS_BASE_URL,
        "announcement_openai_tts_api_key": (
            _clean(shared.get("announcement_openai_tts_api_key"))
            if has_announcement_openai_api_key
            else _clean(shared.get("openai_tts_api_key"))
        ) or DEFAULT_OPENAI_COMPATIBLE_TTS_API_KEY,
    }


def save_speech_settings(
    *,
    stt_backend: Any,
    wyoming_stt_host: Any,
    wyoming_stt_port: Any,
    tts_backend: Any,
    tts_model: Any,
    tts_voice: Any,
    wyoming_tts_host: Any,
    wyoming_tts_port: Any,
    wyoming_tts_voice: Any,
    openai_tts_base_url: Any,
    openai_tts_api_key: Any,
    announcement_tts_backend: Any,
    announcement_tts_model: Any,
    announcement_tts_voice: Any,
    pocket_tts_output_gain: Any = DEFAULT_POCKET_TTS_OUTPUT_GAIN,
    tts_clone_audio: Any = None,
    tts_clone_text: Any = None,
    qwen_tts_clone_audio: Any = "",
    qwen_tts_clone_text: Any = "",
    qwen_tts_language: Any = DEFAULT_QWEN_TTS_LANGUAGE,
    qwen_tts_instruct: Any = "",
    omnivoice_tts_clone_audio: Any = "",
    omnivoice_tts_clone_text: Any = "",
    omnivoice_tts_language: Any = DEFAULT_OMNIVOICE_TTS_LANGUAGE,
    omnivoice_tts_instruct: Any = "",
    announcement_pocket_tts_output_gain: Any = None,
    announcement_tts_clone_audio: Any = None,
    announcement_tts_clone_text: Any = None,
    announcement_qwen_tts_clone_audio: Any = None,
    announcement_qwen_tts_clone_text: Any = None,
    announcement_qwen_tts_language: Any = None,
    announcement_qwen_tts_instruct: Any = None,
    announcement_omnivoice_tts_clone_audio: Any = None,
    announcement_omnivoice_tts_clone_text: Any = None,
    announcement_omnivoice_tts_language: Any = None,
    announcement_omnivoice_tts_instruct: Any = None,
    announcement_wyoming_tts_host: Any = None,
    announcement_wyoming_tts_port: Any = None,
    announcement_wyoming_tts_voice: Any = None,
    announcement_openai_tts_base_url: Any = None,
    announcement_openai_tts_api_key: Any = None,
    satellite_ducking_target_percent: Any = DEFAULT_SATELLITE_DUCKING_TARGET_PERCENT,
    satellite_ducking_attack_ms: Any = DEFAULT_SATELLITE_DUCKING_ATTACK_MS,
    satellite_ducking_release_ms: Any = DEFAULT_SATELLITE_DUCKING_RELEASE_MS,
    acceleration: Any = DEFAULT_SPEECH_ACCELERATION,
) -> None:
    direct_tts_backend = _normalize_tts_backend(tts_backend)
    shared_clone_audio = _clean(
        tts_clone_audio
        if tts_clone_audio is not None
        else qwen_tts_clone_audio or omnivoice_tts_clone_audio
    )
    shared_clone_text = _clean(
        tts_clone_text
        if tts_clone_text is not None
        else qwen_tts_clone_text or omnivoice_tts_clone_text
    )
    shared_announcement_clone_audio = _clean(
        announcement_tts_clone_audio
        if announcement_tts_clone_audio is not None
        else announcement_qwen_tts_clone_audio
        if announcement_qwen_tts_clone_audio is not None
        else announcement_omnivoice_tts_clone_audio
        if announcement_omnivoice_tts_clone_audio is not None
        else shared_clone_audio
    )
    shared_announcement_clone_text = _clean(
        announcement_tts_clone_text
        if announcement_tts_clone_text is not None
        else announcement_qwen_tts_clone_text
        if announcement_qwen_tts_clone_text is not None
        else announcement_omnivoice_tts_clone_text
        if announcement_omnivoice_tts_clone_text is not None
        else shared_clone_text
    )
    redis_client.hset(
        SPEECH_SETTINGS_KEY,
        mapping={
            "stt_backend": _normalize_stt_backend(stt_backend),
            "acceleration": normalize_speech_acceleration(acceleration),
            "wyoming_stt_host": _clean(wyoming_stt_host) or DEFAULT_WYOMING_STT_HOST,
            "wyoming_stt_port": str(_as_int(wyoming_stt_port, DEFAULT_WYOMING_STT_PORT)),
            "tts_backend": direct_tts_backend,
            "tts_model": _normalize_tts_model_for_backend(direct_tts_backend, tts_model),
            "tts_voice": _clean(tts_voice),
            "pocket_tts_output_gain": str(
                normalize_tts_output_gain(pocket_tts_output_gain, DEFAULT_POCKET_TTS_OUTPUT_GAIN)
            ),
            "tts_clone_audio": shared_clone_audio,
            "tts_clone_text": shared_clone_text,
            "qwen_tts_clone_audio": shared_clone_audio,
            "qwen_tts_clone_text": shared_clone_text,
            "qwen_tts_language": _clean(qwen_tts_language) or DEFAULT_QWEN_TTS_LANGUAGE,
            "qwen_tts_instruct": _clean(qwen_tts_instruct),
            "omnivoice_tts_clone_audio": shared_clone_audio,
            "omnivoice_tts_clone_text": shared_clone_text,
            "omnivoice_tts_language": _clean(omnivoice_tts_language) or DEFAULT_OMNIVOICE_TTS_LANGUAGE,
            "omnivoice_tts_instruct": _clean(omnivoice_tts_instruct),
            "wyoming_tts_host": _clean(wyoming_tts_host) or DEFAULT_WYOMING_TTS_HOST,
            "wyoming_tts_port": str(_as_int(wyoming_tts_port, DEFAULT_WYOMING_TTS_PORT)),
            "wyoming_tts_voice": _clean(wyoming_tts_voice),
            "openai_tts_base_url": _clean(openai_tts_base_url),
            "openai_tts_api_key": _clean(openai_tts_api_key),
            "announcement_tts_backend": normalize_announcement_tts_backend(
                announcement_tts_backend,
                default=DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
            ),
            "announcement_tts_model": _normalize_tts_model_for_backend(
                announcement_tts_backend,
                announcement_tts_model,
            ),
            "announcement_tts_voice": _clean(announcement_tts_voice),
            "announcement_pocket_tts_output_gain": str(
                normalize_tts_output_gain(
                    announcement_pocket_tts_output_gain
                    if announcement_pocket_tts_output_gain is not None
                    else pocket_tts_output_gain,
                    DEFAULT_POCKET_TTS_OUTPUT_GAIN,
                )
            ),
            "announcement_tts_clone_audio": shared_announcement_clone_audio,
            "announcement_tts_clone_text": shared_announcement_clone_text,
            "announcement_qwen_tts_clone_audio": shared_announcement_clone_audio,
            "announcement_qwen_tts_clone_text": shared_announcement_clone_text,
            "announcement_qwen_tts_language": _clean(
                announcement_qwen_tts_language
                if announcement_qwen_tts_language is not None
                else qwen_tts_language
            ) or DEFAULT_QWEN_TTS_LANGUAGE,
            "announcement_qwen_tts_instruct": _clean(
                announcement_qwen_tts_instruct
                if announcement_qwen_tts_instruct is not None
                else qwen_tts_instruct
            ),
            "announcement_omnivoice_tts_clone_audio": shared_announcement_clone_audio,
            "announcement_omnivoice_tts_clone_text": shared_announcement_clone_text,
            "announcement_omnivoice_tts_language": _clean(
                announcement_omnivoice_tts_language
                if announcement_omnivoice_tts_language is not None
                else omnivoice_tts_language
            ) or DEFAULT_OMNIVOICE_TTS_LANGUAGE,
            "announcement_omnivoice_tts_instruct": _clean(
                announcement_omnivoice_tts_instruct
                if announcement_omnivoice_tts_instruct is not None
                else omnivoice_tts_instruct
            ),
            "satellite_ducking_target_percent": str(
                _as_int(
                    satellite_ducking_target_percent,
                    DEFAULT_SATELLITE_DUCKING_TARGET_PERCENT,
                    minimum=0,
                    maximum=100,
                )
            ),
            "satellite_ducking_attack_ms": str(
                _as_int(
                    satellite_ducking_attack_ms,
                    DEFAULT_SATELLITE_DUCKING_ATTACK_MS,
                    minimum=0,
                    maximum=10000,
                )
            ),
            "satellite_ducking_release_ms": str(
                _as_int(
                    satellite_ducking_release_ms,
                    DEFAULT_SATELLITE_DUCKING_RELEASE_MS,
                    minimum=0,
                    maximum=10000,
                )
            ),
            "announcement_wyoming_tts_host": (
                _clean(announcement_wyoming_tts_host)
                if announcement_wyoming_tts_host is not None
                else _clean(wyoming_tts_host)
            ) or DEFAULT_WYOMING_TTS_HOST,
            "announcement_wyoming_tts_port": str(
                _as_int(
                    announcement_wyoming_tts_port if announcement_wyoming_tts_port is not None else wyoming_tts_port,
                    DEFAULT_WYOMING_TTS_PORT,
                )
            ),
            "announcement_wyoming_tts_voice": (
                _clean(announcement_wyoming_tts_voice)
                if announcement_wyoming_tts_voice is not None
                else _clean(wyoming_tts_voice)
            ),
            "announcement_openai_tts_base_url": (
                _clean(announcement_openai_tts_base_url)
                if announcement_openai_tts_base_url is not None
                else _clean(openai_tts_base_url)
            ),
            "announcement_openai_tts_api_key": (
                _clean(announcement_openai_tts_api_key)
                if announcement_openai_tts_api_key is not None
                else _clean(openai_tts_api_key)
            ),
        },
    )
    with contextlib.suppress(Exception):
        redis_client.hdel(
            SPEECH_SETTINGS_KEY,
            "tts_public_base_url",
        )


def save_tts_clone_audio_path(path: Any, *, profile: Any = "direct") -> None:
    announcement = _clean(profile).lower() == "announcement"
    field = "announcement_tts_clone_audio" if announcement else "tts_clone_audio"
    legacy_fields = (
        ("announcement_qwen_tts_clone_audio", "announcement_omnivoice_tts_clone_audio")
        if announcement
        else ("qwen_tts_clone_audio", "omnivoice_tts_clone_audio")
    )
    value = _clean(path)
    redis_client.hset(SPEECH_SETTINGS_KEY, mapping={field: value, **{key: value for key in legacy_fields}})


def save_tts_clone_text(text: Any, *, profile: Any = "direct") -> None:
    announcement = _clean(profile).lower() == "announcement"
    field = "announcement_tts_clone_text" if announcement else "tts_clone_text"
    legacy_fields = (
        ("announcement_qwen_tts_clone_text", "announcement_omnivoice_tts_clone_text")
        if announcement
        else ("qwen_tts_clone_text", "omnivoice_tts_clone_text")
    )
    value = _clean(text)
    redis_client.hset(SPEECH_SETTINGS_KEY, mapping={field: value, **{key: value for key in legacy_fields}})


def get_speech_ui_payload(settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    current = dict(settings or get_speech_settings())
    current_tts_backend = _normalize_tts_backend(current.get("tts_backend"))
    current_tts_model = _clean(current.get("tts_model"))
    current_tts_voice = _clean(current.get("tts_voice"))

    pocket_model_rows = _pocket_tts_model_option_rows(
        current_value=current_tts_model if current_tts_backend == "pocket_tts" else ""
    )
    qwen_model_rows = _option_rows_from_values(
        [DEFAULT_QWEN_TTS_MODEL, QWEN_TTS_LARGE_MODEL],
        current_value=current_tts_model if current_tts_backend == "qwen3_tts" else "",
        labels={
            DEFAULT_QWEN_TTS_MODEL: "Qwen3-TTS 0.6B Base — Faster",
            QWEN_TTS_LARGE_MODEL: "Qwen3-TTS 1.7B Base — Higher quality",
        },
    )
    omnivoice_model_rows = _option_rows_from_values(
        [DEFAULT_OMNIVOICE_TTS_MODEL],
        current_value=current_tts_model if current_tts_backend == "omnivoice" else "",
        labels={DEFAULT_OMNIVOICE_TTS_MODEL: "OmniVoice"},
    )
    voice_options_by_model: Dict[str, List[Dict[str, str]]] = {}

    return {
        "stt_backend_options": _stt_backend_option_rows(),
        "capabilities": {
            "cuda_available": _cuda_runtime_available(),
            "ctranslate2_cuda_available": _ctranslate2_cuda_available(),
            "onnx_cuda_available": _onnx_cuda_available(),
            "onnx_rocm_available": _onnx_rocm_available(),
            "torch_cuda_available": _torch_cuda_available(),
            "torch_rocm_available": _torch_rocm_available(),
            "mps_available": _mps_runtime_available(),
            "parakeet_onnx_available": _module_available("onnx_asr"),
        },
        "acceleration_options": [
            {"value": "auto", "label": "Auto"},
            {"value": "cpu", "label": "CPU"},
            {"value": "cuda", "label": "NVIDIA CUDA"},
            {"value": "rocm", "label": "AMD ROCm"},
            {"value": "mps", "label": "Apple Metal / MPS"},
        ],
        "tts_backend_options": [
            {"value": "wyoming", "label": "Wyoming"},
            {"value": "openai_compatible", "label": "OpenAI-Compatible"},
            {"value": "pocket_tts", "label": "Pocket TTS"},
            {"value": "qwen3_tts", "label": "Qwen3-TTS"},
            {"value": "omnivoice", "label": "OmniVoice"},
        ],
        "tts_model_options_by_backend": {
            "openai_compatible": [],
            "pocket_tts": pocket_model_rows,
            "qwen3_tts": qwen_model_rows,
            "omnivoice": omnivoice_model_rows,
        },
        "tts_language_options_by_backend": {
            "qwen3_tts": _option_rows_from_values(
                list(QWEN_TTS_LANGUAGES),
                labels={"Auto": "Auto detect (recommended)"},
            ),
            "omnivoice": _option_rows_from_values(
                list(OMNIVOICE_TTS_LANGUAGES),
                labels={"Auto": "Auto detect (recommended)"},
            ),
        },
        "tts_voice_options_by_model": voice_options_by_model,
    }


def get_announcement_tts_ui_payload(
    *,
    backend: Any = "",
    model: Any = "",
    voice: Any = "",
    default_backend: str = DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
) -> Dict[str, Any]:
    current_backend = normalize_announcement_tts_backend(backend, default=default_backend)
    current_model = _clean(model)
    current_voice = _clean(voice)
    speech_ui = get_speech_ui_payload(
        {
            "tts_backend": default_backend if current_backend == ANNOUNCEMENT_TTS_DIRECT_BACKEND else current_backend,
            "tts_model": current_model,
            "tts_voice": current_voice,
        }
    )
    backend_options = [
        {
            "value": ANNOUNCEMENT_TTS_DIRECT_BACKEND,
            "label": "Same as Direct Reply TTS",
        },
        *list(speech_ui.get("tts_backend_options") or []),
    ]
    return {
        "tts_backend_options": backend_options,
        "tts_model_options_by_backend": dict(speech_ui.get("tts_model_options_by_backend") or {}),
        "tts_language_options_by_backend": dict(speech_ui.get("tts_language_options_by_backend") or {}),
        "tts_voice_options_by_model": dict(speech_ui.get("tts_voice_options_by_model") or {}),
    }


def _selected_option_value(rows: List[Dict[str, str]], current_value: Any = "") -> str:
    current = _clean(current_value)
    values = [_clean((row or {}).get("value")) for row in list(rows or [])]
    values = [value for value in values if value]
    if current and current in values:
        return current
    return values[0] if values else ""


def build_announcement_tts_fields(
    *,
    backend: Any = "",
    model: Any = "",
    voice: Any = "",
    default_backend: str = DEFAULT_ANNOUNCEMENT_TTS_BACKEND,
    backend_key: str = "tts_backend",
    model_key: str = "tts_model",
    voice_key: str = "tts_voice",
    backend_label: str = "TTS Backend",
    model_label: str = "TTS Model",
    voice_label: str = "TTS Voice",
    backend_description: str = "",
    model_description: str = "",
    voice_description: str = "",
) -> List[Dict[str, Any]]:
    payload = get_announcement_tts_ui_payload(
        backend=backend,
        model=model,
        voice=voice,
        default_backend=default_backend,
    )
    current_backend = normalize_announcement_tts_backend(backend, default=default_backend)
    backend_options = [row for row in list(payload.get("tts_backend_options") or []) if isinstance(row, dict)]
    model_options_by_backend = dict(payload.get("tts_model_options_by_backend") or {})
    voice_options_by_model = dict(payload.get("tts_voice_options_by_model") or {})

    current_model_options = [row for row in list(model_options_by_backend.get(current_backend) or []) if isinstance(row, dict)]
    current_model = _selected_option_value(current_model_options, model)
    current_voice_options = [row for row in list(voice_options_by_model.get(current_model) or []) if isinstance(row, dict)]
    current_voice = _selected_option_value(current_voice_options, voice)

    backend_keys_with_models = [key for key, rows in model_options_by_backend.items() if isinstance(rows, list) and rows]
    backend_keys_with_voices = []
    for backend_key_value in backend_keys_with_models:
        model_rows = list(model_options_by_backend.get(backend_key_value) or [])
        if any(list(voice_options_by_model.get(_clean((row or {}).get("value"))) or []) for row in model_rows):
            backend_keys_with_voices.append(backend_key_value)

    return [
        {
            "key": backend_key,
            "label": _clean(backend_label) or "TTS Backend",
            "type": "select",
            "options": backend_options,
            "value": current_backend,
            "description": _clean(backend_description),
        },
        {
            "key": model_key,
            "label": _clean(model_label) or "TTS Model",
            "type": "select",
            "options": current_model_options,
            "value": current_model,
            "description": _clean(model_description),
            "show_when": {"source_key": backend_key, "any_of": backend_keys_with_models},
            "dependent_options": {
                "source_key": backend_key,
                "options_by_source": model_options_by_backend,
                "default_options": current_model_options,
            },
        },
        {
            "key": voice_key,
            "label": _clean(voice_label) or "TTS Voice",
            "type": "select",
            "options": current_voice_options,
            "value": current_voice,
            "description": _clean(voice_description),
            "show_when": {"source_key": backend_key, "any_of": backend_keys_with_voices},
            "dependent_options": {
                "source_key": model_key,
                "options_by_source": voice_options_by_model,
                "default_options": current_voice_options,
            },
        },
    ]

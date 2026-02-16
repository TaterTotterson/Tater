import ast
import base64
import codecs
import csv
import fnmatch
import io
import json
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time
import uuid
import zipfile
import importlib.util
from datetime import datetime, timedelta
import ipaddress
import socket
from html.parser import HTMLParser
import redis
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from helpers import redis_client
from plugin_loader import load_plugins_from_directory
from plugin_base import ToolPlugin
from plugin_registry import reload_plugins
from plugin_settings import get_plugin_enabled
from vision_settings import get_vision_settings as get_shared_vision_settings
from notify import dispatch_notification_sync, notifier_supports_attachments
from notify.queue import ALLOWED_PLATFORMS as NOTIFY_ALLOWED_PLATFORMS
from notify.queue import normalize_platform as normalize_notify_platform
from notify.queue import load_default_targets, normalize_origin, resolve_targets
from conversation_media_refs import load_recent_media_refs, save_media_ref


BASE_DIR = Path(__file__).resolve().parent
AGENT_LAB_DIR = BASE_DIR / "agent_lab"
AGENT_PLUGINS_DIR = AGENT_LAB_DIR / "plugins"
AGENT_PLATFORMS_DIR = AGENT_LAB_DIR / "platforms"
AGENT_ARTIFACTS_DIR = AGENT_LAB_DIR / "artifacts"
AGENT_DOCUMENTS_DIR = AGENT_LAB_DIR / "documents"
AGENT_DOWNLOADS_DIR = AGENT_LAB_DIR / "downloads"
AGENT_WORKSPACE_DIR = AGENT_LAB_DIR / "workspace"
AGENT_LOGS_DIR = AGENT_LAB_DIR / "logs"
SKILLS_DIR = BASE_DIR / "skills"
AGENT_SKILLS_DIR = SKILLS_DIR / "agent_lab"
AGENT_REQUIREMENTS = AGENT_LAB_DIR / "requirements.txt"

STABLE_PLUGINS_DIR = BASE_DIR / os.getenv("TATER_PLUGIN_DIR", "plugins")
STABLE_PLATFORMS_DIR = BASE_DIR / "platforms"

_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")
_SAFE_DEP_RE = re.compile(r"^[A-Za-z0-9_.\\-\\[\\]==<>!~]+$")
_AGENT_LAB_SHORTCUT_DIRS = {
    "plugins",
    "platforms",
    "documents",
    "downloads",
    "workspace",
    "artifacts",
    "logs",
}
_READ_FILE_MAX_CHARS = int(os.getenv("TATER_READ_FILE_MAX_CHARS", "400000"))
_READ_PDF_MAX_PAGES = int(os.getenv("TATER_READ_PDF_MAX_PAGES", "120"))
_READ_DOCX_MAX_TABLE_ROWS = int(os.getenv("TATER_READ_DOCX_MAX_TABLE_ROWS", "300"))
_READ_XLSX_MAX_SHEETS = int(os.getenv("TATER_READ_XLSX_MAX_SHEETS", "4"))
_READ_XLSX_MAX_ROWS_PER_SHEET = int(os.getenv("TATER_READ_XLSX_MAX_ROWS_PER_SHEET", "120"))
_READ_XLSX_MAX_COLS_PER_ROW = int(os.getenv("TATER_READ_XLSX_MAX_COLS_PER_ROW", "24"))
_READ_CSV_MAX_ROWS = int(os.getenv("TATER_READ_CSV_MAX_ROWS", "500"))
_READ_CSV_MAX_COLS = int(os.getenv("TATER_READ_CSV_MAX_COLS", "64"))
_READ_PPTX_MAX_SLIDES = int(os.getenv("TATER_READ_PPTX_MAX_SLIDES", "120"))
_SEARCH_DEFAULT_MAX_RESULTS = int(os.getenv("TATER_SEARCH_MAX_RESULTS", "100"))
_SEARCH_MAX_FILE_CHARS = int(os.getenv("TATER_SEARCH_MAX_FILE_CHARS", "200000"))
_ARCHIVE_LIST_MAX_ENTRIES = int(os.getenv("TATER_ARCHIVE_LIST_MAX_ENTRIES", "1000"))
_ARCHIVE_EXTRACT_MAX_FILES = int(os.getenv("TATER_ARCHIVE_EXTRACT_MAX_FILES", "1000"))
_ARCHIVE_EXTRACT_MAX_TOTAL_BYTES = int(os.getenv("TATER_ARCHIVE_EXTRACT_MAX_TOTAL_BYTES", "100000000"))
_VISION_WEBUI_BLOB_PREFIX = "webui:file:"
_VISION_MAX_SCAN_LIMIT = int(os.getenv("TATER_VISION_MAX_SCAN_LIMIT", "400"))
_VISION_MAX_IMAGE_BYTES = int(os.getenv("TATER_VISION_MAX_IMAGE_BYTES", str(12 * 1024 * 1024)))
_VISION_ALLOWED_MIMETYPES = {
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/webp",
    "image/gif",
    "image/bmp",
    "image/tiff",
}
_VISION_DEFAULT_PROMPT = (
    "Describe this image clearly and concisely. Mention important objects, people, actions, "
    "and any visible text."
)

_VISION_BLOB_REDIS: Any = None

WEB_SEARCH_API_KEY_REDIS_KEY = "tater:web_search:google_api_key"
WEB_SEARCH_CX_REDIS_KEY = "tater:web_search:google_cx"
WEB_SEARCH_LEGACY_SETTINGS_KEY = "plugin_settings:Web Search"
WEB_SEARCH_TIMEOUT_SEC = int(os.getenv("TATER_WEB_SEARCH_TIMEOUT_SEC", "15"))
WEB_SEARCH_MAX_RESULTS = int(os.getenv("TATER_WEB_SEARCH_MAX_RESULTS", "10"))
WEB_SEARCH_MAX_RESPONSE_BYTES = int(os.getenv("TATER_WEB_SEARCH_MAX_RESPONSE_BYTES", "2000000"))
WEB_SEARCH_MAX_SNIPPET_CHARS = int(os.getenv("TATER_WEB_SEARCH_MAX_SNIPPET_CHARS", "600"))

SEND_MESSAGE_SETTINGS_KEYS = (
    "plugin_settings:Send Message",
    "plugin_settings: Send Message",
)
SEND_MESSAGE_HA_API_DEFAULT_KEY = "ENABLE_HA_API_NOTIFICATION"
SEND_MESSAGE_RECENT_MEDIA_MAX_AGE_SEC = int(
    os.getenv(
        "TATER_SEND_MESSAGE_RECENT_MEDIA_MAX_AGE_SEC",
        os.getenv("TATER_SEND_MESSAGE_LATEST_IMAGE_MAX_AGE_SEC", "300"),
    )
)
SEND_MESSAGE_AUTO_MEDIA_MAX_ITEMS = int(os.getenv("TATER_SEND_MESSAGE_AUTO_MEDIA_MAX_ITEMS", "3"))

AI_TASKS_KEY_PREFIX = "reminders:"
AI_TASKS_DUE_ZSET = "reminders:due"
AI_TASKS_DAILY_MARKERS = ("every day", "everyday", "daily", "each day")
AI_TASKS_WEEKLY_MARKERS = ("every week", "weekly")
AI_TASKS_WEEKDAY_MAP = {
    "monday": 0,
    "mon": 0,
    "tuesday": 1,
    "tue": 1,
    "tues": 1,
    "wednesday": 2,
    "wed": 2,
    "thursday": 3,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "friday": 4,
    "fri": 4,
    "saturday": 5,
    "sat": 5,
    "sunday": 6,
    "sun": 6,
}
AI_TASKS_LOCAL_TZ_HINT_RE = re.compile(r"\bassume\s+local\s+timezone\b\.?", re.IGNORECASE)
AI_TASKS_WEATHER_DEFAULT_HINT_RE = re.compile(
    r"\bif\s+location\s+is\s+not\s+specified,\s+use\s+the\s+configured\s+default\s+weather\s+location\b\.?",
    re.IGNORECASE,
)
AI_TASKS_TIME_PREFIX_RE = re.compile(
    r"^\s*(?:(?:in|after)\s+\d+\s*(?:seconds?|minutes?|hours?|days?|weeks?)|at\s+(?:\d{3,4}|\d{1,2}(?::\d{2})?(?::\d{2})?)\s*(?:am|pm)?)\b",
    re.IGNORECASE,
)
AI_TASKS_SCHEDULE_PREFIX_PATTERNS = (
    re.compile(
        r"^\s*(?:every\s+day|everyday|daily|each\s+day|weekdays?|weekends?)\b(?:\s+at\s+(?:\d{3,4}|\d{1,2}(?::\d{2})?(?::\d{2})?)\s*(?:am|pm)?)?\s*(?:,|:|-)?\s*",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(?:every\s+week|weekly)\b(?:\s+on\s+[a-z,\s]+)?(?:\s+at\s+(?:\d{3,4}|\d{1,2}(?::\d{2})?(?::\d{2})?)\s*(?:am|pm)?)?\s*(?:,|:|-)?\s*",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*every\s+\d+\s*(?:seconds?|minutes?|hours?|days?|weeks?)\b\s*(?:,|:|-)?\s*",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(?:in|after)\s+\d+\s*(?:seconds?|minutes?|hours?|days?|weeks?)\b\s*(?:,|:|-)?\s*",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*at\s+(?:\d{3,4}|\d{1,2}(?::\d{2})?(?::\d{2})?)\s*(?:am|pm)?\b\s*(?:,|:|-)?\s*",
        re.IGNORECASE,
    ),
)

MEMORY_HASH_PREFIX = "tater:memory"
MEMORY_EXPLICIT_ONLY_REDIS_KEY = "tater:memory:explicit_only"
MEMORY_DEFAULT_TTL_REDIS_KEY = "tater:memory:default_ttl_sec"
MEMORY_MAX_LIST_LIMIT = int(os.getenv("TATER_MEMORY_MAX_LIST_LIMIT", "200"))
MEMORY_MAX_VALUE_CHARS = int(os.getenv("TATER_MEMORY_MAX_VALUE_CHARS", "4000"))
MEMORY_SEARCH_MAX_RESULTS = int(os.getenv("TATER_MEMORY_SEARCH_MAX_RESULTS", "50"))
MEMORY_KEY_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,120}$")
MEMORY_EXPLICIT_PHRASES = (
    "remember",
    "make that the default",
    "set that as default",
    "use this by default",
    "always use",
    "yes save that",
    "save that",
    "save this preference",
    "store this preference",
)
MEMORY_VOLATILE_PREFIXES = (
    "volatile.",
    "temp.",
    "last.",
    "recent.",
    "cache.",
    "session.",
)


def _ensure_dirs() -> None:
    for path in (
        AGENT_LAB_DIR,
        AGENT_PLUGINS_DIR,
        AGENT_PLATFORMS_DIR,
        AGENT_ARTIFACTS_DIR,
        AGENT_DOCUMENTS_DIR,
        AGENT_DOWNLOADS_DIR,
        AGENT_WORKSPACE_DIR,
        AGENT_LOGS_DIR,
        AGENT_SKILLS_DIR,
        SKILLS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
    if not AGENT_REQUIREMENTS.exists():
        AGENT_REQUIREMENTS.write_text("", encoding="utf-8")


def _log_write(action: str, path: Path, size: int = 0) -> None:
    try:
        _ensure_dirs()
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} | {action} | {path} | {size} bytes\n"
        with (AGENT_LOGS_DIR / "agent_writes.log").open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        return


def _resolve_safe_path(path: str, allowed_roots: List[Path]) -> Optional[Path]:
    if not path:
        return None

    raw = str(path).strip()
    normalized = raw.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]

    if normalized.startswith("/app/"):
        app_rel = normalized[len("/app/") :]
        raw = str(BASE_DIR / app_rel)
    elif normalized == "/agent_lab":
        raw = str(AGENT_LAB_DIR)
    elif normalized.startswith("/agent_lab/"):
        suffix = normalized[len("/agent_lab/") :]
        raw = str(AGENT_LAB_DIR / suffix)
    elif normalized.startswith("/"):
        # Treat absolute shortcut paths like /plugins/... or /documents/...
        # as agent_lab-relative for Agent Mode ergonomics.
        abs_rel = normalized[1:]
        parts = abs_rel.split("/", 1)
        head = parts[0] if parts else ""
        if head in _AGENT_LAB_SHORTCUT_DIRS:
            raw = str(AGENT_LAB_DIR / abs_rel)
    else:
        parts = normalized.split("/", 1)
        head = parts[0] if parts else ""
        if head in _AGENT_LAB_SHORTCUT_DIRS:
            raw = str(AGENT_LAB_DIR / normalized)

    p = Path(raw)
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    else:
        p = p.resolve()

    for root in allowed_roots:
        try:
            root_resolved = root.resolve()
        except Exception:
            root_resolved = root
        if p == root_resolved or root_resolved in p.parents:
            return p
    return None


def _coerce_int(value: Any, default: int, min_value: int = 0, max_value: Optional[int] = None) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    if out < min_value:
        out = min_value
    if max_value is not None and out > max_value:
        out = max_value
    return out


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "yes", "on"}:
            return True
        if v in {"0", "false", "no", "off"}:
            return False
    return bool(default)


def _slice_content(text: str, start: Any = 0, max_chars: Any = None) -> Tuple[str, Dict[str, Any]]:
    start_i = _coerce_int(start, default=0, min_value=0)
    limit_default = _READ_FILE_MAX_CHARS
    limit_i = _coerce_int(
        max_chars if max_chars is not None else limit_default,
        default=limit_default,
        min_value=1,
        max_value=2_000_000,
    )
    total = len(text)
    if start_i > total:
        start_i = total
    end_i = min(total, start_i + limit_i)
    chunk = text[start_i:end_i]
    has_more = end_i < total
    meta = {
        "start": start_i,
        "end": end_i,
        "max_chars": limit_i,
        "total_chars": total,
        "returned_chars": len(chunk),
        "has_more": has_more,
        "next_start": end_i if has_more else None,
    }
    return chunk, meta


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if not raw:
        return ""

    if raw.startswith(codecs.BOM_UTF8):
        text = raw.decode("utf-8-sig", errors="replace")
    elif raw.startswith(codecs.BOM_UTF16_LE) or raw.startswith(codecs.BOM_UTF16_BE):
        text = raw.decode("utf-16", errors="replace")
    elif raw.startswith(codecs.BOM_UTF32_LE) or raw.startswith(codecs.BOM_UTF32_BE):
        text = raw.decode("utf-32", errors="replace")
    else:
        if b"\x00" in raw[:8192]:
            raise ValueError(
                "Binary file is not readable as plain text. Supported document formats: "
                ".pdf, .docx, .xlsx, .xlsm, .csv, .tsv, .pptx."
            )
        text = raw.decode("utf-8", errors="replace")

    return text


def _read_pdf_text(path: Path) -> Tuple[str, Dict[str, Any]]:
    try:
        from pypdf import PdfReader
    except Exception as e:
        raise RuntimeError("PDF parsing requires the `pypdf` package.") from e

    reader = PdfReader(str(path))
    total_pages = len(reader.pages)
    max_pages = max(1, _READ_PDF_MAX_PAGES)
    pages_to_read = min(total_pages, max_pages)
    chunks: List[str] = []

    for idx in range(pages_to_read):
        page_text = ""
        try:
            page_text = reader.pages[idx].extract_text() or ""
        except Exception:
            page_text = ""
        page_text = page_text.strip()
        if page_text:
            chunks.append(f"[Page {idx + 1}]\n{page_text}")
        else:
            chunks.append(f"[Page {idx + 1}]\n")

    if not chunks:
        chunks = ["[No extractable text found in PDF.]"]

    merged = "\n\n".join(chunks).strip()
    if not merged:
        merged = "[No extractable text found in PDF.]"
    metadata = {
        "format": "pdf",
        "pages": total_pages,
        "pages_read": pages_to_read,
        "source_truncated": bool(total_pages > pages_to_read),
    }
    return merged, metadata


def _read_docx_text(path: Path) -> Tuple[str, Dict[str, Any]]:
    try:
        import docx
    except Exception as e:
        raise RuntimeError("DOCX parsing requires the `python-docx` package.") from e

    doc = docx.Document(str(path))
    chunks: List[str] = []
    for para in doc.paragraphs:
        text = (para.text or "").strip()
        if text:
            chunks.append(text)

    table_rows = 0
    total_table_rows = 0
    max_rows = max(1, _READ_DOCX_MAX_TABLE_ROWS)
    for table in doc.tables:
        for row in table.rows:
            total_table_rows += 1
            if table_rows >= max_rows:
                break
            cells = [str((cell.text or "")).strip() for cell in row.cells]
            if any(cells):
                chunks.append(" | ".join(cells))
            table_rows += 1

    merged = "\n".join(chunks).strip() or "[No extractable text found in DOCX.]"
    metadata = {
        "format": "docx",
        "paragraphs": len(doc.paragraphs),
        "table_rows": total_table_rows,
        "table_rows_read": min(table_rows, max_rows),
        "source_truncated": bool(total_table_rows > max_rows),
    }
    return merged, metadata


def _read_xlsx_text(path: Path) -> Tuple[str, Dict[str, Any]]:
    try:
        from openpyxl import load_workbook
    except Exception as e:
        raise RuntimeError("XLSX parsing requires the `openpyxl` package.") from e

    max_sheets = max(1, _READ_XLSX_MAX_SHEETS)
    max_rows = max(1, _READ_XLSX_MAX_ROWS_PER_SHEET)
    max_cols = max(1, _READ_XLSX_MAX_COLS_PER_ROW)
    wb = load_workbook(filename=str(path), read_only=True, data_only=True)
    chunks: List[str] = []
    sheet_names = list(wb.sheetnames or [])
    sheets_to_read = sheet_names[:max_sheets]
    source_truncated = False

    for sheet_name in sheets_to_read:
        ws = wb[sheet_name]
        chunks.append(f"[Sheet: {sheet_name}]")
        row_index = 0
        for row in ws.iter_rows(min_row=1, max_row=max_rows + 1, max_col=max_cols, values_only=True):
            if row_index >= max_rows:
                source_truncated = True
                break
            values = ["" if cell is None else str(cell) for cell in row]
            if any(v.strip() for v in values):
                chunks.append("\t".join(values).rstrip())
            row_index += 1
        if row_index == 0:
            chunks.append("[Empty sheet]")
        chunks.append("")

    wb.close()
    merged = "\n".join(chunks).strip() or "[No extractable content found in XLSX.]"
    source_truncated = bool(source_truncated or len(sheet_names) > len(sheets_to_read))
    metadata = {
        "format": "xlsx",
        "sheets": len(sheet_names),
        "sheets_read": len(sheets_to_read),
        "rows_per_sheet_limit": max_rows,
        "source_truncated": source_truncated,
    }
    return merged, metadata


def _sniff_csv_delimiter(sample: str, path: Path) -> str:
    if path.suffix.lower() == ".tsv":
        return "\t"
    try:
        dialect = csv.Sniffer().sniff(sample or "", delimiters=",;\t|")
        delim = getattr(dialect, "delimiter", ",")
        return delim if isinstance(delim, str) and delim else ","
    except Exception:
        return ","


def _read_csv_text(path: Path) -> Tuple[str, Dict[str, Any]]:
    try:
        raw = path.read_bytes()
    except Exception as e:
        raise RuntimeError(f"Unable to read CSV file: {e}") from e
    if not raw:
        return "", {"format": "csv", "rows": 0, "rows_read": 0, "source_truncated": False}

    try:
        text = raw.decode("utf-8-sig")
    except Exception:
        text = raw.decode("utf-8", errors="replace")

    delimiter = _sniff_csv_delimiter(text[:4096], path)
    max_rows = max(1, _READ_CSV_MAX_ROWS)
    max_cols = max(1, _READ_CSV_MAX_COLS)
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter)

    out_lines: List[str] = []
    total_rows = 0
    rows_read = 0
    max_seen_cols = 0
    source_truncated = False

    for row in reader:
        total_rows += 1
        max_seen_cols = max(max_seen_cols, len(row))
        if rows_read >= max_rows:
            source_truncated = True
            continue
        if len(row) > max_cols:
            source_truncated = True
        visible = row[:max_cols]
        out_lines.append("\t".join(str(cell) for cell in visible))
        rows_read += 1

    merged = "\n".join(out_lines).strip()
    if not merged and total_rows == 0:
        merged = ""
    elif not merged:
        merged = "[No non-empty rows found in CSV/TSV.]"

    metadata = {
        "format": "csv",
        "delimiter": delimiter,
        "rows": total_rows,
        "rows_read": rows_read,
        "max_columns_seen": max_seen_cols,
        "source_truncated": bool(source_truncated),
    }
    return merged, metadata


def _read_pptx_text(path: Path) -> Tuple[str, Dict[str, Any]]:
    try:
        from pptx import Presentation
    except Exception as e:
        raise RuntimeError("PPTX parsing requires the `python-pptx` package.") from e

    prs = Presentation(str(path))
    total_slides = len(prs.slides)
    max_slides = max(1, _READ_PPTX_MAX_SLIDES)
    slides_to_read = min(total_slides, max_slides)
    chunks: List[str] = []

    for idx, slide in enumerate(prs.slides):
        if idx >= slides_to_read:
            break
        slide_lines: List[str] = []
        for shape in slide.shapes:
            if hasattr(shape, "has_text_frame") and shape.has_text_frame:
                raw = shape.text or ""
                text = raw.strip()
                if text:
                    slide_lines.append(text)
            elif hasattr(shape, "table") and shape.table is not None:
                for row in shape.table.rows:
                    cells = [str((cell.text or "")).strip() for cell in row.cells]
                    if any(cells):
                        slide_lines.append(" | ".join(cells))

        if slide_lines:
            chunks.append(f"[Slide {idx + 1}]\n" + "\n".join(slide_lines))
        else:
            chunks.append(f"[Slide {idx + 1}]\n")

    merged = "\n\n".join(chunks).strip() or "[No extractable text found in PPTX.]"
    metadata = {
        "format": "pptx",
        "slides": total_slides,
        "slides_read": slides_to_read,
        "source_truncated": bool(total_slides > slides_to_read),
    }
    return merged, metadata


def _extract_file_content(path: Path) -> Tuple[str, Dict[str, Any]]:
    ext = path.suffix.lower()
    if ext == ".pdf":
        return _read_pdf_text(path)
    if ext == ".docx":
        return _read_docx_text(path)
    if ext in {".xlsx", ".xlsm"}:
        return _read_xlsx_text(path)
    if ext in {".csv", ".tsv"}:
        return _read_csv_text(path)
    if ext == ".pptx":
        return _read_pptx_text(path)
    return _read_text(path), {"format": "text", "source_truncated": False}


def _sanitize_filename(name: str) -> str:
    raw = os.path.basename((name or "").strip())
    if not raw:
        return ""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", raw)
    safe = safe.lstrip(".")
    return safe or ""


def _is_private_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
    )


def _host_is_private(host: str) -> Tuple[bool, Optional[str]]:
    if not host:
        return True, "URL must include a host."
    try:
        if _is_private_ip(host):
            return True, None
    except Exception:
        return True, "Invalid host."
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception:
        return True, "Unable to resolve host."
    for info in infos:
        ip_str = info[4][0]
        if _is_private_ip(ip_str):
            return True, None
    return False, None


def _validate_url(url: str) -> Optional[str]:
    if not url:
        return "URL is required."
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return "Only http/https URLs are allowed."
    if not parsed.hostname:
        return "URL must include a host."
    is_private, err = _host_is_private(parsed.hostname)
    if err:
        return err
    if is_private:
        return "Private or local network hosts are not allowed."
    return None


def _normalize_url_input(url: Any) -> str:
    raw = _as_text(url).strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        return f"https:{raw}"
    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme:
        return raw
    if re.match(r"^(?:www\.)?[A-Za-z0-9][A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:[/:?#].*)?$", raw):
        return f"https://{raw}"
    return raw


def _clean_redis_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore").strip()
    return str(value).strip()


def _web_search_settings() -> Tuple[str, str]:
    api_key = ""
    cx = ""
    try:
        api_key = _clean_redis_str(redis_client.get(WEB_SEARCH_API_KEY_REDIS_KEY))
        cx = _clean_redis_str(redis_client.get(WEB_SEARCH_CX_REDIS_KEY))
    except Exception:
        api_key = ""
        cx = ""

    if api_key and cx:
        return api_key, cx

    try:
        legacy = redis_client.hgetall(WEB_SEARCH_LEGACY_SETTINGS_KEY) or {}
    except Exception:
        legacy = {}
    if not api_key:
        api_key = _clean_redis_str(legacy.get("GOOGLE_API_KEY") or legacy.get("google_api_key"))
    if not cx:
        cx = _clean_redis_str(legacy.get("GOOGLE_CX") or legacy.get("google_cx"))
    return api_key, cx


def search_web(
    query: str,
    *,
    num_results: int = 5,
    start: int = 1,
    site: Optional[str] = None,
    safe: str = "active",
    country: Optional[str] = None,
    language: Optional[str] = None,
    timeout_sec: int = WEB_SEARCH_TIMEOUT_SEC,
) -> Dict[str, Any]:
    q = str(query or "").strip()
    if not q:
        return {"tool": "search_web", "ok": False, "error": "query is required."}

    api_key, cx = _web_search_settings()
    if not api_key or not cx:
        return {
            "tool": "search_web",
            "ok": False,
            "error": "Web search is not configured. Set Google API Key and Search Engine ID (CX) in WebUI Settings.",
            "needs": [
                "Please set Google API Key in WebUI Settings > Web Search.",
                "Please set Google Search Engine ID (CX) in WebUI Settings > Web Search.",
            ],
        }

    max_results = _coerce_int(num_results, default=5, min_value=1, max_value=WEB_SEARCH_MAX_RESULTS)
    start_index = _coerce_int(start, default=1, min_value=1, max_value=91)
    timeout_val = _coerce_int(timeout_sec, default=WEB_SEARCH_TIMEOUT_SEC, min_value=3, max_value=60)
    safe_mode = str(safe or "active").strip().lower()
    if safe_mode not in {"active", "off"}:
        safe_mode = "active"

    params: Dict[str, Any] = {
        "key": api_key,
        "cx": cx,
        "q": q,
        "num": max_results,
        "start": start_index,
        "safe": safe_mode,
    }
    site_val = str(site or "").strip()
    if site_val:
        params["siteSearch"] = site_val
        params["siteSearchFilter"] = "i"

    country_val = str(country or "").strip().lower()
    if country_val and re.fullmatch(r"[a-z]{2}", country_val):
        params["gl"] = country_val

    language_val = str(language or "").strip().lower()
    if language_val:
        if language_val.startswith("lang_"):
            params["lr"] = language_val
        elif re.fullmatch(r"[a-z]{2}", language_val):
            params["lr"] = f"lang_{language_val}"

    endpoint = "https://www.googleapis.com/customsearch/v1"
    url = endpoint + "?" + urllib.parse.urlencode(params, doseq=True)
    req = urllib.request.Request(url, headers={"User-Agent": "Tater-AgentLab/1.0"})

    try:
        with urllib.request.urlopen(req, timeout=timeout_val) as resp:
            raw = resp.read(WEB_SEARCH_MAX_RESPONSE_BYTES)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read(1000).decode("utf-8", errors="replace")
        except Exception:
            body = ""
        message = body.strip() or str(e)
        return {"tool": "search_web", "ok": False, "error": f"Google CSE request failed ({e.code}): {message}"}
    except Exception as e:
        return {"tool": "search_web", "ok": False, "error": f"Web search failed: {e}"}

    try:
        payload = json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        return {"tool": "search_web", "ok": False, "error": "Invalid response from Google CSE."}

    if isinstance(payload, dict) and payload.get("error"):
        err = payload.get("error") or {}
        msg = str(err.get("message") or "Unknown Google CSE error.")
        return {"tool": "search_web", "ok": False, "error": msg}

    items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        items = []

    results: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        link = str(item.get("link") or "").strip()
        if not link:
            continue
        title = str(item.get("title") or "").strip() or link
        snippet = str(item.get("snippet") or "").strip()
        if len(snippet) > WEB_SEARCH_MAX_SNIPPET_CHARS:
            snippet = snippet[:WEB_SEARCH_MAX_SNIPPET_CHARS].rstrip() + "..."
        results.append(
            {
                "title": title,
                "url": link,
                "snippet": snippet,
                "display_url": str(item.get("displayLink") or "").strip(),
            }
        )

    search_info = payload.get("searchInformation") if isinstance(payload, dict) else {}
    search_time = None
    if isinstance(search_info, dict):
        search_time = search_info.get("searchTime")

    next_start = None
    total_results = None
    queries_blob = payload.get("queries") if isinstance(payload, dict) else None
    if isinstance(queries_blob, dict):
        req_pages = queries_blob.get("request")
        if isinstance(req_pages, list) and req_pages:
            req0 = req_pages[0] if isinstance(req_pages[0], dict) else {}
            raw_total = req0.get("totalResults")
            if raw_total is not None:
                try:
                    total_results = int(str(raw_total))
                except Exception:
                    total_results = None
        next_pages = queries_blob.get("nextPage")
        if isinstance(next_pages, list) and next_pages:
            np0 = next_pages[0] if isinstance(next_pages[0], dict) else {}
            raw_next = np0.get("startIndex")
            if raw_next is not None:
                try:
                    next_start = int(raw_next)
                except Exception:
                    next_start = None

    return {
        "tool": "search_web",
        "ok": True,
        "query": q,
        "start": start_index,
        "count": len(results),
        "num_results": max_results,
        "results": results,
        "site_filter": site_val or None,
        "search_time_sec": search_time,
        "total_results": total_results,
        "has_more": bool(next_start),
        "next_start": next_start,
    }


def _send_message_boolish(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _send_message_normalize_platform(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if raw in {"home assistant", "ha"}:
        return "homeassistant"
    compact = raw.replace(" ", "")
    if compact == "homeassistant":
        return "homeassistant"
    return normalize_notify_platform(raw)


def _send_message_load_settings() -> Dict[str, Any]:
    for key in SEND_MESSAGE_SETTINGS_KEYS:
        try:
            data = redis_client.hgetall(key) or {}
        except Exception:
            data = {}
        if isinstance(data, dict) and data:
            return data
    return {}


def _send_message_extract_target_hint(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    for pattern in (r"![^\s]+", r"#[A-Za-z0-9][A-Za-z0-9._:-]*", r"@[A-Za-z0-9_]+"):
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    text = re.sub(r"^(?:room|channel|chat)\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\s+(?:in|on)\s+(?:discord|irc|matrix|telegram|home\s*assistant|homeassistant|ntfy|wordpress)\b.*$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return text.strip(" .")


def _send_message_coerce_targets(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        return dict(payload)
    if isinstance(payload, str):
        hint = _send_message_extract_target_hint(payload)
        if hint:
            return {"channel": hint}
    return {}


def _send_message_clean_attachment_payload(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict):
            out.append(dict(item))
    return out


def _send_message_blob_candidates(*, blob_key: Any, file_id: Any) -> List[str]:
    candidates: List[str] = []
    blob = str(blob_key or "").strip()
    if blob:
        candidates.append(blob)
    fid = str(file_id or "").strip()
    if fid:
        if fid.startswith(_VISION_WEBUI_BLOB_PREFIX) or fid.startswith("tater:blob:") or fid.startswith("tater:matrix:"):
            candidates.append(fid)
        else:
            candidates.append(f"{_VISION_WEBUI_BLOB_PREFIX}{fid}")
            candidates.append(fid)
    unique: List[str] = []
    seen = set()
    for key in candidates:
        if key and key not in seen:
            seen.add(key)
            unique.append(key)
    return unique


def _send_message_resolve_blob_key(*, blob_key: Any, file_id: Any) -> str:
    candidates = _send_message_blob_candidates(blob_key=blob_key, file_id=file_id)
    if not candidates:
        return ""
    client = _get_blob_redis_client()
    for key in candidates:
        try:
            found = client.get(key)
        except Exception:
            found = None
        if found is not None:
            return key
    return ""


def _send_message_attachment_from_ref(ref: Any, *, default_type: str = "image") -> Optional[Dict[str, Any]]:
    if not isinstance(ref, dict):
        return None
    out: Dict[str, Any] = {}
    media_type = str(ref.get("type") or default_type).strip().lower()
    if media_type not in {"image", "audio", "video", "file"}:
        media_type = default_type if default_type in {"image", "audio", "video", "file"} else "file"
    out["type"] = media_type

    name = str(ref.get("name") or "").strip()
    if name:
        out["name"] = name
    mimetype = str(ref.get("mimetype") or "").strip()
    if mimetype:
        out["mimetype"] = mimetype

    resolved_blob = _send_message_resolve_blob_key(blob_key=ref.get("blob_key"), file_id=ref.get("file_id"))
    if resolved_blob:
        out["blob_key"] = resolved_blob
        return out

    path = str(ref.get("path") or "").strip()
    if path:
        out["path"] = path
        return out

    url = str(ref.get("url") or "").strip()
    if url:
        out["url"] = url
        return out
    return None


def _send_message_build_needs(error_text: str) -> List[str]:
    text = str(error_text or "").strip().lower()
    if "missing destination platform" in text:
        return ["Which platform should I send to? (discord, irc, matrix, telegram, homeassistant, ntfy, wordpress)"]
    if "missing target channel/room" in text:
        return ["What room/channel/chat should I send this to?"]
    if "missing message" in text:
        return ["What message should I send? You can also include attachments/media."]
    if "missing ntfy topic" in text:
        return ["What ntfy topic should I send this to? You can pass targets.topic or targets.channel."]
    if "missing wordpress settings" in text:
        return ["WordPress is not configured. Provide wordpress site URL, username, and app password settings."]
    return []


def _send_message_media_ref_is_fresh(ref: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(ref, dict):
        return False
    raw_ts = ref.get("updated_at")
    if raw_ts in (None, ""):
        # Legacy refs may not include timestamps; treat as usable.
        return True
    try:
        ts = float(raw_ts)
    except Exception:
        return True
    if ts <= 0:
        return False
    max_age = max(0, int(SEND_MESSAGE_RECENT_MEDIA_MAX_AGE_SEC))
    if max_age <= 0:
        return True
    return (time.time() - ts) <= float(max_age)


def _send_message_text_refs_recent_media(text: Any) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return False
    # Must reference prior context somehow.
    if not bool(
        re.search(
            r"\b(this|that|it|them|those|these|latest|same|above|previous|earlier|here|here's|there|from before)\b",
            s,
        )
    ):
        return False

    # Must also mention sending/attaching media or use media nouns.
    return bool(
        re.search(
            r"\b(attach|post|send|add|share|upload|include|drop|image|photo|picture|pic|screenshot|logo|attachment|attachments|media|video|audio|music|song|file|files|document|pdf|zip|archive)\b",
            s,
        )
    )


def _send_message_requested_media_count(text: Any, *, max_available: int) -> int:
    if max_available <= 0:
        return 0
    s = str(text or "").strip().lower()
    if not s:
        return 1
    if re.search(r"\b(all|everything|all of them|all attachments|all files)\b", s):
        return min(max_available, max(1, SEND_MESSAGE_AUTO_MEDIA_MAX_ITEMS))
    if re.search(r"\b(both|two|2)\b", s):
        return min(max_available, 2)
    if re.search(r"\b(three|3)\b", s):
        return min(max_available, 3)
    return 1


def _send_message_media_types_from_text(text: Any) -> List[str]:
    s = str(text or "").strip().lower()
    if not s:
        return []
    media_types: List[str] = []
    if re.search(r"\b(image|photo|picture|pic|screenshot|logo)\b", s):
        media_types.append("image")
    if re.search(r"\b(video|clip|movie)\b", s):
        media_types.append("video")
    if re.search(r"\b(audio|music|song|voice|mp3|wav|flac)\b", s):
        media_types.append("audio")
    if re.search(r"\b(file|files|document|documents|pdf|zip|archive|attachment|attachments)\b", s):
        media_types.append("file")
    return media_types


def _download_file_detect_media(path: Path, content_type: str) -> Tuple[str, str]:
    mime = str(content_type or "").split(";", 1)[0].strip().lower()
    if not mime:
        mime = str(mimetypes.guess_type(str(path))[0] or "").strip().lower()
    media_type = "file"
    if mime.startswith("image/"):
        media_type = "image"
    elif mime.startswith("audio/"):
        media_type = "audio"
    elif mime.startswith("video/"):
        media_type = "video"
    if not mime:
        if media_type == "image":
            mime = "image/png"
        elif media_type == "audio":
            mime = "audio/mpeg"
        elif media_type == "video":
            mime = "video/mp4"
        else:
            mime = "application/octet-stream"
    return media_type, mime


def _send_message_context_platform_scope(
    *,
    platform: Optional[str],
    origin: Optional[Dict[str, Any]],
) -> Tuple[str, str]:
    origin_map = dict(origin) if isinstance(origin, dict) else {}
    raw_platform = _as_text(origin_map.get("platform") or platform).strip().lower()
    ref_platform = _send_message_normalize_platform(raw_platform)
    if not ref_platform and raw_platform:
        ref_platform = re.sub(r"[^a-z0-9_.:\-]+", "_", raw_platform).strip("_")
    ref_scope = str(
        origin_map.get("scope")
        or origin_map.get("channel_id")
        or origin_map.get("room_id")
        or origin_map.get("chat_id")
        or origin_map.get("session_id")
        or ""
    ).strip()
    return ref_platform, ref_scope


def _save_media_ref_for_context(
    *,
    ref: Dict[str, Any],
    platform: Optional[str],
    origin: Optional[Dict[str, Any]],
) -> None:
    ref_platform, ref_scope = _send_message_context_platform_scope(platform=platform, origin=origin)
    if not ref_platform or not ref_scope:
        return
    try:
        save_media_ref(
            redis_client,
            platform=ref_platform,
            scope=ref_scope,
            ref=ref,
        )
    except Exception:
        return


def _load_recent_media_refs_for_context(
    *,
    platform: Optional[str],
    origin: Optional[Dict[str, Any]],
    limit: int = 8,
    media_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    ref_platform, ref_scope = _send_message_context_platform_scope(platform=platform, origin=origin)
    if not ref_platform or not ref_scope:
        return []
    try:
        refs = load_recent_media_refs(
            redis_client,
            platform=ref_platform,
            scope=ref_scope,
            limit=max(1, limit),
            media_types=media_types or None,
            fresh_within_sec=SEND_MESSAGE_RECENT_MEDIA_MAX_AGE_SEC,
        )
    except Exception:
        refs = []
    return [item for item in refs if _send_message_media_ref_is_fresh(item)]


def _send_message_select_recent_refs(
    *,
    refs: List[Dict[str, Any]],
    text: str,
    explicit_use_latest: Optional[bool],
) -> List[Dict[str, Any]]:
    if not refs:
        return []

    requested_types = _send_message_media_types_from_text(text)
    candidates = refs
    if requested_types:
        type_set = set(requested_types)
        typed = [item for item in refs if str(item.get("type") or "").strip().lower() in type_set]
        if typed:
            candidates = typed

    if explicit_use_latest is False:
        return []
    if explicit_use_latest is True:
        count = _send_message_requested_media_count(text, max_available=len(candidates))
        return candidates[: max(1, count)]

    if text and _send_message_text_refs_recent_media(text):
        count = _send_message_requested_media_count(text, max_available=len(candidates))
        return candidates[: max(1, count)]

    return []


def _ai_tasks_normalize_channel_targets(dest: str, targets: Dict[str, Any]) -> Dict[str, Any]:
    t = dict(targets or {})
    channel_ref = t.get("channel")

    if not channel_ref:
        if dest == "discord":
            channel_ref = t.get("channel_id")
        elif dest == "matrix":
            channel_ref = t.get("room_id")
        elif dest == "homeassistant":
            channel_ref = t.get("device_service")
        elif dest == "telegram":
            channel_ref = t.get("chat_id")

    if not channel_ref:
        return t

    ref = str(channel_ref).strip()
    if not ref:
        return {}

    if dest == "discord":
        if ref.isdigit():
            return {"channel_id": ref}
        return {"channel": ref}

    if dest == "matrix":
        if not ref.startswith(("!", "#")) and ":" in ref:
            ref = f"#{ref}"
        return {"room_id": ref}

    if dest == "homeassistant":
        return {"device_service": ref}

    if dest == "telegram":
        return {"chat_id": ref}

    return {"channel": ref}


def _ai_tasks_extract_time_of_day_parts(text: str) -> Optional[Tuple[int, int, int]]:
    raw = str(text or "").strip().lower()
    if not raw:
        return None

    m = re.search(r"(?:^|\b)(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?\s*(am|pm)(?:\b|$)", raw)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2) or 0)
        second = int(m.group(3) or 0)
        meridiem = str(m.group(4) or "").lower()
        if hour < 1 or hour > 12 or minute > 59 or second > 59:
            return None
        if meridiem == "am":
            hour = 0 if hour == 12 else hour
        else:
            hour = 12 if hour == 12 else hour + 12
        return (hour, minute, second)

    m24 = re.search(r"(?:^|\b)([01]?\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?(?:\b|$)", raw)
    if m24:
        return (int(m24.group(1)), int(m24.group(2)), int(m24.group(3) or 0))

    # Accept compact local times like "at 710" or "at 0710".
    m_compact = re.search(r"\bat\s+(\d{3,4})(?:\b|$)", raw)
    if m_compact:
        digits = str(m_compact.group(1) or "")
        if len(digits) == 3:
            hour = int(digits[0])
            minute = int(digits[1:])
        else:
            hour = int(digits[:2])
            minute = int(digits[2:])
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return (hour, minute, 0)

    # Accept common natural phrasing like "at 6" / "at 18" / "at 6:30"
    m_at = re.search(r"\bat\s+([01]?\d|2[0-3])(?::([0-5]\d))?(?::([0-5]\d))?(?:\b|$)", raw)
    if m_at:
        return (int(m_at.group(1)), int(m_at.group(2) or 0), int(m_at.group(3) or 0))

    # Accept "6 o'clock" style.
    m_oclock = re.search(r"\b([1-9]|1[0-2])\s*o'?clock\b", raw)
    if m_oclock:
        return (int(m_oclock.group(1)), 0, 0)

    return None


def _ai_tasks_infer_interval_from_text(text: str) -> float:
    raw = str(text or "").strip().lower()
    if not raw:
        return 0.0

    m = re.search(
        r"\bevery\s+(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks)\b",
        raw,
    )
    if m:
        count = int(m.group(1))
        unit = str(m.group(2) or "").lower()
        if count <= 0:
            return 0.0
        if unit.startswith("second"):
            return float(count)
        if unit.startswith("minute"):
            return float(count * 60)
        if unit.startswith("hour"):
            return float(count * 3600)
        if unit.startswith("day"):
            return float(count * 86400)
        if unit.startswith("week"):
            return float(count * 604800)

    if any(marker in raw for marker in AI_TASKS_DAILY_MARKERS):
        return 86400.0
    if "weekdays" in raw or "weekday" in raw:
        return 86400.0
    if any(marker in raw for marker in AI_TASKS_WEEKLY_MARKERS):
        return 604800.0
    return 0.0


def _ai_tasks_extract_weekdays(text: str) -> List[int]:
    raw = str(text or "").strip().lower()
    if not raw:
        return []
    if "weekdays" in raw or "weekday" in raw:
        return [0, 1, 2, 3, 4]
    if "weekends" in raw or "weekend" in raw:
        return [5, 6]

    out: List[int] = []
    for token, idx in AI_TASKS_WEEKDAY_MAP.items():
        if re.search(rf"\b{re.escape(token)}\b", raw):
            out.append(int(idx))
    if not out:
        return []
    return sorted(set(out))


def _ai_tasks_derive_recurrence(
    *,
    when_txt: Any,
    interval: float,
    next_run_ts: float,
    fallback_text: str = "",
) -> Dict[str, Any]:
    if interval <= 0 or next_run_ts <= 0:
        return {}

    text = str(when_txt or "").strip().lower()
    if not text and fallback_text:
        text = str(fallback_text).strip().lower()

    anchor_local = datetime.fromtimestamp(float(next_run_ts)).astimezone()
    weekdays = _ai_tasks_extract_weekdays(text)
    time_parts = _ai_tasks_extract_time_of_day_parts(text)

    if any(marker in text for marker in AI_TASKS_DAILY_MARKERS) or "weekday" in text or "weekdays" in text:
        recurrence: Dict[str, Any] = {
            "kind": "daily_local_time",
            "hour": int(anchor_local.hour),
            "minute": int(anchor_local.minute),
            "second": int(anchor_local.second),
        }
        if weekdays:
            recurrence["weekdays"] = weekdays
        return recurrence

    if any(marker in text for marker in AI_TASKS_WEEKLY_MARKERS) or weekdays:
        return {
            "kind": "weekly_local_time",
            "hour": int(anchor_local.hour),
            "minute": int(anchor_local.minute),
            "second": int(anchor_local.second),
            "weekdays": weekdays or [int(anchor_local.weekday())],
        }

    if time_parts and abs(float(interval) - 86400.0) <= 1.0:
        return {
            "kind": "daily_local_time",
            "hour": int(anchor_local.hour),
            "minute": int(anchor_local.minute),
            "second": int(anchor_local.second),
        }
    if time_parts and abs(float(interval) - 604800.0) <= 1.0:
        return {
            "kind": "weekly_local_time",
            "hour": int(anchor_local.hour),
            "minute": int(anchor_local.minute),
            "second": int(anchor_local.second),
            "weekdays": [int(anchor_local.weekday())],
        }
    return {}


def _ai_tasks_parse_when(when_ts: Any, when_txt: Any, in_seconds: Any) -> Optional[float]:
    now = time.time()

    if when_ts is not None:
        try:
            return float(when_ts)
        except Exception:
            pass

    if in_seconds is not None:
        try:
            return now + float(in_seconds)
        except Exception:
            pass

    if isinstance(when_txt, str) and when_txt.strip():
        text = when_txt.strip()
        text_lower = text.lower()
        if text.isdigit():
            # Treat long digit strings as epoch timestamps; short values are likely local hour-of-day.
            if len(text) >= 9:
                try:
                    return float(text)
                except Exception:
                    return None
            try:
                hour = int(text)
            except Exception:
                return None
            if 0 <= hour <= 23:
                now_dt = datetime.now().astimezone()
                dt = now_dt.replace(hour=hour, minute=0, second=0, microsecond=0)
                if dt.timestamp() <= now_dt.timestamp():
                    dt = dt + timedelta(days=1)
                return dt.timestamp()
            return None

        try:
            iso_text = text
            if iso_text.endswith("Z"):
                iso_text = iso_text[:-1] + "+00:00"
            dt = datetime.fromisoformat(iso_text)
        except Exception:
            dt = None

        if dt is None:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except Exception:
                    dt = None

        if dt is None:
            parts = _ai_tasks_extract_time_of_day_parts(text_lower)
            if parts:
                hour, minute, second = parts
                now_dt = datetime.now().astimezone()
                dt = now_dt.replace(hour=hour, minute=minute, second=second, microsecond=0)
                if dt.timestamp() <= now_dt.timestamp():
                    dt = dt + timedelta(days=1)

        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
        return dt.timestamp()

    return None


def _ai_tasks_clean_task_prompt(task_text: Any) -> str:
    raw = str(task_text or "").strip()
    if not raw:
        return ""
    compact = re.sub(r"\s+", " ", raw).strip()
    compact = AI_TASKS_LOCAL_TZ_HINT_RE.sub("", compact).strip(" ,.-")
    compact = AI_TASKS_WEATHER_DEFAULT_HINT_RE.sub("", compact).strip(" ,.-")
    if not compact:
        return ""

    cleaned = compact
    for pattern in AI_TASKS_SCHEDULE_PREFIX_PATTERNS:
        match = pattern.match(cleaned)
        if not match:
            continue
        remainder = cleaned[match.end() :].strip(" ,.-")
        if remainder:
            cleaned = remainder
            break
    time_prefix = AI_TASKS_TIME_PREFIX_RE.match(cleaned)
    if time_prefix:
        remainder = cleaned[time_prefix.end() :].strip(" ,.-")
        if remainder:
            cleaned = remainder
    cleaned = re.sub(r"^(?:to\s+)", "", cleaned, flags=re.IGNORECASE).strip()
    return cleaned or compact


def _ai_tasks_default_title(task_prompt: str, recurrence: Dict[str, Any], interval: float) -> str:
    base = _ai_tasks_clean_task_prompt(task_prompt)
    if base:
        base = re.sub(
            r"^(?:please\s+)?(?:hey\s+\w+\s*,?\s*)?(?:can you|could you|would you|will you|do)\s+",
            "",
            base,
            flags=re.IGNORECASE,
        ).strip()
        base = re.sub(r"\s+", " ", base).strip(" .")
    recurrence_kind = str((recurrence or {}).get("kind") or "").strip().lower()
    cadence_prefix = ""
    if recurrence_kind == "daily_local_time":
        cadence_prefix = "Daily"
    elif recurrence_kind == "weekly_local_time":
        cadence_prefix = "Weekly"
    elif float(interval or 0.0) > 0:
        cadence_prefix = "Recurring"

    lowered = base.lower()
    if any(token in lowered for token in ("weather", "forecast", "rain chance", "rain chances")):
        return f"{cadence_prefix} Weather Forecast".strip()

    if base:
        if len(base) > 80:
            base = base[:77].rstrip() + "..."
        return base[0].upper() + base[1:] if len(base) > 1 else base.upper()

    if cadence_prefix:
        return f"{cadence_prefix} AI task"
    return "Scheduled AI task"


def ai_tasks(
    *,
    message: Any = None,
    content: Any = None,
    task_prompt: Any = None,
    title: Any = None,
    platform: Any = None,
    targets: Any = None,
    when_ts: Any = None,
    when: Any = None,
    in_seconds: Any = None,
    every_seconds: Any = None,
    in_minutes: Any = None,
    in_hours: Any = None,
    every_minutes: Any = None,
    every_hours: Any = None,
    priority: Any = None,
    tags: Any = None,
    ttl_sec: Any = None,
    origin: Optional[Dict[str, Any]] = None,
    channel_id: Any = None,
    channel: Any = None,
    guild_id: Any = None,
    room_id: Any = None,
    device_service: Any = None,
    chat_id: Any = None,
) -> Dict[str, Any]:
    text = str(message if message is not None else content or "").strip()
    task_text_raw = str(task_prompt if task_prompt is not None else text or "").strip()
    task_text = _ai_tasks_clean_task_prompt(task_text_raw)
    if not task_text:
        return {"tool": "ai_tasks", "ok": False, "error": "Cannot queue: missing task prompt"}
    if not text:
        text = task_text

    destination = _send_message_normalize_platform(platform)
    origin_map = dict(origin) if isinstance(origin, dict) else {}
    if not destination:
        origin_platform = _send_message_normalize_platform(origin_map.get("platform"))
        if origin_platform in NOTIFY_ALLOWED_PLATFORMS:
            destination = origin_platform
    if destination not in NOTIFY_ALLOWED_PLATFORMS:
        return {
            "tool": "ai_tasks",
            "ok": False,
            "error": "Cannot queue: missing destination platform",
            "supported_platforms": list(NOTIFY_ALLOWED_PLATFORMS),
        }

    target_map = _send_message_coerce_targets(targets)
    for key, value in (
        ("channel_id", channel_id),
        ("channel", channel),
        ("guild_id", guild_id),
        ("room_id", room_id),
        ("device_service", device_service),
        ("chat_id", chat_id),
    ):
        if value is not None and key not in target_map:
            target_map[key] = value

    in_seconds_val = in_seconds
    if in_seconds_val is None and in_minutes is not None:
        try:
            in_seconds_val = float(in_minutes) * 60.0
        except Exception:
            in_seconds_val = None
    if in_seconds_val is None and in_hours is not None:
        try:
            in_seconds_val = float(in_hours) * 3600.0
        except Exception:
            in_seconds_val = None

    every_seconds_val = every_seconds
    if every_seconds_val is None and every_minutes is not None:
        try:
            every_seconds_val = float(every_minutes) * 60.0
        except Exception:
            every_seconds_val = None
    if every_seconds_val is None and every_hours is not None:
        try:
            every_seconds_val = float(every_hours) * 3600.0
        except Exception:
            every_seconds_val = None

    try:
        interval = float(every_seconds_val) if every_seconds_val is not None else 0.0
    except Exception:
        interval = 0.0
    if interval < 0:
        interval = 0.0
    timing_seed_text = str(when if when is not None else "").strip() or task_text_raw
    if interval <= 0:
        interval = _ai_tasks_infer_interval_from_text(timing_seed_text)

    now = time.time()
    next_run = _ai_tasks_parse_when(when_ts, when, in_seconds_val)
    if next_run is None and interval > 0:
        next_run = _ai_tasks_parse_when(None, timing_seed_text, None)
    if next_run is None and interval > 0:
        next_run = now + max(1.0, interval)
    if next_run is None:
        return {"tool": "ai_tasks", "ok": False, "error": "Cannot schedule: missing or invalid time"}
    if next_run < now:
        next_run = now

    normalized_targets = _ai_tasks_normalize_channel_targets(destination, target_map)
    defaults = load_default_targets(destination, redis_client)
    resolved_targets, err = resolve_targets(destination, normalized_targets, origin_map, defaults)
    if err:
        return {"tool": "ai_tasks", "ok": False, "error": str(err)}

    recurrence = _ai_tasks_derive_recurrence(
        when_txt=when,
        interval=float(interval),
        next_run_ts=float(next_run),
        fallback_text=task_text_raw or task_text,
    )
    resolved_title = str(title or "").strip() or _ai_tasks_default_title(task_text, recurrence, float(interval))

    reminder_id = str(uuid.uuid4())
    schedule: Dict[str, Any] = {
        "next_run_ts": float(next_run),
        "interval_sec": float(interval),
        "anchor_ts": float(next_run),
    }
    if recurrence:
        schedule["recurrence"] = recurrence

    reminder = {
        "id": reminder_id,
        "created_at": float(now),
        "platform": destination,
        "title": resolved_title or None,
        "message": task_text,
        "task_prompt": task_text,
        "targets": resolved_targets or {},
        "origin": normalize_origin(origin_map),
        "meta": {
            "priority": priority,
            "tags": tags,
            "ttl_sec": ttl_sec,
            "source_request": text,
        },
        "schedule": schedule,
    }

    redis_client.set(f"{AI_TASKS_KEY_PREFIX}{reminder_id}", json.dumps(reminder))
    redis_client.zadd(AI_TASKS_DUE_ZSET, {reminder_id: float(next_run)})

    human = datetime.fromtimestamp(float(next_run)).strftime("%Y-%m-%d %H:%M:%S")
    recurrence_kind = str((recurrence or {}).get("kind") or "").strip().lower()
    if interval > 0:
        if recurrence_kind == "daily_local_time":
            result_text = f"Recurring AI task scheduled daily (next at {human})."
        elif recurrence_kind == "weekly_local_time":
            result_text = f"Recurring AI task scheduled weekly (next at {human})."
        else:
            result_text = f"Recurring AI task scheduled every {int(interval)}s (next at {human})."
    else:
        result_text = f"AI task scheduled for {human}."

    return {
        "tool": "ai_tasks",
        "ok": True,
        "result": result_text,
        "platform": destination,
        "reminder_id": reminder_id,
        "next_run_ts": float(next_run),
        "interval_sec": float(interval),
        "recurrence": recurrence or None,
        "targets": resolved_targets or {},
    }


def send_message(
    *,
    message: Any = None,
    content: Any = None,
    title: Any = None,
    platform: Any = None,
    targets: Any = None,
    attachments: Any = None,
    artifacts: Any = None,
    media: Any = None,
    files: Any = None,
    priority: Any = None,
    tags: Any = None,
    ttl_sec: Any = None,
    origin: Optional[Dict[str, Any]] = None,
    channel_id: Any = None,
    channel: Any = None,
    guild_id: Any = None,
    room_id: Any = None,
    room_alias: Any = None,
    device_service: Any = None,
    persistent: Any = None,
    api_notification: Any = None,
    chat_id: Any = None,
    blob_key: Any = None,
    file_id: Any = None,
    path: Any = None,
    url: Any = None,
    name: Any = None,
    mimetype: Any = None,
    media_type: Any = None,
    use_latest_media: Any = None,
    use_recent_media: Any = None,
    media_ref: Optional[Dict[str, Any]] = None,
    media_refs: Any = None,
    image_ref: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    text = str(message if message is not None else content or "").strip()
    destination = _send_message_normalize_platform(platform)
    origin_map = dict(origin) if isinstance(origin, dict) else {}

    if not destination and isinstance(origin_map, dict):
        origin_platform = _send_message_normalize_platform(origin_map.get("platform"))
        if origin_platform in NOTIFY_ALLOWED_PLATFORMS:
            destination = origin_platform

    if destination not in NOTIFY_ALLOWED_PLATFORMS:
        error = "Cannot queue: missing destination platform"
        return {
            "tool": "send_message",
            "ok": False,
            "error": error,
            "needs": _send_message_build_needs(error),
            "supported_platforms": list(NOTIFY_ALLOWED_PLATFORMS),
        }

    target_map = _send_message_coerce_targets(targets)
    for key, value in (
        ("channel_id", channel_id),
        ("channel", channel),
        ("guild_id", guild_id),
        ("room_id", room_id),
        ("room_alias", room_alias),
        ("device_service", device_service),
        ("persistent", persistent),
        ("api_notification", api_notification),
        ("chat_id", chat_id),
    ):
        if value is not None and key not in target_map:
            target_map[key] = value

    payload_attachments: List[Dict[str, Any]] = []
    for raw in (attachments, artifacts, media, files):
        payload_attachments.extend(_send_message_clean_attachment_payload(raw))

    single_attachment = _send_message_attachment_from_ref(
        {
            "type": media_type,
            "name": name,
            "mimetype": mimetype,
            "blob_key": blob_key,
            "file_id": file_id,
            "path": path,
            "url": url,
        },
        default_type="file",
    )
    if single_attachment:
        payload_attachments.append(single_attachment)

    image_ref_attachment = _send_message_attachment_from_ref(image_ref, default_type="image")
    if image_ref_attachment:
        payload_attachments.append(image_ref_attachment)

    media_ref_attachment = _send_message_attachment_from_ref(media_ref, default_type="file")
    if media_ref_attachment:
        payload_attachments.append(media_ref_attachment)
    if isinstance(media_refs, list):
        for item in media_refs:
            if not isinstance(item, dict):
                continue
            item_attachment = _send_message_attachment_from_ref(item, default_type="file")
            if item_attachment:
                payload_attachments.append(item_attachment)

    recent_refs_from_origin = origin_map.get("media_refs") if isinstance(origin_map.get("media_refs"), list) else []
    recent_refs: List[Dict[str, Any]] = [
        item
        for item in recent_refs_from_origin
        if isinstance(item, dict) and _send_message_media_ref_is_fresh(item)
    ]
    recent_refs.extend(
        _load_recent_media_refs_for_context(
            platform=destination,
            origin=origin_map,
            limit=max(6, SEND_MESSAGE_AUTO_MEDIA_MAX_ITEMS * 3),
        )
    )

    explicit_use_latest: Optional[bool] = None
    if use_latest_media is not None:
        explicit_use_latest = _send_message_boolish(use_latest_media, False)
    elif use_recent_media is not None:
        explicit_use_latest = _send_message_boolish(use_recent_media, False)

    if not payload_attachments:
        selected_refs = _send_message_select_recent_refs(
            refs=recent_refs,
            text=text,
            explicit_use_latest=explicit_use_latest,
        )
        if (
            not selected_refs
            and explicit_use_latest is not False
            and not text
        ):
            selected_refs = recent_refs[:1]
        for selected_ref in selected_refs:
            selected_attachment = _send_message_attachment_from_ref(selected_ref, default_type="file")
            if selected_attachment:
                payload_attachments.append(selected_attachment)

    if destination == "homeassistant" and "api_notification" not in target_map:
        settings = _send_message_load_settings()
        target_map["api_notification"] = _send_message_boolish(
            settings.get(SEND_MESSAGE_HA_API_DEFAULT_KEY),
            True,
        )

    meta: Dict[str, Any] = {
        "priority": priority,
        "tags": tags,
        "ttl_sec": ttl_sec,
    }

    if not text and payload_attachments:
        text = "Attachment"
    if not text and not payload_attachments:
        error = "Cannot queue: missing message"
        return {
            "tool": "send_message",
            "ok": False,
            "error": error,
            "needs": _send_message_build_needs(error),
            "platform": destination,
        }

    try:
        result_text = dispatch_notification_sync(
            platform=destination,
            title=str(title or "").strip() or None,
            content=text,
            targets=target_map,
            origin=origin_map,
            meta=meta,
            attachments=payload_attachments,
        )
    except Exception as e:
        return {
            "tool": "send_message",
            "ok": False,
            "error": f"Send failed: {e}",
            "platform": destination,
        }

    ok = str(result_text or "").strip().lower().startswith("queued notification for")
    out: Dict[str, Any] = {
        "tool": "send_message",
        "ok": ok,
        "platform": destination,
        "result": str(result_text or "").strip(),
        "targets": target_map,
        "attachment_count": len(payload_attachments),
    }
    if ok and payload_attachments:
        for item in payload_attachments:
            if not isinstance(item, dict):
                continue
            ref = {
                "type": _as_text(item.get("type")).strip().lower() or "file",
                "blob_key": _as_text(item.get("blob_key")).strip() or None,
                "file_id": _as_text(item.get("file_id")).strip() or None,
                "path": _as_text(item.get("path")).strip() or None,
                "url": _as_text(item.get("url")).strip() or None,
                "name": _as_text(item.get("name")).strip() or "attachment.bin",
                "mimetype": _as_text(item.get("mimetype")).strip().lower() or "application/octet-stream",
                "source": "send_message",
                "updated_at": time.time(),
            }
            _save_media_ref_for_context(
                ref=ref,
                platform=destination,
                origin=origin_map,
            )
    if payload_attachments and not notifier_supports_attachments(destination):
        out["attachment_warning"] = (
            f"{destination} notifications currently ignore attachments; text was sent instead."
        )
    if not ok:
        out["error"] = out["result"] or "Send failed."
        needs = _send_message_build_needs(out["error"])
        if needs:
            out["needs"] = needs
    return out


def read_url(
    url: str,
    *,
    max_bytes: int = 200_000,
    timeout_sec: int = 15,
) -> Dict[str, Any]:
    normalized_url = _normalize_url_input(url)
    err = _validate_url(normalized_url)
    if err:
        return {"tool": "read_url", "ok": False, "error": err}
    try:
        req = urllib.request.Request(
            normalized_url,
            headers={"User-Agent": "Tater-AgentLab/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            content_type = resp.headers.get("Content-Type", "") or ""
            final_url = resp.geturl() or normalized_url
            raw = resp.read(max_bytes + 1)
        truncated = len(raw) > max_bytes
        if truncated:
            raw = raw[:max_bytes]
        # Only allow textual content.
        if not (
            content_type.startswith("text/")
            or "json" in content_type
            or "xml" in content_type
            or "yaml" in content_type
            or "yml" in content_type
        ):
            return {
                "tool": "read_url",
                "ok": False,
                "error": f"Non-text content type ({content_type or 'unknown'}). Use download_file instead.",
            }
        try:
            content = raw.decode("utf-8")
        except Exception:
            content = raw.decode("utf-8", errors="replace")
        return {
            "tool": "read_url",
            "ok": True,
            "url": final_url,
            "content_type": content_type,
            "bytes": len(raw),
            "truncated": truncated,
            "content": content,
        }
    except Exception as e:
        return {"tool": "read_url", "ok": False, "error": str(e)}


class _WebpageInspectorParser(HTMLParser):
    def __init__(self, *, base_url: str, max_links: int, max_images: int):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.max_links = max(1, int(max_links))
        self.max_images = max(1, int(max_images))
        self.title = ""
        self.description = ""
        self._in_title = False
        self._suppress_depth = 0
        self._anchor_href = ""
        self._anchor_text_parts: List[str] = []
        self.links: List[Dict[str, Any]] = []
        self.images: List[Dict[str, Any]] = []
        self._text_parts: List[str] = []

    def _attrs(self, attrs) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for k, v in attrs or []:
            key = _as_text(k).strip().lower()
            if not key:
                continue
            out[key] = _as_text(v).strip()
        return out

    def _abs(self, ref: str) -> str:
        raw = _as_text(ref).strip()
        if not raw:
            return ""
        return urllib.parse.urljoin(self.base_url, raw)

    def _push_link(self, href: str, text: str, *, source: str = "a") -> None:
        if len(self.links) >= self.max_links:
            return
        url = self._abs(href)
        if not url:
            return
        self.links.append(
            {
                "url": url,
                "text": " ".join(_as_text(text).split()).strip(),
                "source": source,
            }
        )

    def _push_image(
        self,
        src: str,
        *,
        alt: str = "",
        title: str = "",
        class_name: str = "",
        id_name: str = "",
        source: str = "img",
    ) -> None:
        if len(self.images) >= self.max_images:
            return
        url = self._abs(src)
        if not url:
            return
        self.images.append(
            {
                "url": url,
                "alt": " ".join(_as_text(alt).split()).strip(),
                "title": " ".join(_as_text(title).split()).strip(),
                "class": " ".join(_as_text(class_name).split()).strip(),
                "id": " ".join(_as_text(id_name).split()).strip(),
                "source": source,
            }
        )

    def handle_starttag(self, tag: str, attrs) -> None:
        t = _as_text(tag).strip().lower()
        amap = self._attrs(attrs)
        if t in {"script", "style", "noscript"}:
            self._suppress_depth += 1
            return
        if t == "title":
            self._in_title = True
            return
        if t == "meta":
            meta_key = _as_text(amap.get("name") or amap.get("property")).strip().lower()
            content = _as_text(amap.get("content")).strip()
            if meta_key == "description" and content and not self.description:
                self.description = content
            if meta_key in {"og:image", "twitter:image", "twitter:image:src"} and content:
                self._push_image(content, source="meta")
            return
        if t == "link":
            rel = _as_text(amap.get("rel")).strip().lower()
            href = _as_text(amap.get("href")).strip()
            if not href:
                return
            if "icon" in rel or "apple-touch-icon" in rel:
                self._push_image(href, source="icon")
            elif rel in {"canonical", "alternate"}:
                self._push_link(href, "", source="link")
            return
        if t == "a":
            self._anchor_href = _as_text(amap.get("href")).strip()
            self._anchor_text_parts = []
            return
        if t == "img":
            self._push_image(
                _as_text(amap.get("src")).strip(),
                alt=amap.get("alt", ""),
                title=amap.get("title", ""),
                class_name=amap.get("class", ""),
                id_name=amap.get("id", ""),
                source="img",
            )

    def handle_endtag(self, tag: str) -> None:
        t = _as_text(tag).strip().lower()
        if t in {"script", "style", "noscript"}:
            self._suppress_depth = max(0, self._suppress_depth - 1)
            return
        if t == "title":
            self._in_title = False
            return
        if t == "a":
            if self._anchor_href:
                text = " ".join(" ".join(self._anchor_text_parts).split()).strip()
                self._push_link(self._anchor_href, text, source="a")
            self._anchor_href = ""
            self._anchor_text_parts = []

    def handle_data(self, data: str) -> None:
        text = " ".join(_as_text(data).split()).strip()
        if not text:
            return
        if self._in_title and not self.title:
            self.title = text
        if self._suppress_depth > 0:
            return
        if self._anchor_href:
            self._anchor_text_parts.append(text)
        if len(self._text_parts) < 400:
            self._text_parts.append(text)

    def visible_text(self, *, max_chars: int = 1200) -> str:
        joined = " ".join(self._text_parts).strip()
        if len(joined) <= max_chars:
            return joined
        clipped = joined[:max_chars]
        if " " in clipped[200:]:
            clipped = clipped[: clipped.rfind(" ")]
        return clipped.rstrip(" .,;:") + "..."


def _score_image_candidate(image: Dict[str, Any]) -> int:
    words = " ".join(
        [
            _as_text(image.get("alt")).lower(),
            _as_text(image.get("title")).lower(),
            _as_text(image.get("class")).lower(),
            _as_text(image.get("id")).lower(),
            _as_text(image.get("url")).lower(),
            _as_text(image.get("source")).lower(),
        ]
    )
    score = 0
    if any(token in words for token in ("logo", "brand", "wordmark", "logotype")):
        score += 4
    if any(token in words for token in ("icon", "favicon")):
        score += 2
    if ".svg" in words:
        score += 1
    if any(token in words for token in ("sprite", "blank", "placeholder", "spacer")):
        score -= 3
    return score


def inspect_webpage(
    url: str,
    *,
    max_bytes: int = 300_000,
    timeout_sec: int = 20,
    max_links: int = 20,
    max_images: int = 20,
    platform: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_url = _normalize_url_input(url)
    err = _validate_url(normalized_url)
    if err:
        return {"tool": "inspect_webpage", "ok": False, "error": err}
    try:
        req = urllib.request.Request(
            normalized_url,
            headers={"User-Agent": "Tater-AgentLab/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            content_type = _as_text(resp.headers.get("Content-Type") or "")
            final_url = resp.geturl() or normalized_url
            raw = resp.read(max_bytes + 1)
    except Exception as e:
        return {"tool": "inspect_webpage", "ok": False, "error": str(e)}

    truncated = len(raw) > max_bytes
    if truncated:
        raw = raw[:max_bytes]

    content_type_norm = content_type.split(";", 1)[0].strip().lower()
    if content_type_norm and not (
        content_type_norm.startswith("text/html")
        or content_type_norm.startswith("application/xhtml+xml")
        or content_type_norm.startswith("text/")
    ):
        return {
            "tool": "inspect_webpage",
            "ok": False,
            "url": final_url,
            "content_type": content_type,
            "error": f"Non-HTML content type ({content_type_norm or 'unknown'}).",
        }

    try:
        html = raw.decode("utf-8")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    parser = _WebpageInspectorParser(
        base_url=final_url,
        max_links=max_links,
        max_images=max_images,
    )
    try:
        parser.feed(html)
    except Exception:
        pass
    parser.close()

    unique_links: List[Dict[str, Any]] = []
    seen_links = set()
    for item in parser.links:
        link_url = _as_text(item.get("url")).strip()
        if not link_url or link_url in seen_links:
            continue
        seen_links.add(link_url)
        unique_links.append(item)

    unique_images: List[Dict[str, Any]] = []
    seen_images = set()
    for item in parser.images:
        image_url = _as_text(item.get("url")).strip()
        if not image_url or image_url in seen_images:
            continue
        seen_images.add(image_url)
        scored = dict(item)
        score = _score_image_candidate(scored)
        scored["score"] = score
        scored["logo_hint"] = bool(score >= 2)
        unique_images.append(scored)

    best_image_url = ""
    if unique_images:
        ranked = sorted(
            enumerate(unique_images),
            key=lambda pair: (int(pair[1].get("score") or 0), -pair[0]),
            reverse=True,
        )
        best_image_url = _as_text(ranked[0][1].get("url")).strip()

    media_ref = None
    if best_image_url:
        path_name = Path(urllib.parse.urlparse(best_image_url).path).name or "image.png"
        guessed_mime = _as_text(mimetypes.guess_type(path_name)[0]).strip().lower()
        if not guessed_mime.startswith("image/"):
            guessed_mime = "image/png"
        media_ref = {
            "type": "image",
            "url": best_image_url,
            "name": path_name,
            "mimetype": guessed_mime,
            "source": "inspect_webpage",
            "updated_at": time.time(),
        }
        _save_media_ref_for_context(
            ref=media_ref,
            platform=platform,
            origin=origin,
        )

    return {
        "tool": "inspect_webpage",
        "ok": True,
        "url": final_url,
        "content_type": content_type,
        "bytes": len(raw),
        "truncated": truncated,
        "title": _as_text(parser.title).strip(),
        "description": _as_text(parser.description).strip(),
        "text_preview": parser.visible_text(),
        "links": unique_links,
        "link_count": len(unique_links),
        "images": unique_images,
        "image_count": len(unique_images),
        "best_image_url": best_image_url or None,
        "media_ref": media_ref,
    }


def download_file(
    url: str,
    *,
    filename: Optional[str] = None,
    subdir: Optional[str] = None,
    max_bytes: int = 25_000_000,
    timeout_sec: int = 30,
    platform: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure_dirs()
    normalized_url = _normalize_url_input(url)
    err = _validate_url(normalized_url)
    if err:
        return {"tool": "download_file", "ok": False, "error": err}

    # Resolve target directory inside agent_lab (default: downloads)
    target_dir = AGENT_LAB_DIR / (subdir or "downloads")
    try:
        target_dir = target_dir.resolve()
    except Exception:
        target_dir = AGENT_DOCUMENTS_DIR
    if not (
        target_dir == AGENT_LAB_DIR.resolve()
        or AGENT_LAB_DIR.resolve() in target_dir.parents
    ):
        return {"tool": "download_file", "ok": False, "error": "Target directory not allowed."}
    target_dir.mkdir(parents=True, exist_ok=True)

    parsed = urllib.parse.urlparse(normalized_url)
    default_name = _sanitize_filename(os.path.basename(parsed.path)) or "download.bin"
    safe_name = _sanitize_filename(filename or default_name) or "download.bin"
    dest = target_dir / safe_name

    import hashlib

    hasher = hashlib.sha256()
    size = 0
    content_type = ""
    try:
        req = urllib.request.Request(normalized_url, headers={"User-Agent": "Tater-AgentLab/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            content_type = resp.headers.get("Content-Type", "") or ""
            final_url = resp.geturl() or normalized_url
            length = resp.headers.get("Content-Length")
            if length:
                try:
                    if int(length) > max_bytes:
                        return {
                            "tool": "download_file",
                            "ok": False,
                            "error": f"File exceeds max_bytes ({max_bytes}).",
                        }
                except Exception:
                    pass
            with dest.open("wb") as f:
                while True:
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > max_bytes:
                        f.close()
                        try:
                            dest.unlink()
                        except Exception:
                            pass
                        return {
                            "tool": "download_file",
                            "ok": False,
                            "error": f"File exceeds max_bytes ({max_bytes}).",
                        }
                    f.write(chunk)
                    hasher.update(chunk)
    except Exception as e:
        try:
            if dest.exists():
                dest.unlink()
        except Exception:
            pass
        return {"tool": "download_file", "ok": False, "error": str(e)}

    _log_write("download_file", dest, size)
    out: Dict[str, Any] = {
        "tool": "download_file",
        "ok": True,
        "url": final_url,
        "path": str(dest),
        "bytes": size,
        "sha256": hasher.hexdigest(),
        "content_type": content_type,
    }

    media_type, detected_mime = _download_file_detect_media(dest, content_type)
    media_ref = {
        "type": media_type,
        "path": str(dest),
        "name": dest.name,
        "mimetype": detected_mime,
        "source": "download_file",
        "updated_at": time.time(),
        "size": size,
    }
    out["media_ref"] = media_ref
    _save_media_ref_for_context(
        ref=media_ref,
        platform=platform,
        origin=origin,
    )

    return out


def read_file(path: str, start: int = 0, max_chars: Optional[int] = None) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR, STABLE_PLUGINS_DIR, STABLE_PLATFORMS_DIR, SKILLS_DIR]
    resolved = _resolve_safe_path(path, allowed)
    if not resolved:
        return {"tool": "read_file", "ok": False, "error": "Path not allowed."}
    if not resolved.exists() or not resolved.is_file():
        return {"tool": "read_file", "ok": False, "error": "File not found."}
    try:
        full_content, metadata = _extract_file_content(resolved)
        chunk, window = _slice_content(full_content, start=start, max_chars=max_chars)
        source_truncated = bool(metadata.get("source_truncated"))
        return {
            "tool": "read_file",
            "ok": True,
            "path": str(resolved),
            "content": chunk,
            "source_truncated": source_truncated,
            "truncated": bool(source_truncated or window.get("has_more")),
            **window,
            **metadata,
        }
    except Exception as e:
        return {"tool": "read_file", "ok": False, "error": str(e)}


def _is_hidden_path(path: Path) -> bool:
    for part in path.parts:
        if part.startswith(".") and part not in {".", ".."}:
            return True
    return False


def _find_query_hits(
    content: str,
    query: str,
    *,
    case_sensitive: bool = False,
    max_hits: int = 50,
) -> List[Dict[str, Any]]:
    if not query:
        return []
    needle = query if case_sensitive else query.lower()
    hits: List[Dict[str, Any]] = []
    for idx, line in enumerate(content.splitlines(), start=1):
        hay = line if case_sensitive else line.lower()
        if needle in hay:
            snippet = line.strip()
            if len(snippet) > 320:
                snippet = snippet[:320].rstrip() + "..."
            hits.append({"line": idx, "snippet": snippet})
            if len(hits) >= max_hits:
                break
    return hits


def search_files(
    query: str,
    *,
    path: Optional[str] = None,
    max_results: int = _SEARCH_DEFAULT_MAX_RESULTS,
    case_sensitive: bool = False,
    include_hidden: bool = False,
    file_glob: Optional[str] = None,
) -> Dict[str, Any]:
    _ensure_dirs()
    needle = str(query or "").strip()
    if not needle:
        return {"tool": "search_files", "ok": False, "error": "query is required."}

    max_results_i = _coerce_int(max_results, default=_SEARCH_DEFAULT_MAX_RESULTS, min_value=1, max_value=2000)
    allow_roots = [AGENT_LAB_DIR, STABLE_PLUGINS_DIR, STABLE_PLATFORMS_DIR, SKILLS_DIR]

    try:
        targets: List[Path] = []
        if path and str(path).strip():
            resolved = _resolve_safe_path(str(path), allow_roots)
            if not resolved:
                return {"tool": "search_files", "ok": False, "error": "Path not allowed."}
            if not resolved.exists():
                return {"tool": "search_files", "ok": False, "error": "Path not found."}
            targets = [resolved]
        else:
            targets = [AGENT_DOCUMENTS_DIR, AGENT_WORKSPACE_DIR]

        scanned_files = 0
        skipped_files = 0
        matched_files = 0
        results: List[Dict[str, Any]] = []
        seen: set[str] = set()
        per_file_hit_limit = 5

        def _iter_files(base: Path) -> List[Path]:
            if base.is_file():
                return [base]
            out: List[Path] = []
            for p in base.rglob("*"):
                if p.is_file():
                    out.append(p)
            return out

        for target in targets:
            files = _iter_files(target)
            for file_path in files:
                file_key = str(file_path.resolve())
                if file_key in seen:
                    continue
                seen.add(file_key)
                if not include_hidden and _is_hidden_path(file_path):
                    continue
                if file_glob and not fnmatch.fnmatch(file_path.name, str(file_glob)):
                    continue

                scanned_files += 1
                try:
                    text, _meta = _extract_file_content(file_path)
                except Exception:
                    skipped_files += 1
                    continue
                if len(text) > _SEARCH_MAX_FILE_CHARS:
                    text = text[:_SEARCH_MAX_FILE_CHARS]

                hits = _find_query_hits(
                    text,
                    needle,
                    case_sensitive=case_sensitive,
                    max_hits=per_file_hit_limit,
                )
                if not hits:
                    continue

                matched_files += 1
                for hit in hits:
                    results.append(
                        {
                            "path": str(file_path),
                            "line": hit["line"],
                            "snippet": hit["snippet"],
                        }
                    )
                    if len(results) >= max_results_i:
                        break
                if len(results) >= max_results_i:
                    break
            if len(results) >= max_results_i:
                break

        return {
            "tool": "search_files",
            "ok": True,
            "query": needle,
            "results": results,
            "count": len(results),
            "scanned_files": scanned_files,
            "matched_files": matched_files,
            "skipped_files": skipped_files,
            "paths": [str(p) for p in targets],
            "max_results": max_results_i,
        }
    except Exception as e:
        return {"tool": "search_files", "ok": False, "error": str(e)}


def write_file(
    path: str,
    content: Optional[str] = None,
    *,
    content_b64: Optional[str] = None,
    content_lines: Optional[List[str]] = None,
) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR]
    resolved = _resolve_safe_path(path, allowed)
    if not resolved:
        return {"tool": "write_file", "ok": False, "error": "Path not allowed."}
    try:
        # Prevent creating executable modules via write_file.
        if resolved.suffix == ".py":
            try:
                rel = resolved.relative_to(AGENT_LAB_DIR)
                top = rel.parts[0] if rel.parts else ""
            except Exception:
                top = ""
            if top in {"plugins", "platforms"}:
                return {
                    "tool": "write_file",
                    "ok": False,
                    "error": "Use create_plugin/create_platform for Agent Lab plugins/platforms.",
                }
            return {
                "tool": "write_file",
                "ok": False,
                "error": "Python files are not allowed via write_file. Use create_plugin/create_platform.",
            }
        resolved.parent.mkdir(parents=True, exist_ok=True)
        if content_b64:
            try:
                import base64
                data = base64.b64decode(content_b64.encode("utf-8")).decode("utf-8")
            except Exception as e:
                return {"tool": "write_file", "ok": False, "error": f"Invalid content_b64: {e}"}
        elif isinstance(content_lines, list):
            data = "\n".join(str(x) for x in content_lines)
        else:
            data = content if content is not None else ""
        resolved.write_text(data, encoding="utf-8")
        _log_write("write_file", resolved, len(data.encode("utf-8")))
        return {"tool": "write_file", "ok": True, "path": str(resolved), "bytes": len(data)}
    except Exception as e:
        return {"tool": "write_file", "ok": False, "error": str(e)}


def list_directory(path: str) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR, STABLE_PLUGINS_DIR, STABLE_PLATFORMS_DIR]
    resolved = _resolve_safe_path(path, allowed)
    if not resolved:
        return {"tool": "list_directory", "ok": False, "error": "Path not allowed."}
    if not resolved.exists() or not resolved.is_dir():
        return {"tool": "list_directory", "ok": False, "error": "Directory not found."}
    try:
        files = []
        dirs = []
        for item in sorted(resolved.iterdir()):
            if item.is_dir():
                dirs.append(item.name)
            else:
                files.append(item.name)
        return {"tool": "list_directory", "ok": True, "path": str(resolved), "files": files, "directories": dirs}
    except Exception as e:
        return {"tool": "list_directory", "ok": False, "error": str(e)}


def _safe_archive_target(base_dir: Path, member_name: str) -> Optional[Path]:
    raw = str(member_name or "").strip().replace("\\", "/")
    if not raw:
        return None
    while raw.startswith("/"):
        raw = raw[1:]
    normalized = os.path.normpath(raw).replace("\\", "/")
    if normalized in {"", ".", ".."} or normalized.startswith("../"):
        return None
    first = normalized.split("/", 1)[0]
    if ":" in first:
        return None
    try:
        target = (base_dir / normalized).resolve()
        base = base_dir.resolve()
    except Exception:
        return None
    if target == base or base in target.parents:
        return target
    return None


def list_archive(path: str, max_entries: int = _ARCHIVE_LIST_MAX_ENTRIES) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR]
    archive_path = _resolve_safe_path(path, allowed)
    if not archive_path:
        return {"tool": "list_archive", "ok": False, "error": "Path not allowed."}
    if not archive_path.exists() or not archive_path.is_file():
        return {"tool": "list_archive", "ok": False, "error": "Archive not found."}

    max_entries_i = _coerce_int(max_entries, default=_ARCHIVE_LIST_MAX_ENTRIES, min_value=1, max_value=5000)
    entries: List[Dict[str, Any]] = []
    truncated = False
    lowered_name = archive_path.name.lower()

    try:
        if lowered_name.endswith(".7z"):
            try:
                import py7zr
            except Exception as e:
                return {"tool": "list_archive", "ok": False, "error": "7z support requires the `py7zr` package."}
            with py7zr.SevenZipFile(str(archive_path), mode="r") as zf:
                for info in zf.list():
                    if len(entries) >= max_entries_i:
                        truncated = True
                        break
                    name = str(getattr(info, "filename", "") or "")
                    is_dir = bool(getattr(info, "is_directory", False)) or name.endswith("/")
                    entries.append(
                        {
                            "name": name,
                            "size": int(getattr(info, "uncompressed", 0) or 0),
                            "compressed_size": int(getattr(info, "compressed", 0) or 0),
                            "is_dir": is_dir,
                        }
                    )
            return {
                "tool": "list_archive",
                "ok": True,
                "path": str(archive_path),
                "format": "7z",
                "entries": entries,
                "count": len(entries),
                "truncated": truncated,
            }

        if lowered_name.endswith(".rar"):
            try:
                import rarfile
            except Exception as e:
                return {"tool": "list_archive", "ok": False, "error": "RAR support requires the `rarfile` package."}
            with rarfile.RarFile(str(archive_path), "r") as rf:
                for info in rf.infolist():
                    if len(entries) >= max_entries_i:
                        truncated = True
                        break
                    entries.append(
                        {
                            "name": info.filename,
                            "size": int(getattr(info, "file_size", 0) or 0),
                            "compressed_size": int(getattr(info, "compress_size", 0) or 0),
                            "is_dir": bool(info.isdir()),
                            "is_symlink": bool(getattr(info, "is_symlink", lambda: False)()),
                        }
                    )
            return {
                "tool": "list_archive",
                "ok": True,
                "path": str(archive_path),
                "format": "rar",
                "entries": entries,
                "count": len(entries),
                "truncated": truncated,
            }

        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path, "r") as zf:
                for info in zf.infolist():
                    if len(entries) >= max_entries_i:
                        truncated = True
                        break
                    entries.append(
                        {
                            "name": info.filename,
                            "size": int(info.file_size),
                            "compressed_size": int(info.compress_size),
                            "is_dir": bool(info.is_dir()),
                        }
                    )
            return {
                "tool": "list_archive",
                "ok": True,
                "path": str(archive_path),
                "format": "zip",
                "entries": entries,
                "count": len(entries),
                "truncated": truncated,
            }

        if tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path, "r:*") as tf:
                for member in tf.getmembers():
                    if len(entries) >= max_entries_i:
                        truncated = True
                        break
                    entries.append(
                        {
                            "name": member.name,
                            "size": int(member.size or 0),
                            "is_dir": bool(member.isdir()),
                            "is_symlink": bool(member.issym() or member.islnk()),
                        }
                    )
            return {
                "tool": "list_archive",
                "ok": True,
                "path": str(archive_path),
                "format": "tar",
                "entries": entries,
                "count": len(entries),
                "truncated": truncated,
            }

        return {
            "tool": "list_archive",
            "ok": False,
            "error": "Unsupported archive format. Supported: zip, tar(.gz/.bz2/.xz), 7z, rar.",
        }
    except Exception as e:
        return {"tool": "list_archive", "ok": False, "error": str(e)}


def _archive_output_folder_name(path: Path) -> str:
    name = path.name
    lowered = name.lower()
    suffixes = [".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz", ".zip", ".tar", ".7z", ".rar"]
    for suffix in suffixes:
        if lowered.endswith(suffix):
            base = name[: -len(suffix)]
            return base or path.stem or "archive"
    return path.stem or "archive"


def extract_archive(
    path: str,
    *,
    destination: Optional[str] = None,
    overwrite: bool = False,
    max_files: int = _ARCHIVE_EXTRACT_MAX_FILES,
    max_total_bytes: int = _ARCHIVE_EXTRACT_MAX_TOTAL_BYTES,
) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR]
    archive_path = _resolve_safe_path(path, allowed)
    if not archive_path:
        return {"tool": "extract_archive", "ok": False, "error": "Path not allowed."}
    if not archive_path.exists() or not archive_path.is_file():
        return {"tool": "extract_archive", "ok": False, "error": "Archive not found."}

    default_dest = f"workspace/extracted_{_archive_output_folder_name(archive_path)}"
    auto_destination = destination in (None, "")
    dest_path = _resolve_safe_path(destination or default_dest, allowed)
    if not dest_path:
        return {"tool": "extract_archive", "ok": False, "error": "Destination path not allowed."}
    if dest_path.exists() and not dest_path.is_dir():
        return {"tool": "extract_archive", "ok": False, "error": "Destination exists and is not a directory."}
    dest_existed_before = dest_path.exists()
    if not dest_existed_before:
        dest_path.mkdir(parents=True, exist_ok=True)

    max_files_i = _coerce_int(max_files, default=_ARCHIVE_EXTRACT_MAX_FILES, min_value=1, max_value=20000)
    max_bytes_i = _coerce_int(
        max_total_bytes,
        default=_ARCHIVE_EXTRACT_MAX_TOTAL_BYTES,
        min_value=1,
        max_value=2_000_000_000,
    )

    extracted: List[str] = []
    skipped: List[Dict[str, str]] = []
    extracted_count = 0
    total_bytes = 0
    limit_hit = False
    lowered_name = archive_path.name.lower()

    def _record_skip(name: str, reason: str) -> None:
        skipped.append({"name": name, "reason": reason})

    try:
        if lowered_name.endswith(".7z"):
            try:
                import py7zr
            except Exception as e:
                return {"tool": "extract_archive", "ok": False, "error": "7z support requires the `py7zr` package."}
            with py7zr.SevenZipFile(str(archive_path), mode="r") as zf:
                selected_names: List[str] = []
                selected_targets: List[Path] = []
                selected_sizes: List[int] = []
                for info in zf.list():
                    name = str(getattr(info, "filename", "") or "")
                    is_dir = bool(getattr(info, "is_directory", False)) or name.endswith("/")
                    if is_dir:
                        continue
                    target = _safe_archive_target(dest_path, name)
                    if not target:
                        _record_skip(name, "unsafe_path")
                        continue
                    size = int(getattr(info, "uncompressed", 0) or 0)
                    if size < 0:
                        _record_skip(name, "invalid_size")
                        continue
                    if extracted_count >= max_files_i:
                        _record_skip(name, "max_files_reached")
                        limit_hit = True
                        break
                    if total_bytes + size > max_bytes_i:
                        _record_skip(name, "max_total_bytes_reached")
                        limit_hit = True
                        break
                    if target.exists() and not overwrite:
                        _record_skip(name, "exists")
                        continue
                    selected_names.append(name)
                    selected_targets.append(target)
                    selected_sizes.append(size)
                    extracted_count += 1
                    total_bytes += size
                    extracted.append(str(target))
                if selected_names:
                    zf.extract(path=str(dest_path), targets=selected_names)
                    for target, size in zip(selected_targets, selected_sizes):
                        _log_write("extract_archive", target, size)

            return {
                "tool": "extract_archive",
                "ok": True,
                "path": str(archive_path),
                "format": "7z",
                "destination": str(dest_path),
                "extracted_count": extracted_count,
                "extracted": extracted,
                "skipped_count": len(skipped),
                "skipped": skipped,
                "bytes_written": total_bytes,
                "limit_hit": limit_hit,
            }

        if lowered_name.endswith(".rar"):
            try:
                import rarfile
            except Exception as e:
                return {"tool": "extract_archive", "ok": False, "error": "RAR support requires the `rarfile` package."}
            with rarfile.RarFile(str(archive_path), "r") as rf:
                for info in rf.infolist():
                    name = info.filename
                    if info.isdir():
                        continue
                    is_symlink = bool(getattr(info, "is_symlink", lambda: False)())
                    if is_symlink:
                        _record_skip(name, "symlink_not_allowed")
                        continue
                    target = _safe_archive_target(dest_path, name)
                    if not target:
                        _record_skip(name, "unsafe_path")
                        continue
                    size = int(getattr(info, "file_size", 0) or 0)
                    if size < 0:
                        _record_skip(name, "invalid_size")
                        continue
                    if extracted_count >= max_files_i:
                        _record_skip(name, "max_files_reached")
                        limit_hit = True
                        break
                    if total_bytes + size > max_bytes_i:
                        _record_skip(name, "max_total_bytes_reached")
                        limit_hit = True
                        break
                    if target.exists() and not overwrite:
                        _record_skip(name, "exists")
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with rf.open(info) as src, target.open("wb") as dst:
                        shutil.copyfileobj(src, dst)
                    total_bytes += size
                    extracted_count += 1
                    extracted.append(str(target))
                    _log_write("extract_archive", target, size)

            return {
                "tool": "extract_archive",
                "ok": True,
                "path": str(archive_path),
                "format": "rar",
                "destination": str(dest_path),
                "extracted_count": extracted_count,
                "extracted": extracted,
                "skipped_count": len(skipped),
                "skipped": skipped,
                "bytes_written": total_bytes,
                "limit_hit": limit_hit,
            }

        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path, "r") as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    name = info.filename
                    target = _safe_archive_target(dest_path, name)
                    if not target:
                        _record_skip(name, "unsafe_path")
                        continue
                    if info.file_size < 0:
                        _record_skip(name, "invalid_size")
                        continue
                    if extracted_count >= max_files_i:
                        _record_skip(name, "max_files_reached")
                        limit_hit = True
                        break
                    if total_bytes + int(info.file_size) > max_bytes_i:
                        _record_skip(name, "max_total_bytes_reached")
                        limit_hit = True
                        break
                    if target.exists() and not overwrite:
                        _record_skip(name, "exists")
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(info, "r") as src, target.open("wb") as dst:
                        shutil.copyfileobj(src, dst)
                    total_bytes += int(info.file_size)
                    extracted_count += 1
                    extracted.append(str(target))
                    _log_write("extract_archive", target, int(info.file_size))

            return {
                "tool": "extract_archive",
                "ok": True,
                "path": str(archive_path),
                "format": "zip",
                "destination": str(dest_path),
                "extracted_count": extracted_count,
                "extracted": extracted,
                "skipped_count": len(skipped),
                "skipped": skipped,
                "bytes_written": total_bytes,
                "limit_hit": limit_hit,
            }

        if tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path, "r:*") as tf:
                for member in tf.getmembers():
                    name = member.name
                    if member.isdir():
                        continue
                    if member.issym() or member.islnk():
                        _record_skip(name, "symlink_not_allowed")
                        continue
                    target = _safe_archive_target(dest_path, name)
                    if not target:
                        _record_skip(name, "unsafe_path")
                        continue
                    size = int(member.size or 0)
                    if extracted_count >= max_files_i:
                        _record_skip(name, "max_files_reached")
                        limit_hit = True
                        break
                    if total_bytes + size > max_bytes_i:
                        _record_skip(name, "max_total_bytes_reached")
                        limit_hit = True
                        break
                    if target.exists() and not overwrite:
                        _record_skip(name, "exists")
                        continue
                    stream = tf.extractfile(member)
                    if stream is None:
                        _record_skip(name, "unreadable_entry")
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with stream as src, target.open("wb") as dst:
                        shutil.copyfileobj(src, dst)
                    total_bytes += size
                    extracted_count += 1
                    extracted.append(str(target))
                    _log_write("extract_archive", target, size)

            return {
                "tool": "extract_archive",
                "ok": True,
                "path": str(archive_path),
                "format": "tar",
                "destination": str(dest_path),
                "extracted_count": extracted_count,
                "extracted": extracted,
                "skipped_count": len(skipped),
                "skipped": skipped,
                "bytes_written": total_bytes,
                "limit_hit": limit_hit,
            }

        return {
            "tool": "extract_archive",
            "ok": False,
            "error": "Unsupported archive format. Supported: zip, tar(.gz/.bz2/.xz), 7z, rar.",
        }
    except Exception as e:
        return {"tool": "extract_archive", "ok": False, "error": str(e)}
    finally:
        # Clean up auto-created default extraction dirs when nothing was written.
        if auto_destination and not dest_existed_before:
            try:
                if (
                    dest_path.exists()
                    and dest_path.is_dir()
                    and dest_path != AGENT_WORKSPACE_DIR
                    and next(dest_path.iterdir(), None) is None
                ):
                    dest_path.rmdir()
            except Exception:
                pass


def delete_file(path: str) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR]
    resolved = _resolve_safe_path(path, allowed)
    if not resolved:
        return {"tool": "delete_file", "ok": False, "error": "Path not allowed."}
    if not resolved.exists() or not resolved.is_file():
        return {"tool": "delete_file", "ok": False, "error": "File not found."}
    try:
        resolved.unlink()
        _log_write("delete_file", resolved, 0)
        return {"tool": "delete_file", "ok": True, "path": str(resolved), "deleted": True}
    except Exception as e:
        return {"tool": "delete_file", "ok": False, "error": str(e)}


def _requirements_path() -> Path:
    _ensure_dirs()
    return AGENT_REQUIREMENTS


def _read_requirements() -> List[str]:
    path = _requirements_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception:
        return []
    out = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


def _write_requirements(lines: List[str]) -> None:
    path = _requirements_path()
    uniq = []
    seen = set()
    for line in lines:
        if not line or line.startswith("#"):
            continue
        if line not in seen:
            seen.add(line)
            uniq.append(line)
    path.write_text("\n".join(sorted(uniq)) + ("\n" if uniq else ""), encoding="utf-8")


def _normalize_dependency(dep: str) -> str:
    return str(dep or "").strip()


def _dependency_import_name(dep: str) -> str:
    dep = _normalize_dependency(dep)
    if not dep:
        return ""
    # strip extras and version specifiers
    name = re.split(r"[<>=!~]", dep, maxsplit=1)[0]
    name = name.split("[", 1)[0]
    return name.strip()


def _extract_declared_dependencies(path: Path) -> List[str]:
    try:
        source = path.read_text(encoding="utf-8")
    except Exception:
        return []
    try:
        tree = ast.parse(source, filename=str(path))
    except Exception:
        return []
    deps = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"dependencies", "DEPENDENCIES", "requirements"}:
                    if isinstance(node.value, (ast.List, ast.Tuple)):
                        for item in node.value.elts:
                            if isinstance(item, ast.Constant) and isinstance(item.value, str):
                                deps.append(item.value.strip())
    return [d for d in deps if d]


def _validate_platform_source(source: str) -> Tuple[bool, str]:
    if not source or not str(source).strip():
        return False, "Missing code for platform."
    try:
        tree = ast.parse(source)
    except Exception as e:
        return False, f"Syntax error: {e}"

    has_platform = False
    has_run = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "PLATFORM":
                    has_platform = True
        if isinstance(node, ast.FunctionDef) and node.name == "run":
            has_run = True

    if not has_platform:
        return False, "Missing PLATFORM dict."
    if not has_run:
        return False, "Missing run() function."
    return True, ""


def _validate_plugin_source(source: str) -> Tuple[bool, str]:
    if not source or not str(source).strip():
        return False, "Missing code for plugin."
    try:
        tree = ast.parse(source)
    except Exception as e:
        return False, f"Syntax error: {e}"

    has_plugin_assignment = False
    plugin_is_dict = False
    has_toolplugin_class = False
    has_toolplugin_import = False

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == "plugin_base":
                for alias in node.names:
                    if alias.name == "ToolPlugin":
                        has_toolplugin_import = True
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "plugin":
                    has_plugin_assignment = True
                    if isinstance(node.value, ast.Dict):
                        plugin_is_dict = True
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                if isinstance(base, ast.Name) and base.id == "ToolPlugin":
                    has_toolplugin_class = True
                elif isinstance(base, ast.Attribute) and base.attr == "ToolPlugin":
                    has_toolplugin_class = True

    if not has_plugin_assignment:
        return False, "Missing module-level `plugin` instance."
    if plugin_is_dict:
        return False, "`plugin` must be a ToolPlugin instance (not a dict)."
    if not has_toolplugin_class:
        return False, "Missing ToolPlugin subclass."
    if not has_toolplugin_import:
        return False, "Import ToolPlugin from plugin_base."
    action_failure_issues = _action_failure_call_issues(tree)
    if action_failure_issues:
        return False, "; ".join(action_failure_issues)
    return True, ""


def _action_failure_call_issues(tree: ast.AST) -> List[str]:
    issues: List[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func_name = ""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr
        if func_name != "action_failure":
            continue

        kw_names = {kw.arg for kw in node.keywords if kw.arg}
        if node.args:
            issues.append("action_failure must use keyword args only (`code` and `message`).")
        if "fail_text" in kw_names:
            issues.append("action_failure does not accept `fail_text`; use `code` and `message`.")
        if "code" not in kw_names or "message" not in kw_names:
            issues.append("action_failure requires `code` and `message` keyword arguments.")

    # Keep output deterministic and compact.
    deduped: List[str] = []
    for item in issues:
        if item not in deduped:
            deduped.append(item)
    return deduped


def _waiting_prompt_style_issues(template: str) -> List[str]:
    text = str(template or "").strip()
    if not text:
        return ["missing waiting_prompt_template text"]

    lowered = text.lower()
    issues: List[str] = []

    if "{mention}" not in text:
        issues.append("must include {mention}")

    has_wait_tone = any(
        token in lowered
        for token in (
            "wait",
            "working on",
            "working",
            "creating",
            "processing",
            "loading",
            "one moment",
            "hang tight",
            "be right back",
            "right now",
            "in progress",
        )
    )
    if not has_wait_tone:
        issues.append("must describe progress/please-wait status")

    has_message_constraint = any(
        phrase in lowered
        for phrase in (
            "only output that message",
            "output only that message",
            "return only that message",
            "only output the message",
            "return only the message",
        )
    )
    if not has_message_constraint:
        issues.append("must constrain output to only that message")

    if any(
        phrase in lowered
        for phrase in (
            "only output the joke",
            "only output the summary",
            "only output the answer",
            "tell me a random joke",
        )
    ):
        issues.append("must be a wait/status message, not the final task output")

    return issues


def _missing_dependencies(deps: List[str]) -> List[str]:
    missing = []
    for dep in deps:
        name = _dependency_import_name(dep)
        if not name:
            continue
        try:
            __import__(name)
        except Exception:
            missing.append(dep)
    return missing


def _log_dependency(action: str, detail: str) -> None:
    try:
        _ensure_dirs()
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} | {action} | {detail}\n"
        with (AGENT_LOGS_DIR / "agent_dependencies.log").open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        return


def _install_dependencies(deps: List[str]) -> Tuple[List[str], List[str]]:
    installed = []
    errors = []
    for dep in deps:
        dep = _normalize_dependency(dep)
        if not dep:
            continue
        if not _SAFE_DEP_RE.fullmatch(dep):
            errors.append(f"{dep} (invalid dependency spec)")
            continue
        try:
            _log_dependency("install_start", dep)
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", dep],
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode == 0:
                installed.append(dep)
                _log_dependency("install_ok", dep)
            else:
                err = (result.stderr or result.stdout or "").strip()
                errors.append(f"{dep} ({err[:200]})")
                _log_dependency("install_fail", f"{dep} | {err[:200]}")
        except Exception as e:
            errors.append(f"{dep} ({e})")
            _log_dependency("install_exception", f"{dep} | {e}")
    return installed, errors


def _update_requirements_union(deps: List[str]) -> None:
    if not deps:
        return
    current = _read_requirements()
    merged = current[:]
    for dep in deps:
        dep = _normalize_dependency(dep)
        if dep and dep not in merged:
            merged.append(dep)
    _write_requirements(merged)


def _store_validation(kind: str, name: str, report: Dict[str, Any]) -> None:
    try:
        key = f"exp:validation:{kind}:{name}"
        redis_client.set(key, json.dumps(report, ensure_ascii=False))
    except Exception:
        return


def list_stable_plugins() -> Dict[str, Any]:
    try:
        from plugin_registry import get_registry_snapshot
        registry = get_registry_snapshot()
    except Exception:
        registry = {}
    items = []
    for pid, plugin in sorted(registry.items(), key=lambda kv: kv[0].lower()):
        version = (
            getattr(plugin, "version", None)
            or getattr(plugin, "__version__", None)
            or getattr(plugin, "plugin_version", None)
            or "0.0.0"
        )
        platforms = getattr(plugin, "platforms", []) or []
        items.append(
            {
                "id": pid,
                "version": str(version),
                "platforms": platforms,
                "enabled": bool(get_plugin_enabled(pid)),
            }
        )
    return {"tool": "list_stable_plugins", "ok": True, "plugins": items, "count": len(items)}


def list_stable_platforms() -> Dict[str, Any]:
    from platform_registry import platform_registry
    items = []
    for entry in platform_registry:
        key = entry.get("key")
        if not key:
            continue
        running = str(redis_client.get(f"{key}_running") or "").strip().lower() == "true"
        items.append({"key": key, "running": running})
    return {"tool": "list_stable_platforms", "ok": True, "platforms": items, "count": len(items)}


def inspect_plugin(plugin_id: str) -> Dict[str, Any]:
    try:
        from plugin_registry import get_registry_snapshot
        registry = get_registry_snapshot()
    except Exception:
        registry = {}
    plugin = registry.get(plugin_id)
    if not plugin:
        return {"tool": "inspect_plugin", "ok": False, "error": f"Plugin '{plugin_id}' not found."}
    return {
        "tool": "inspect_plugin",
        "ok": True,
        "id": plugin_id,
        "platforms": getattr(plugin, "platforms", []) or [],
        "description": (
            getattr(plugin, "description", None)
            or getattr(plugin, "plugin_dec", None)
            or ""
        ),
        "required_settings": getattr(plugin, "required_settings", None) or {},
    }


def _exp_plugin_path(name: str) -> Path:
    return AGENT_PLUGINS_DIR / f"{name}.py"


def _exp_platform_path(name: str) -> Path:
    return AGENT_PLATFORMS_DIR / f"{name}.py"


def _import_from_path(path: Path) -> Optional[Any]:
    try:
        module_name = f"tater_exp_{path.stem}_{int(path.stat().st_mtime_ns)}"
        spec = importlib.util.spec_from_file_location(module_name, str(path))
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return module
    except Exception:
        return None


def validate_plugin(name: str, auto_install: bool = True) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        report = {"tool": "validate_plugin", "ok": False, "error": "Invalid plugin name."}
        _store_validation("plugin", name, report)
        return report
    path = _exp_plugin_path(name)
    if not path.exists():
        report = {"tool": "validate_plugin", "ok": False, "error": "Plugin file not found."}
        _store_validation("plugin", name, report)
        return report

    # Syntax check
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except Exception as e:
        report = {
            "tool": "validate_plugin",
            "ok": False,
            "error": f"Syntax error: {e}",
            "path": str(path),
        }
        _store_validation("plugin", name, report)
        return report

    action_failure_issues = _action_failure_call_issues(tree)
    if action_failure_issues:
        report = {
            "tool": "validate_plugin",
            "ok": False,
            "error": "; ".join(action_failure_issues),
            "path": str(path),
            "missing_fields": ["action_failure_signature"],
            "warnings": action_failure_issues,
        }
        _store_validation("plugin", name, report)
        return report

    declared_deps = _extract_declared_dependencies(path)
    _update_requirements_union(declared_deps)
    missing_deps = _missing_dependencies(declared_deps)
    installed_deps: List[str] = []
    install_errors: List[str] = []
    if missing_deps and auto_install:
        installed_deps, install_errors = _install_dependencies(missing_deps)
        missing_deps = _missing_dependencies(declared_deps)

    module = _import_from_path(path)
    if not module:
        report = {
            "tool": "validate_plugin",
            "ok": False,
            "error": "Import failed.",
            "path": str(path),
            "missing_dependencies": missing_deps,
            "installed_dependencies": installed_deps,
            "install_errors": install_errors,
        }
        _store_validation("plugin", name, report)
        return report

    plugin = getattr(module, "plugin", None)
    missing = []
    warnings = []
    if not plugin:
        missing.append("plugin")
    else:
        if not isinstance(plugin, ToolPlugin):
            missing.append("plugin")
        for field in ("name", "version", "platforms", "description"):
            value = getattr(plugin, field, None)
            if value is None or (isinstance(value, str) and not value.strip()):
                missing.append(field)
            if field == "platforms" and (not isinstance(value, list) or not value):
                missing.append(field)
        usage_val = getattr(plugin, "usage", None)
        if not isinstance(usage_val, str) or not usage_val.strip():
            missing.append("usage")
        declared_name = getattr(plugin, "name", None)
        if not isinstance(declared_name, str) or not declared_name.strip():
            missing.append("name")
        else:
            declared_name = declared_name.strip()
            if not _SAFE_NAME_RE.fullmatch(declared_name):
                missing.append("name")
                warnings.append(
                    "name must use only letters, numbers, underscore, or hyphen."
                )
            if declared_name != name:
                missing.append("name")
                warnings.append(
                    f"name must match filename id '{name}' to load reliably in Agent Lab."
                )
        explicit_wait_prompt = None
        try:
            if "waiting_prompt_template" in getattr(plugin, "__dict__", {}):
                explicit_wait_prompt = plugin.__dict__.get("waiting_prompt_template")
            elif "waiting_prompt_template" in getattr(plugin.__class__, "__dict__", {}):
                explicit_wait_prompt = plugin.__class__.__dict__.get("waiting_prompt_template")
        except Exception:
            explicit_wait_prompt = None

        if not isinstance(explicit_wait_prompt, str) or not explicit_wait_prompt.strip():
            missing.append("waiting_prompt_template")
        else:
            issues = _waiting_prompt_style_issues(explicit_wait_prompt)
            if issues:
                missing.append("waiting_prompt_template")
                warnings.append(
                    "waiting_prompt_template must be a friendly progress/wait message for {mention} "
                    "and end with an only-that-message output constraint. "
                    "Issues: " + "; ".join(issues) + "."
                )
        # Validate platform ids
        try:
            from plugin_kernel import KNOWN_PLATFORMS, expand_plugin_platforms
            platforms = getattr(plugin, "platforms", []) or []
            invalid = []
            for p in platforms:
                if str(p).strip().lower() == "both":
                    continue
                if str(p).strip().lower() not in KNOWN_PLATFORMS:
                    invalid.append(p)
            if invalid:
                missing.append("platforms")
        except Exception:
            pass

    ok = not missing and not missing_deps
    report = {
        "tool": "validate_plugin",
        "ok": ok,
        "name": name,
        "path": str(path),
        "missing_fields": sorted(set(missing)),
        "declared_dependencies": declared_deps,
        "missing_dependencies": missing_deps,
        "installed_dependencies": installed_deps,
        "install_errors": install_errors,
        "warnings": warnings,
    }
    if ok:
        report["plugin_name"] = getattr(plugin, "plugin_name", None) or getattr(plugin, "name", name)
        report["version"] = getattr(plugin, "version", "")
        report["platforms"] = getattr(plugin, "platforms", []) or []
    _store_validation("plugin", name, report)
    return report


def test_plugin(name: str, platform: Optional[str] = None, auto_install: bool = False) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        return {"tool": "test_plugin", "ok": False, "error": "Invalid plugin name."}

    agent_path = _exp_plugin_path(name)
    stable_path = STABLE_PLUGINS_DIR / f"{name}.py"

    source_kind = ""
    path: Optional[Path] = None
    validation: Optional[Dict[str, Any]] = None
    if agent_path.exists():
        source_kind = "agent_lab"
        path = agent_path
        validation = validate_plugin(name, auto_install=auto_install)
        if not validation.get("ok"):
            return {
                "tool": "test_plugin",
                "ok": False,
                "name": name,
                "source": source_kind,
                "path": str(path),
                "static_tested": True,
                "live_tested": False,
                "error": "Validation failed.",
                "validation": validation,
                "summary": "Static test failed: plugin validation did not pass.",
            }
    elif stable_path.exists():
        source_kind = "stable"
        path = stable_path
    else:
        return {"tool": "test_plugin", "ok": False, "error": "Plugin file not found."}

    module = _import_from_path(path)
    if not module:
        return {
            "tool": "test_plugin",
            "ok": False,
            "name": name,
            "source": source_kind,
            "path": str(path),
            "static_tested": True,
            "live_tested": False,
            "error": "Import failed.",
            "summary": "Static test failed: plugin import failed.",
        }

    plugin = getattr(module, "plugin", None)
    if not isinstance(plugin, ToolPlugin):
        return {
            "tool": "test_plugin",
            "ok": False,
            "name": name,
            "source": source_kind,
            "path": str(path),
            "static_tested": True,
            "live_tested": False,
            "error": "Missing module-level ToolPlugin instance `plugin`.",
            "summary": "Static test failed: plugin instance is missing or invalid.",
        }

    try:
        from plugin_kernel import expand_plugin_platforms

        supported_platforms = expand_plugin_platforms(getattr(plugin, "platforms", []) or [])
    except Exception:
        supported_platforms = list(getattr(plugin, "platforms", []) or [])

    requested_platform = str(platform or "").strip().lower()
    if not requested_platform:
        requested_platform = supported_platforms[0] if supported_platforms else ""

    if not requested_platform:
        return {
            "tool": "test_plugin",
            "ok": False,
            "name": name,
            "source": source_kind,
            "path": str(path),
            "static_tested": True,
            "live_tested": False,
            "error": "Plugin does not declare any supported platforms.",
            "summary": "Static test failed: plugin has no supported platform handlers to test.",
        }

    handler_name = f"handle_{requested_platform}"
    class_dict = getattr(plugin.__class__, "__dict__", {})
    handler_obj = getattr(plugin, handler_name, None)
    handler_present = bool(handler_name in class_dict and callable(handler_obj))

    usage_raw = getattr(plugin, "usage", None)
    usage_parse_ok = False
    usage_function = None
    usage_matches_name = False
    usage_error = ""
    if not isinstance(usage_raw, str) or not usage_raw.strip():
        usage_error = "usage is missing or empty."
    else:
        try:
            usage_obj = json.loads(usage_raw)
            if not isinstance(usage_obj, dict):
                usage_error = "usage must parse to a JSON object."
            else:
                usage_function = _as_text(usage_obj.get("function")).strip()
                usage_args = usage_obj.get("arguments")
                usage_parse_ok = bool(usage_function and isinstance(usage_args, dict))
                usage_matches_name = usage_function == _as_text(getattr(plugin, "name", name)).strip()
                if not usage_parse_ok:
                    usage_error = "usage must include function and arguments object."
                elif not usage_matches_name:
                    usage_error = "usage.function must match plugin name."
        except Exception as e:
            usage_error = f"usage JSON parse failed: {e}"

    errors: List[str] = []
    if requested_platform not in supported_platforms:
        errors.append(f"requested platform '{requested_platform}' is not in plugin platforms.")
    if not handler_present:
        errors.append(f"missing callable handler `{handler_name}`.")
    if not usage_parse_ok or not usage_matches_name:
        errors.append(usage_error or "usage metadata check failed.")

    ok = not errors
    summary = (
        f"Static test passed for `{name}` on `{requested_platform}`. "
        "Live platform execution was not run."
        if ok
        else f"Static test failed for `{name}` on `{requested_platform}`: " + "; ".join(errors)
    )

    return {
        "tool": "test_plugin",
        "ok": ok,
        "name": name,
        "source": source_kind,
        "path": str(path),
        "platform_tested": requested_platform,
        "supported_platforms": supported_platforms,
        "handler_name": handler_name,
        "handler_present": handler_present,
        "usage_parse_ok": usage_parse_ok,
        "usage_function": usage_function,
        "usage_matches_name": usage_matches_name,
        "usage_error": usage_error,
        "static_tested": True,
        "live_tested": False,
        "limitations": [
            "This runs static readiness checks only (import/metadata/handler).",
            "It does not execute the live platform event loop or send real platform messages.",
        ],
        "errors": errors,
        "validation": validation,
        "summary": summary,
    }


def create_plugin(
    name: str,
    code: Optional[str] = None,
    *,
    code_lines: Optional[List[str]] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        return {"tool": "create_plugin", "ok": False, "error": "Invalid plugin name."}
    path = _exp_plugin_path(name)
    if path.exists() and not _coerce_bool(overwrite, default=False):
        return {
            "tool": "create_plugin",
            "ok": False,
            "error": f"Plugin '{name}' already exists.",
            "error_code": "already_exists",
            "name": name,
            "path": str(path),
            "overwrite_required": True,
            "needs": [f"Plugin `{name}` already exists. Overwrite it? (yes/no)"],
        }
    try:
        if isinstance(code_lines, list):
            for idx, line in enumerate(code_lines):
                if isinstance(line, str) and ("\n" in line or "\r" in line):
                    return {
                        "tool": "create_plugin",
                        "ok": False,
                        "error": "code_lines entries must be single-line strings (no embedded newlines).",
                    }
            payload = "\n".join(str(x) for x in code_lines)
        else:
            payload = code or ""

        ok, err = _validate_plugin_source(payload)
        if not ok:
            return {"tool": "create_plugin", "ok": False, "error": err}

        path.write_text(payload, encoding="utf-8")
        _log_write("create_plugin", path, len(payload.encode("utf-8")))
    except Exception as e:
        return {"tool": "create_plugin", "ok": False, "error": str(e)}

    report = validate_plugin(name)
    report["tool"] = "create_plugin"
    return report


def promote_plugin(name: str, confirm: Optional[bool] = None, delete_source: bool = False) -> Dict[str, Any]:
    _ensure_dirs()
    if not confirm:
        return {
            "tool": "promote_plugin",
            "ok": False,
            "error": "Confirmation required.",
            "needs": ["Please confirm promotion to stable plugins by setting confirm=true."],
        }
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        return {"tool": "promote_plugin", "ok": False, "error": "Invalid plugin name."}
    src = _exp_plugin_path(name)
    if not src.exists():
        return {"tool": "promote_plugin", "ok": False, "error": "Agent Lab plugin not found."}
    dest = STABLE_PLUGINS_DIR / f"{name}.py"
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        reload_plugins()
        _log_write("promote_plugin", dest, dest.stat().st_size if dest.exists() else 0)
        if delete_source:
            src.unlink()
        return {"tool": "promote_plugin", "ok": True, "path": str(dest)}
    except Exception as e:
        return {"tool": "promote_plugin", "ok": False, "error": str(e)}


def validate_platform(name: str, auto_install: bool = True) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        report = {"tool": "validate_platform", "ok": False, "error": "Invalid platform name."}
        _store_validation("platform", name, report)
        return report
    path = _exp_platform_path(name)
    if not path.exists():
        report = {"tool": "validate_platform", "ok": False, "error": "Platform file not found."}
        _store_validation("platform", name, report)
        return report

    try:
        source = path.read_text(encoding="utf-8")
        ast.parse(source, filename=str(path))
    except Exception as e:
        report = {
            "tool": "validate_platform",
            "ok": False,
            "error": f"Syntax error: {e}",
            "path": str(path),
        }
        _store_validation("platform", name, report)
        return report

    declared_deps = _extract_declared_dependencies(path)
    _update_requirements_union(declared_deps)
    missing_deps = _missing_dependencies(declared_deps)
    installed_deps: List[str] = []
    install_errors: List[str] = []
    if missing_deps and auto_install:
        installed_deps, install_errors = _install_dependencies(missing_deps)
        missing_deps = _missing_dependencies(declared_deps)

    module = _import_from_path(path)
    if not module:
        report = {
            "tool": "validate_platform",
            "ok": False,
            "error": "Import failed.",
            "path": str(path),
            "missing_dependencies": missing_deps,
            "installed_dependencies": installed_deps,
            "install_errors": install_errors,
        }
        _store_validation("platform", name, report)
        return report

    platform_dict = getattr(module, "PLATFORM", None)
    run_fn = getattr(module, "run", None)
    missing = []
    if not isinstance(platform_dict, dict):
        missing.append("PLATFORM")
    if not callable(run_fn):
        missing.append("run")

    ok = not missing and not missing_deps
    report = {
        "tool": "validate_platform",
        "ok": ok,
        "name": name,
        "path": str(path),
        "missing_fields": missing,
        "declared_dependencies": declared_deps,
        "missing_dependencies": missing_deps,
        "installed_dependencies": installed_deps,
        "install_errors": install_errors,
    }
    _store_validation("platform", name, report)
    return report


def create_platform(
    name: str,
    code: Optional[str] = None,
    *,
    code_lines: Optional[List[str]] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        return {"tool": "create_platform", "ok": False, "error": "Invalid platform name."}
    path = _exp_platform_path(name)
    if path.exists() and not _coerce_bool(overwrite, default=False):
        return {
            "tool": "create_platform",
            "ok": False,
            "error": f"Platform '{name}' already exists.",
            "error_code": "already_exists",
            "name": name,
            "path": str(path),
            "overwrite_required": True,
            "needs": [f"Platform `{name}` already exists. Overwrite it? (yes/no)"],
        }
    try:
        if isinstance(code_lines, list):
            for idx, line in enumerate(code_lines):
                if isinstance(line, str) and ("\n" in line or "\r" in line):
                    return {
                        "tool": "create_platform",
                        "ok": False,
                        "error": "code_lines entries must be single-line strings (no embedded newlines).",
                    }
            payload = "\n".join(str(x) for x in code_lines)
        else:
            payload = code or ""

        ok, err = _validate_platform_source(payload)
        if not ok:
            return {"tool": "create_platform", "ok": False, "error": err}

        path.write_text(payload, encoding="utf-8")
        _log_write("create_platform", path, len(payload.encode("utf-8")))
    except Exception as e:
        return {"tool": "create_platform", "ok": False, "error": str(e)}

    report = validate_platform(name)
    report["tool"] = "create_platform"
    return report


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    return str(value)


def _origin_value(origin: Optional[Dict[str, Any]], *keys: str) -> str:
    if not isinstance(origin, dict):
        return ""
    for key in keys:
        val = origin.get(key)
        if val in (None, ""):
            continue
        text = _as_text(val).strip()
        if text:
            return text
    return ""


def _get_blob_redis_client():
    global _VISION_BLOB_REDIS
    if _VISION_BLOB_REDIS is None:
        host = os.getenv("REDIS_HOST", "127.0.0.1")
        port = _coerce_int(os.getenv("REDIS_PORT", "6379"), default=6379, min_value=1, max_value=65535)
        _VISION_BLOB_REDIS = redis.Redis(host=host, port=port, db=0, decode_responses=False)
    return _VISION_BLOB_REDIS


def _vision_decode_base64(data: str) -> Optional[bytes]:
    text = _as_text(data).strip()
    if not text:
        return None
    if text.startswith("data:") and "," in text:
        text = text.split(",", 1)[1]
    padding = len(text) % 4
    if padding:
        text += "=" * (4 - padding)
    try:
        decoded = base64.b64decode(text)
    except Exception:
        return None
    return bytes(decoded) if decoded else None


def _vision_mimetype_allowed(value: Any) -> bool:
    mimetype = _as_text(value).strip().lower()
    if not mimetype:
        return False
    return mimetype in _VISION_ALLOWED_MIMETYPES


def _vision_ext_from_mime(value: Any) -> str:
    mimetype = _as_text(value).strip().lower()
    if mimetype in {"image/jpeg", "image/jpg"}:
        return "jpg"
    if mimetype == "image/webp":
        return "webp"
    if mimetype == "image/gif":
        return "gif"
    if mimetype == "image/bmp":
        return "bmp"
    if mimetype == "image/tiff":
        return "tiff"
    return "png"


def _vision_to_data_url(image_bytes: bytes, filename: str) -> str:
    mime = mimetypes.guess_type(filename or "")[0] or "image/png"
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    return f"data:{mime};base64,{b64}"


def _vision_normalize_name(name: Any, mimetype: Any) -> str:
    cleaned = _as_text(name).strip()
    if cleaned:
        return cleaned
    if mimetype:
        return f"image.{_vision_ext_from_mime(mimetype)}"
    return "image.png"


def _vision_blob_candidates(*, blob_key: Any, file_id: Any) -> List[str]:
    candidates: List[str] = []
    blob = _as_text(blob_key).strip()
    if blob:
        candidates.append(blob)
    fid = _as_text(file_id).strip()
    if fid:
        if fid.startswith(_VISION_WEBUI_BLOB_PREFIX) or fid.startswith("tater:blob:") or fid.startswith("tater:matrix:"):
            candidates.append(fid)
        else:
            candidates.append(f"{_VISION_WEBUI_BLOB_PREFIX}{fid}")
            candidates.append(fid)
    unique: List[str] = []
    seen = set()
    for key in candidates:
        if key and key not in seen:
            unique.append(key)
            seen.add(key)
    return unique


def _vision_load_blob_bytes(*, blob_key: Any = None, file_id: Any = None) -> Optional[bytes]:
    keys = _vision_blob_candidates(blob_key=blob_key, file_id=file_id)
    if not keys:
        return None
    client = _get_blob_redis_client()
    for key in keys:
        try:
            raw = client.get(key)
        except Exception:
            raw = None
        if raw is None:
            continue
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw)
        if isinstance(raw, str):
            return raw.encode("utf-8", errors="replace")
    return None


def _vision_extract_image_from_payload(payload: Any) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    if payload is None:
        return None, None, None

    if isinstance(payload, dict) and payload.get("marker") == "plugin_response":
        return _vision_extract_image_from_payload(payload.get("content"))

    if isinstance(payload, list):
        for item in payload:
            image_bytes, name, mimetype = _vision_extract_image_from_payload(item)
            if image_bytes:
                return image_bytes, name, mimetype
        return None, None, None

    if not isinstance(payload, dict):
        return None, None, None

    media_type = _as_text(payload.get("type")).strip().lower()
    mimetype = _as_text(payload.get("mimetype")).strip().lower()
    if not mimetype:
        guess_name = _as_text(payload.get("name")).strip()
        mimetype = _as_text(mimetypes.guess_type(guess_name)[0]).strip().lower()

    if media_type in {"image", "file"}:
        if media_type == "file" and (not mimetype or not mimetype.startswith("image/")):
            return None, None, None
        if mimetype and not _vision_mimetype_allowed(mimetype):
            return None, None, None

        name = _vision_normalize_name(payload.get("name"), mimetype)
        raw: Optional[bytes] = None

        if isinstance(payload.get("bytes"), (bytes, bytearray)):
            raw = bytes(payload["bytes"])
        elif isinstance(payload.get("data"), (bytes, bytearray)):
            raw = bytes(payload["data"])
        elif isinstance(payload.get("data"), str):
            raw = _vision_decode_base64(payload.get("data") or "")

        if raw is None:
            raw = _vision_load_blob_bytes(blob_key=payload.get("blob_key"), file_id=payload.get("id"))

        if raw:
            return raw, name, mimetype or _as_text(mimetypes.guess_type(name)[0]).strip().lower() or "image/png"
        return None, None, None

    if payload.get("blob_key") or payload.get("id"):
        raw = _vision_load_blob_bytes(blob_key=payload.get("blob_key"), file_id=payload.get("id"))
        if raw:
            name = _vision_normalize_name(payload.get("name"), mimetype)
            mm = mimetype or _as_text(mimetypes.guess_type(name)[0]).strip().lower() or "image/png"
            if mm.startswith("image/"):
                return raw, name, mm
    return None, None, None


def _vision_read_local_image(path: Any) -> Tuple[Optional[bytes], Optional[str], Optional[str], Optional[str]]:
    raw_path = _as_text(path).strip()
    if not raw_path:
        return None, None, None, "Local image path is empty."

    allowed_roots = [
        BASE_DIR,
        AGENT_LAB_DIR,
        AGENT_DOWNLOADS_DIR,
        AGENT_WORKSPACE_DIR,
    ]
    resolved = _resolve_safe_path(raw_path, allowed_roots)
    if resolved is None:
        return None, None, None, "Path is outside allowed workspace roots."
    if not resolved.exists() or not resolved.is_file():
        return None, None, None, f"Local image path does not exist: {resolved}"

    try:
        data = resolved.read_bytes()
    except Exception as e:
        return None, None, None, f"Failed to read local image: {e}"
    if not data:
        return None, None, None, "Local image file is empty."
    if len(data) > _VISION_MAX_IMAGE_BYTES:
        return (
            None,
            None,
            None,
            f"Local image is too large ({len(data)} bytes). Max supported size is {_VISION_MAX_IMAGE_BYTES} bytes.",
        )

    name = resolved.name or "image.png"
    mimetype = _as_text(mimetypes.guess_type(name)[0]).strip().lower() or "image/png"
    return data, name, mimetype, None


def _vision_read_remote_image(url: Any) -> Tuple[Optional[bytes], Optional[str], Optional[str], Optional[str]]:
    raw_url = _normalize_url_input(url)
    if not raw_url:
        return None, None, None, "Image URL is empty."
    blocked = _validate_url(raw_url)
    if blocked:
        return None, None, None, blocked
    try:
        req = urllib.request.Request(raw_url, headers={"User-Agent": "Tater/1.0"})
        with urllib.request.urlopen(req, timeout=30) as response:
            content_type = _as_text(response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            data = response.read(_VISION_MAX_IMAGE_BYTES + 1)
            final_url = response.geturl() or raw_url
    except urllib.error.HTTPError as e:
        return None, None, None, f"Image URL request failed with HTTP {e.code}."
    except Exception as e:
        return None, None, None, f"Image URL request failed: {e}"

    if not data:
        return None, None, None, "Image URL returned no data."
    if len(data) > _VISION_MAX_IMAGE_BYTES:
        return (
            None,
            None,
            None,
            f"Image URL payload is too large ({len(data)} bytes). Max supported size is {_VISION_MAX_IMAGE_BYTES} bytes.",
        )

    parsed = urllib.parse.urlparse(final_url)
    guessed_name = Path(parsed.path).name if parsed.path else ""
    if not guessed_name:
        guessed_name = "image.png"
    guessed_mime = _as_text(mimetypes.guess_type(guessed_name)[0]).strip().lower()
    mimetype = content_type or guessed_mime or "image/png"
    if content_type and not content_type.startswith("image/"):
        return None, None, None, "URL did not return an image content type. Use inspect_webpage first."
    if not content_type and (not guessed_mime or not guessed_mime.startswith("image/")):
        return None, None, None, "URL does not look like a direct image. Use inspect_webpage first."
    if mimetype and mimetype.startswith("image/") and not _vision_mimetype_allowed(mimetype):
        return None, None, None, f"Unsupported image MIME type from URL: {mimetype}"
    return data, guessed_name, mimetype, None


def _vision_load_explicit_ref(
    *,
    path: Any,
    url: Any,
    blob_key: Any,
    file_id: Any,
    media_ref: Optional[Dict[str, Any]],
    media_refs: Optional[List[Dict[str, Any]]],
    image_ref: Optional[Dict[str, Any]],
    platform: Optional[str],
    origin: Optional[Dict[str, Any]],
    request_text: str = "",
) -> Tuple[Optional[bytes], Optional[str], Optional[str], Optional[str], Optional[str]]:
    def _from_ref(ref: Dict[str, Any], source_name: str):
        ref_type = _as_text(ref.get("type")).strip().lower()
        ref_path = _as_text(ref.get("path")).strip()
        ref_url = _as_text(ref.get("url")).strip()
        ref_blob = _as_text(ref.get("blob_key")).strip()
        ref_file = _as_text(ref.get("file_id")).strip()
        ref_name = _as_text(ref.get("name")).strip()
        ref_mimetype = _as_text(ref.get("mimetype")).strip().lower()

        if ref_type and ref_type != "image" and not ref_mimetype.startswith("image/"):
            return None, None, None, source_name, "Reference is not an image."

        if ref_path:
            data, name, mimetype, err = _vision_read_local_image(ref_path)
            if err:
                return None, None, None, source_name, err
            return data, (ref_name or name), (ref_mimetype or mimetype), source_name, None
        if ref_url:
            data, name, mimetype, err = _vision_read_remote_image(ref_url)
            if err:
                return None, None, None, source_name, err
            return data, (ref_name or name), (ref_mimetype or mimetype), source_name, None
        if ref_blob or ref_file:
            data = _vision_load_blob_bytes(blob_key=ref_blob, file_id=ref_file)
            if not data:
                return None, None, None, source_name, "Image blob could not be loaded from Redis."
            name = ref_name or _vision_normalize_name("", ref_mimetype)
            mimetype = ref_mimetype or _as_text(mimetypes.guess_type(name)[0]).strip().lower() or "image/png"
            if mimetype and mimetype.startswith("image/") and not _vision_mimetype_allowed(mimetype):
                return None, None, None, source_name, f"Unsupported image MIME type: {mimetype}"
            return data, name, mimetype, source_name, None
        return None, None, None, source_name, "Image reference is missing path/url/blob_key/file_id."

    explicit_path = _as_text(path).strip()
    if explicit_path:
        data, name, mimetype, err = _vision_read_local_image(explicit_path)
        return data, name, mimetype, "path", err

    explicit_url = _as_text(url).strip()
    if explicit_url:
        data, name, mimetype, err = _vision_read_remote_image(explicit_url)
        return data, name, mimetype, "url", err

    explicit_blob = _as_text(blob_key).strip()
    explicit_file_id = _as_text(file_id).strip()
    if explicit_blob or explicit_file_id:
        data = _vision_load_blob_bytes(blob_key=explicit_blob, file_id=explicit_file_id)
        if not data:
            return None, None, None, "blob", "Image blob could not be loaded from Redis."
        name = _vision_normalize_name("", "")
        mimetype = "image/png"
        return data, name, mimetype, "blob", None

    if isinstance(image_ref, dict):
        data, name, mimetype, source_name, err = _from_ref(image_ref, "image_ref")
        if data or err:
            return data, name, mimetype, source_name, err

    if isinstance(media_ref, dict):
        data, name, mimetype, source_name, err = _from_ref(media_ref, "media_ref")
        if data:
            return data, name, mimetype, source_name, None

    request_lower = _as_text(request_text).strip().lower()
    preferred_index = 0
    if re.search(r"\b(3|third|3rd)\b", request_lower):
        preferred_index = 2
    elif re.search(r"\b(2|second|2nd)\b", request_lower):
        preferred_index = 1

    candidate_refs: List[Tuple[str, Dict[str, Any]]] = []

    if isinstance(media_refs, list):
        for idx, item in enumerate(media_refs):
            if isinstance(item, dict):
                candidate_refs.append((f"media_refs[{idx}]", item))

    if isinstance(origin, dict):
        for idx, item in enumerate(origin.get("media_refs") or []):
            if isinstance(item, dict):
                candidate_refs.append((f"origin.media_refs[{idx}]", item))

    recent_refs = _load_recent_media_refs_for_context(
        platform=platform,
        origin=origin,
        limit=8,
        media_types=["image"],
    )
    for idx, item in enumerate(recent_refs):
        if isinstance(item, dict):
            candidate_refs.append((f"recent_media_refs[{idx}]", item))

    image_candidates: List[Tuple[str, Dict[str, Any]]] = []
    for source_name, ref in candidate_refs:
        ref_type = _as_text(ref.get("type")).strip().lower()
        ref_mimetype = _as_text(ref.get("mimetype")).strip().lower()
        if ref_type == "image" or ref_mimetype.startswith("image/"):
            image_candidates.append((source_name, ref))

    if image_candidates:
        chosen_idx = min(max(preferred_index, 0), len(image_candidates) - 1)
        chosen_source, chosen_ref = image_candidates[chosen_idx]
        data, name, mimetype, source_name, err = _from_ref(chosen_ref, chosen_source)
        if data or err:
            return data, name, mimetype, source_name, err

    return None, None, None, None, None


def _vision_extract_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: List[str] = []
        for item in content:
            if isinstance(item, str) and item.strip():
                chunks.append(item.strip())
            elif isinstance(item, dict):
                if item.get("type") == "text" and _as_text(item.get("text")).strip():
                    chunks.append(_as_text(item.get("text")).strip())
                elif _as_text(item.get("content")).strip():
                    chunks.append(_as_text(item.get("content")).strip())
        return "\n".join(chunks).strip()
    if isinstance(content, dict):
        for key in ("text", "content", "value"):
            value = content.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return _as_text(content).strip()


def _vision_call_openai_compatible(
    *,
    api_base: str,
    model: str,
    api_key: Optional[str],
    image_bytes: bytes,
    filename: str,
    prompt: str,
) -> Tuple[Optional[str], Optional[str]]:
    url = f"{_as_text(api_base).strip().rstrip('/')}/v1/chat/completions"
    payload = {
        "model": _as_text(model).strip(),
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": _as_text(prompt).strip() or _VISION_DEFAULT_PROMPT},
                    {"type": "image_url", "image_url": {"url": _vision_to_data_url(image_bytes, filename)}},
                ],
            }
        ],
        "temperature": 0.2,
    }
    headers = {"Content-Type": "application/json"}
    if _as_text(api_key).strip():
        headers["Authorization"] = f"Bearer {_as_text(api_key).strip()}"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=90) as response:
            raw = response.read(2_000_000)
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = _as_text(e.read(400)).strip()
        except Exception:
            detail = ""
        if detail:
            return None, f"Vision API request failed with HTTP {e.code}: {detail}"
        return None, f"Vision API request failed with HTTP {e.code}."
    except Exception as e:
        return None, f"Vision API request failed: {e}"
    try:
        parsed = json.loads(_as_text(raw))
    except Exception:
        return None, "Vision API returned non-JSON output."
    choices = parsed.get("choices")
    if not isinstance(choices, list) or not choices:
        return None, "Vision API response did not include choices."
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    text = _vision_extract_text(content)
    if not text:
        return None, "Vision API returned an empty description."
    return text, None


def vision_describer(
    prompt: str = "",
    path: Optional[str] = None,
    url: Optional[str] = None,
    blob_key: Optional[str] = None,
    file_id: Optional[str] = None,
    media_ref: Optional[Dict[str, Any]] = None,
    media_refs: Optional[List[Dict[str, Any]]] = None,
    image_ref: Optional[Dict[str, Any]] = None,
    history_key: Optional[str] = None,
    platform: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    image_bytes, filename, mimetype, source_name, resolve_err = _vision_load_explicit_ref(
        path=path,
        url=url,
        blob_key=blob_key,
        file_id=file_id,
        media_ref=media_ref,
        media_refs=media_refs,
        image_ref=image_ref,
        platform=platform,
        origin=origin,
        request_text=(
            _as_text(origin.get("request_text"))
            if isinstance(origin, dict) and _as_text(origin.get("request_text")).strip()
            else _as_text(prompt)
        ),
    )
    if resolve_err:
        return {
            "tool": "vision_describer",
            "ok": False,
            "error": resolve_err,
            "needs": [
                "Provide one explicit image source: `path`, `url`, `blob_key`, `file_id`, `media_ref`, `image_ref`, or attach/upload an image in this chat."
            ],
        }
    if not image_bytes:
        return {
            "tool": "vision_describer",
            "ok": False,
            "error": "No image source was provided.",
            "needs": [
                "Provide one explicit image source: `path`, `url`, `blob_key`, `file_id`, `media_ref`, `image_ref`, or attach/upload an image in this chat."
            ],
        }

    if len(image_bytes) > _VISION_MAX_IMAGE_BYTES:
        return {
            "tool": "vision_describer",
            "ok": False,
            "error": (
                f"Latest image is too large ({len(image_bytes)} bytes). "
                f"Max supported size is {_VISION_MAX_IMAGE_BYTES} bytes."
            ),
            "filename": filename or "image.png",
        }

    settings = get_shared_vision_settings()
    api_base = _as_text(settings.get("api_base")).strip().rstrip("/")
    model = _as_text(settings.get("model")).strip()
    api_key = _as_text(settings.get("api_key")).strip()
    if not api_base or not model:
        return {
            "tool": "vision_describer",
            "ok": False,
            "error": "Vision settings are incomplete. Configure Vision API base and model in Settings.",
            "needs": ["Set Vision API base URL and model in Settings."],
        }

    final_prompt = _as_text(prompt).strip() or _VISION_DEFAULT_PROMPT
    image_name = _vision_normalize_name(filename, mimetype)
    description, err = _vision_call_openai_compatible(
        api_base=api_base,
        model=model,
        api_key=api_key,
        image_bytes=image_bytes,
        filename=image_name,
        prompt=final_prompt,
    )
    if err:
        return {
            "tool": "vision_describer",
            "ok": False,
            "error": err,
            "filename": image_name,
            "model": model,
            "source": source_name or "unknown",
        }

    return {
        "tool": "vision_describer",
        "ok": True,
        "description": description or "",
        "text": description or "",
        "filename": image_name,
        "mimetype": mimetype or _as_text(mimetypes.guess_type(image_name)[0]).strip() or "image/png",
        "model": model,
        "source": source_name or "unknown",
        "history_key_ignored": _as_text(history_key).strip() or None,
    }


def _normalize_key_segment(value: Any, *, default: str) -> str:
    raw = _as_text(value).strip().lower()
    if not raw:
        raw = default
    cleaned = re.sub(r"[^a-z0-9_.:\-]+", "_", raw).strip("_")
    return cleaned or default


def _memory_scope_target(
    *,
    scope: Optional[str],
    user_id: Optional[str],
    room_id: Optional[str],
    platform: Optional[str],
    origin: Optional[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    scope_name = _as_text(scope or "global").strip().lower() or "global"
    if scope_name not in {"global", "user", "room"}:
        return None, "scope must be one of: global, user, room."

    platform_name = _normalize_key_segment(
        platform or _origin_value(origin, "platform"),
        default="webui",
    )
    if scope_name == "global":
        return {
            "scope": "global",
            "platform": None,
            "user_id": None,
            "room_id": None,
            "redis_key": f"{MEMORY_HASH_PREFIX}:global",
        }, None

    if scope_name == "user":
        uid_raw = _as_text(
            user_id
            or _origin_value(origin, "user_id", "user", "username", "sender")
        ).strip()
        if not uid_raw:
            return None, "user_id is required for scope='user'."
        uid = _normalize_key_segment(uid_raw, default="")
        if not uid:
            return None, "user_id is invalid."
        return {
            "scope": "user",
            "platform": None,
            "user_id": uid_raw,
            "room_id": None,
            "redis_key": f"{MEMORY_HASH_PREFIX}:user:{uid}",
        }, None

    room_raw = _as_text(
        room_id
        or _origin_value(origin, "room_id", "room", "channel_id", "channel", "chat_id", "scope")
        or "chat"
    ).strip()
    room_name = _normalize_key_segment(room_raw, default="chat")
    return {
        "scope": "room",
        "platform": platform_name,
        "user_id": None,
        "room_id": room_raw,
        "redis_key": f"{MEMORY_HASH_PREFIX}:room:{platform_name}:{room_name}",
    }, None


def _memory_explicit_only_enabled() -> bool:
    raw = redis_client.get(MEMORY_EXPLICIT_ONLY_REDIS_KEY)
    if raw in (None, ""):
        return True
    return _coerce_bool(raw, default=True)


def _memory_default_ttl() -> int:
    raw = redis_client.get(MEMORY_DEFAULT_TTL_REDIS_KEY)
    return _coerce_int(raw, default=0, min_value=0, max_value=31_536_000)


def _memory_has_explicit_intent(text: Any) -> bool:
    lowered = _as_text(text).strip().lower()
    if not lowered:
        return False
    return any(phrase in lowered for phrase in MEMORY_EXPLICIT_PHRASES)


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except Exception:
        return _as_text(value)


def _memory_parse_entry(raw: Any) -> Dict[str, Any]:
    text = _as_text(raw)
    if not text:
        return {"value": "", "updated_at": 0.0, "expires_at": None, "source": ""}
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None

    if isinstance(parsed, dict) and "value" in parsed:
        updated_at = parsed.get("updated_at")
        expires_at = parsed.get("expires_at")
        try:
            updated_at_f = float(updated_at) if updated_at is not None else 0.0
        except Exception:
            updated_at_f = 0.0
        try:
            expires_at_f = float(expires_at) if expires_at is not None else None
        except Exception:
            expires_at_f = None
        return {
            "value": parsed.get("value"),
            "updated_at": updated_at_f,
            "expires_at": expires_at_f,
            "source": _as_text(parsed.get("source") or "").strip(),
        }

    if parsed is not None:
        return {"value": parsed, "updated_at": 0.0, "expires_at": None, "source": ""}

    return {"value": text, "updated_at": 0.0, "expires_at": None, "source": ""}


def _memory_load_scope(redis_key: str, *, prune_expired: bool = True) -> Dict[str, Dict[str, Any]]:
    try:
        raw_map = redis_client.hgetall(redis_key) or {}
    except Exception:
        raw_map = {}

    now = time.time()
    out: Dict[str, Dict[str, Any]] = {}
    expired_fields: List[str] = []
    for raw_key, raw_value in raw_map.items():
        key = _as_text(raw_key).strip()
        if not key:
            continue
        entry = _memory_parse_entry(raw_value)
        expires_at = entry.get("expires_at")
        if isinstance(expires_at, (int, float)) and expires_at > 0 and expires_at <= now:
            expired_fields.append(key)
            continue
        out[key] = entry

    if prune_expired and expired_fields:
        try:
            redis_client.hdel(redis_key, *expired_fields)
        except Exception:
            pass
    return out


def _memory_item(key: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "key": key,
        "value": entry.get("value"),
        "updated_at": entry.get("updated_at"),
        "expires_at": entry.get("expires_at"),
        "source": entry.get("source") or "",
    }


def _normalize_keys(keys: Any) -> List[str]:
    if isinstance(keys, str):
        parts = [x.strip() for x in keys.split(",")]
    elif isinstance(keys, list):
        parts = [_as_text(x).strip() for x in keys]
    else:
        parts = []
    out: List[str] = []
    for key in parts:
        if key and key not in out:
            out.append(key)
    return out


def _memory_is_volatile_key(key: str) -> bool:
    k = _as_text(key).strip().lower()
    if not k:
        return False
    return any(k.startswith(prefix) for prefix in MEMORY_VOLATILE_PREFIXES)


def _memory_query_tokens(query: str) -> List[str]:
    tokens = re.findall(r"[a-z0-9]+", _as_text(query).lower())
    out: List[str] = []
    for token in tokens:
        if len(token) < 2:
            continue
        if token not in out:
            out.append(token)
    return out


def _value_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return _as_text(value)


def _score_memory_text(query: str, tokens: List[str], *, key: str, value_text: str, source: str) -> int:
    if not tokens:
        return 0
    key_l = key.lower()
    val_l = value_text.lower()
    src_l = source.lower()
    joined = f"{key_l}\n{val_l}\n{src_l}"
    phrase = _as_text(query).strip().lower()

    score = 0
    if phrase and len(phrase) >= 3 and phrase in joined:
        score += 6

    for token in tokens:
        if token in key_l:
            score += 4
        if token in val_l:
            score += 3
        if token in src_l:
            score += 1
    return score


def _score_truth_text(query: str, tokens: List[str], *, plugin_id: str, truth_text: str) -> int:
    if not tokens:
        return 0
    pid_l = plugin_id.lower()
    truth_l = truth_text.lower()
    joined = f"{pid_l}\n{truth_l}"
    phrase = _as_text(query).strip().lower()

    score = 0
    if phrase and len(phrase) >= 3 and phrase in joined:
        score += 5

    for token in tokens:
        if token in pid_l:
            score += 4
        if token in truth_l:
            score += 2
    return score


def _memory_targets_for_search(
    *,
    scope: Optional[str],
    user_id: Optional[str],
    room_id: Optional[str],
    platform: Optional[str],
    origin: Optional[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    scope_name = _as_text(scope or "auto").strip().lower() or "auto"
    if scope_name not in {"auto", "all", "global", "user", "room"}:
        return [], "scope must be one of: auto, all, global, user, room."

    candidates: List[Tuple[str, Optional[str], Optional[str], Optional[str]]] = []
    if scope_name in {"auto", "all"}:
        candidates.append(("global", None, None, None))
        candidates.append(("user", user_id, None, None))
        candidates.append(("room", None, room_id, platform))
    elif scope_name == "global":
        candidates.append(("global", None, None, None))
    elif scope_name == "user":
        candidates.append(("user", user_id, None, None))
    elif scope_name == "room":
        candidates.append(("room", None, room_id, platform))

    targets: List[Dict[str, Any]] = []
    seen = set()
    for s_name, uid, rid, p_name in candidates:
        target, err = _memory_scope_target(
            scope=s_name,
            user_id=uid,
            room_id=rid,
            platform=p_name,
            origin=origin,
        )
        if err or not target:
            # For auto/all we skip unavailable user/room scopes silently.
            if scope_name in {"auto", "all"} and s_name in {"user", "room"}:
                continue
            return [], err or "Invalid memory target."
        redis_key = target.get("redis_key")
        if redis_key in seen:
            continue
        seen.add(redis_key)
        targets.append(target)
    return targets, None


def memory_set(
    entries: Dict[str, Any],
    *,
    scope: str = "global",
    user_id: Optional[str] = None,
    room_id: Optional[str] = None,
    platform: Optional[str] = None,
    ttl_sec: Optional[int] = None,
    source: Optional[str] = None,
    request_text: Optional[str] = None,
    confirmed: bool = False,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not isinstance(entries, dict) or not entries:
        return {"tool": "memory_set", "ok": False, "error": "entries must be a non-empty object."}

    target, err = _memory_scope_target(
        scope=scope,
        user_id=user_id,
        room_id=room_id,
        platform=platform,
        origin=origin,
    )
    if err or not target:
        return {"tool": "memory_set", "ok": False, "error": err or "Invalid memory target."}

    if _memory_explicit_only_enabled() and not _coerce_bool(confirmed, default=False):
        if not _memory_has_explicit_intent(request_text):
            return {
                "tool": "memory_set",
                "ok": False,
                "error": "Memory write blocked until user explicitly asks to remember this.",
                "needs": [
                    "Ask the user to confirm memory write (for example: 'remember this' or 'make this the default')."
                ],
            }

    default_ttl = _memory_default_ttl()
    ttl = _coerce_int(ttl_sec if ttl_sec is not None else default_ttl, default=default_ttl, min_value=0, max_value=31_536_000)
    now = time.time()
    expires_at = now + ttl if ttl > 0 else None
    source_text = _as_text(source or "agent").strip() or "agent"
    ttl_applied: List[str] = []
    ttl_skipped: List[str] = []

    mapping: Dict[str, str] = {}
    rejected: Dict[str, str] = {}
    for raw_key, raw_value in entries.items():
        key = _as_text(raw_key).strip()
        if not MEMORY_KEY_RE.fullmatch(key):
            rejected[key or "<empty>"] = "invalid key format"
            continue
        value = _json_safe(raw_value)
        if isinstance(value, str) and len(value) > MEMORY_MAX_VALUE_CHARS:
            rejected[key] = f"value too large (>{MEMORY_MAX_VALUE_CHARS} chars)"
            continue
        # TTL is intentionally limited to volatile keys (e.g., last.*, temp.*).
        entry_expires_at = None
        if expires_at is not None:
            if _memory_is_volatile_key(key):
                entry_expires_at = expires_at
                ttl_applied.append(key)
            else:
                ttl_skipped.append(key)
        payload = {
            "value": value,
            "updated_at": now,
            "expires_at": entry_expires_at,
            "source": source_text,
        }
        mapping[key] = json.dumps(payload, ensure_ascii=False)

    if not mapping:
        return {
            "tool": "memory_set",
            "ok": False,
            "error": "No memory entries were accepted.",
            "rejected": rejected,
        }

    try:
        redis_client.hset(target["redis_key"], mapping=mapping)
    except Exception as e:
        return {"tool": "memory_set", "ok": False, "error": str(e)}

    return {
        "tool": "memory_set",
        "ok": True,
        "scope": target["scope"],
        "platform": target.get("platform"),
        "user_id": target.get("user_id"),
        "room_id": target.get("room_id"),
        "ttl_sec": ttl,
        "ttl_applied": sorted(ttl_applied),
        "ttl_skipped": sorted(ttl_skipped),
        "written": sorted(mapping.keys()),
        "rejected": rejected,
        "count": len(mapping),
    }


def memory_get(
    keys: Optional[Any] = None,
    *,
    prefix: Optional[str] = None,
    scope: str = "global",
    user_id: Optional[str] = None,
    room_id: Optional[str] = None,
    platform: Optional[str] = None,
    limit: int = 50,
    include_meta: bool = True,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    target, err = _memory_scope_target(
        scope=scope,
        user_id=user_id,
        room_id=room_id,
        platform=platform,
        origin=origin,
    )
    if err or not target:
        return {"tool": "memory_get", "ok": False, "error": err or "Invalid memory target."}

    store = _memory_load_scope(target["redis_key"], prune_expired=True)
    key_list = _normalize_keys(keys)
    prefix_text = _as_text(prefix).strip()
    max_items = _coerce_int(limit, default=50, min_value=1, max_value=MEMORY_MAX_LIST_LIMIT)

    selected: List[str] = []
    missing: List[str] = []
    if key_list:
        for key in key_list:
            if key in store:
                selected.append(key)
            else:
                missing.append(key)
    else:
        for key in sorted(store.keys()):
            if prefix_text and not key.startswith(prefix_text):
                continue
            selected.append(key)
            if len(selected) >= max_items:
                break

    values: Dict[str, Any] = {}
    items: List[Dict[str, Any]] = []
    for key in selected:
        entry = store.get(key)
        if not entry:
            continue
        values[key] = entry.get("value")
        if include_meta:
            items.append(_memory_item(key, entry))

    payload: Dict[str, Any] = {
        "tool": "memory_get",
        "ok": True,
        "scope": target["scope"],
        "platform": target.get("platform"),
        "user_id": target.get("user_id"),
        "room_id": target.get("room_id"),
        "values": values,
        "count": len(values),
        "missing": missing,
    }
    if include_meta:
        payload["items"] = items
    return payload


def memory_list(
    *,
    prefix: Optional[str] = None,
    scope: str = "global",
    user_id: Optional[str] = None,
    room_id: Optional[str] = None,
    platform: Optional[str] = None,
    limit: int = 50,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    target, err = _memory_scope_target(
        scope=scope,
        user_id=user_id,
        room_id=room_id,
        platform=platform,
        origin=origin,
    )
    if err or not target:
        return {"tool": "memory_list", "ok": False, "error": err or "Invalid memory target."}

    store = _memory_load_scope(target["redis_key"], prune_expired=True)
    prefix_text = _as_text(prefix).strip()
    max_items = _coerce_int(limit, default=50, min_value=1, max_value=MEMORY_MAX_LIST_LIMIT)

    filtered = [key for key in sorted(store.keys()) if (not prefix_text or key.startswith(prefix_text))]
    items = [_memory_item(key, store[key]) for key in filtered[:max_items]]
    return {
        "tool": "memory_list",
        "ok": True,
        "scope": target["scope"],
        "platform": target.get("platform"),
        "user_id": target.get("user_id"),
        "room_id": target.get("room_id"),
        "prefix": prefix_text or None,
        "count": len(items),
        "total_count": len(filtered),
        "has_more": len(filtered) > len(items),
        "items": items,
    }


def memory_delete(
    keys: Any,
    *,
    scope: str = "global",
    user_id: Optional[str] = None,
    room_id: Optional[str] = None,
    platform: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    target, err = _memory_scope_target(
        scope=scope,
        user_id=user_id,
        room_id=room_id,
        platform=platform,
        origin=origin,
    )
    if err or not target:
        return {"tool": "memory_delete", "ok": False, "error": err or "Invalid memory target."}

    key_list = _normalize_keys(keys)
    if not key_list:
        return {"tool": "memory_delete", "ok": False, "error": "keys must be a non-empty list or comma-separated string."}

    store = _memory_load_scope(target["redis_key"], prune_expired=True)
    existing = [key for key in key_list if key in store]
    missing = [key for key in key_list if key not in store]
    deleted = 0
    if existing:
        try:
            deleted = int(redis_client.hdel(target["redis_key"], *existing) or 0)
        except Exception as e:
            return {"tool": "memory_delete", "ok": False, "error": str(e)}

    return {
        "tool": "memory_delete",
        "ok": True,
        "scope": target["scope"],
        "platform": target.get("platform"),
        "user_id": target.get("user_id"),
        "room_id": target.get("room_id"),
        "deleted": deleted,
        "missing": missing,
    }


def _scope_priority(scope_name: Any) -> int:
    scope_text = _as_text(scope_name).strip().lower()
    if scope_text == "room":
        return 3
    if scope_text == "user":
        return 2
    if scope_text == "global":
        return 1
    return 0


def memory_explain(
    key: str,
    *,
    scope: str = "auto",
    user_id: Optional[str] = None,
    room_id: Optional[str] = None,
    platform: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    mem_key = _as_text(key).strip()
    if not mem_key:
        return {"tool": "memory_explain", "ok": False, "error": "key is required."}

    targets, err = _memory_targets_for_search(
        scope=scope,
        user_id=user_id,
        room_id=room_id,
        platform=platform,
        origin=origin,
    )
    if err:
        return {"tool": "memory_explain", "ok": False, "error": err}

    entries: List[Dict[str, Any]] = []
    for target in targets:
        store = _memory_load_scope(target["redis_key"], prune_expired=True)
        entry = store.get(mem_key)
        if not entry:
            continue
        entries.append(
            {
                "scope": target.get("scope"),
                "platform": target.get("platform"),
                "user_id": target.get("user_id"),
                "room_id": target.get("room_id"),
                "key": mem_key,
                "value": entry.get("value"),
                "source": entry.get("source") or "",
                "updated_at": entry.get("updated_at"),
                "expires_at": entry.get("expires_at"),
            }
        )

    if not entries:
        return {
            "tool": "memory_explain",
            "ok": False,
            "key": mem_key,
            "scope": _as_text(scope or "auto").strip().lower() or "auto",
            "error": "No memory entry found for key in the selected scope(s).",
        }

    entries.sort(
        key=lambda item: (
            -_scope_priority(item.get("scope")),
            -float(item.get("updated_at") or 0.0),
        )
    )
    active = entries[0]
    value_fingerprint = {json.dumps(_json_safe(item.get("value")), ensure_ascii=False, sort_keys=True) for item in entries}
    has_conflict = len(value_fingerprint) > 1

    explanation_parts = [
        "Highest-precedence scope wins (room > user > global).",
        f"Active value comes from {active.get('scope')} scope.",
    ]
    if has_conflict:
        explanation_parts.append("Conflicting values exist across scopes for this key.")
    else:
        explanation_parts.append("No conflicts detected across matching scopes.")
    if active.get("expires_at"):
        explanation_parts.append("Active value is volatile (has TTL).")
    else:
        explanation_parts.append("Active value has no TTL.")

    return {
        "tool": "memory_explain",
        "ok": True,
        "key": mem_key,
        "scope": _as_text(scope or "auto").strip().lower() or "auto",
        "active": active,
        "entries": entries,
        "has_conflict": has_conflict,
        "explanation": " ".join(explanation_parts),
    }


def memory_search(
    query: str,
    *,
    scope: str = "auto",
    user_id: Optional[str] = None,
    room_id: Optional[str] = None,
    platform: Optional[str] = None,
    include_truth: bool = True,
    limit: int = 8,
    min_score: int = 1,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    q = _as_text(query).strip()
    if not q:
        return {"tool": "memory_search", "ok": False, "error": "query is required."}
    tokens = _memory_query_tokens(q)
    if not tokens:
        return {"tool": "memory_search", "ok": False, "error": "query must include searchable text."}

    max_items = _coerce_int(limit, default=8, min_value=1, max_value=MEMORY_SEARCH_MAX_RESULTS)
    min_score_i = _coerce_int(min_score, default=1, min_value=1, max_value=1000)
    include_truth_b = _coerce_bool(include_truth, default=True)
    targets, err = _memory_targets_for_search(
        scope=scope,
        user_id=user_id,
        room_id=room_id,
        platform=platform,
        origin=origin,
    )
    if err:
        return {"tool": "memory_search", "ok": False, "error": err}

    hits: List[Dict[str, Any]] = []
    for target in targets:
        store = _memory_load_scope(target["redis_key"], prune_expired=True)
        for key, entry in store.items():
            value_text = _value_text(entry.get("value"))
            source_text = _as_text(entry.get("source") or "").strip()
            score = _score_memory_text(q, tokens, key=key, value_text=value_text, source=source_text)
            if score < min_score_i:
                continue
            hits.append(
                {
                    "kind": "memory",
                    "scope": target.get("scope"),
                    "platform": target.get("platform"),
                    "user_id": target.get("user_id"),
                    "room_id": target.get("room_id"),
                    "key": key,
                    "value": entry.get("value"),
                    "source": source_text,
                    "updated_at": entry.get("updated_at"),
                    "expires_at": entry.get("expires_at"),
                    "score": score,
                }
            )

    truth_scanned = 0
    if include_truth_b:
        truth_target = _truth_target(platform=platform, scope=room_id, origin=origin)
        rows = redis_client.lrange(truth_target["list_key"], -300, -1) or []
        truth_scanned = len(rows)
        for raw in reversed(rows):
            entry = _truth_entry(raw)
            if not entry:
                continue
            plugin_id_text = _as_text(entry.get("plugin_id") or "").strip()
            truth_text = _value_text(entry.get("truth"))
            score = _score_truth_text(q, tokens, plugin_id=plugin_id_text, truth_text=truth_text)
            if score < min_score_i:
                continue
            hits.append(
                {
                    "kind": "truth",
                    "scope": entry.get("scope"),
                    "platform": entry.get("platform"),
                    "plugin_id": plugin_id_text,
                    "ts": entry.get("ts"),
                    "truth": entry.get("truth"),
                    "score": score,
                }
            )

    hits.sort(
        key=lambda item: (
            -_coerce_int(item.get("score"), 0, min_value=0),
            -float(item.get("updated_at") or item.get("ts") or 0.0),
            _as_text(item.get("key") or item.get("plugin_id") or ""),
        )
    )
    trimmed = hits[:max_items]
    return {
        "tool": "memory_search",
        "ok": True,
        "query": q,
        "scope": _as_text(scope or "auto").strip().lower() or "auto",
        "include_truth": include_truth_b,
        "count": len(trimmed),
        "total_matches": len(hits),
        "has_more": len(hits) > len(trimmed),
        "memory_scopes": [t.get("scope") for t in targets],
        "truth_scanned": truth_scanned,
        "results": trimmed,
    }


def _truth_target(
    *,
    platform: Optional[str],
    scope: Optional[str],
    origin: Optional[Dict[str, Any]],
) -> Dict[str, str]:
    platform_name = _as_text(platform or _origin_value(origin, "platform") or "webui").strip().lower() or "webui"
    scope_name = _as_text(
        scope or _origin_value(origin, "scope", "channel_id", "channel", "room_id", "room", "chat_id") or "chat"
    ).strip() or "chat"
    return {
        "platform": platform_name,
        "scope": scope_name,
        "list_key": f"tater:truth:{platform_name}:{scope_name}",
        "latest_key": f"tater:truth:last:{platform_name}:{scope_name}",
    }


def _truth_entry(raw: Any) -> Optional[Dict[str, Any]]:
    text = _as_text(raw)
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    return {
        "ts": parsed.get("ts"),
        "platform": parsed.get("platform"),
        "scope": parsed.get("scope"),
        "plugin_id": parsed.get("plugin_id"),
        "truth": parsed.get("truth"),
    }


def truth_get_last(
    *,
    platform: Optional[str] = None,
    scope: Optional[str] = None,
    plugin_id: Optional[str] = None,
    scan_limit: int = 200,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    target = _truth_target(platform=platform, scope=scope, origin=origin)
    plugin_filter = _as_text(plugin_id).strip()
    max_scan = _coerce_int(scan_limit, default=200, min_value=1, max_value=1000)

    latest = _truth_entry(redis_client.get(target["latest_key"]))
    if latest and (not plugin_filter or _as_text(latest.get("plugin_id")) == plugin_filter):
        return {
            "tool": "truth_get_last",
            "ok": True,
            "platform": target["platform"],
            "scope": target["scope"],
            "entry": latest,
        }

    rows = redis_client.lrange(target["list_key"], -max_scan, -1) or []
    for raw in reversed(rows):
        entry = _truth_entry(raw)
        if not entry:
            continue
        if plugin_filter and _as_text(entry.get("plugin_id")) != plugin_filter:
            continue
        return {
            "tool": "truth_get_last",
            "ok": True,
            "platform": target["platform"],
            "scope": target["scope"],
            "entry": entry,
        }

    return {
        "tool": "truth_get_last",
        "ok": False,
        "platform": target["platform"],
        "scope": target["scope"],
        "error": "No matching truth snapshot found.",
    }


def truth_list(
    *,
    platform: Optional[str] = None,
    scope: Optional[str] = None,
    plugin_id: Optional[str] = None,
    limit: int = 10,
    origin: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    target = _truth_target(platform=platform, scope=scope, origin=origin)
    plugin_filter = _as_text(plugin_id).strip()
    max_items = _coerce_int(limit, default=10, min_value=1, max_value=100)
    scan = min(1000, max(max_items * 6, max_items))

    rows = redis_client.lrange(target["list_key"], -scan, -1) or []
    items: List[Dict[str, Any]] = []
    total_matches = 0
    for raw in reversed(rows):
        entry = _truth_entry(raw)
        if not entry:
            continue
        if plugin_filter and _as_text(entry.get("plugin_id")) != plugin_filter:
            continue
        total_matches += 1
        if len(items) < max_items:
            items.append(entry)

    return {
        "tool": "truth_list",
        "ok": True,
        "platform": target["platform"],
        "scope": target["scope"],
        "plugin_id": plugin_filter or None,
        "count": len(items),
        "total_count": total_matches,
        "has_more": total_matches > len(items),
        "entries": items,
    }


def write_workspace_note(content: str) -> Dict[str, Any]:
    _ensure_dirs()
    ts = time.strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    filename = f"note_{ts}_{suffix}.md"
    path = AGENT_WORKSPACE_DIR / filename
    try:
        data = content or ""
        path.write_text(data, encoding="utf-8")
        _log_write("write_workspace_note", path, len(data.encode("utf-8")))
        return {"tool": "write_workspace_note", "ok": True, "path": str(path)}
    except Exception as e:
        return {"tool": "write_workspace_note", "ok": False, "error": str(e)}


def list_workspace() -> Dict[str, Any]:
    _ensure_dirs()
    try:
        files = sorted([p.name for p in AGENT_WORKSPACE_DIR.iterdir() if p.is_file()])
        return {"tool": "list_workspace", "ok": True, "files": files}
    except Exception as e:
        return {"tool": "list_workspace", "ok": False, "error": str(e)}


def list_agent_plugins() -> Dict[str, Any]:
    _ensure_dirs()
    items = []
    errors = []
    for path in sorted(AGENT_PLUGINS_DIR.glob("*.py")):
        name = path.stem
        module = _import_from_path(path)
        if not module:
            errors.append({"name": name, "error": "Import failed"})
            continue
        plugin = getattr(module, "plugin", None)
        if not plugin:
            errors.append({"name": name, "error": "Missing plugin instance"})
            continue
        items.append(
            {
                "id": getattr(plugin, "name", name),
                "version": getattr(plugin, "version", ""),
                "platforms": getattr(plugin, "platforms", []) or [],
                "description": getattr(plugin, "description", "") or getattr(plugin, "plugin_dec", "") or "",
            }
        )
    return {"tool": "list_agent_plugins", "ok": True, "plugins": items, "errors": errors}


def list_agent_platforms() -> Dict[str, Any]:
    _ensure_dirs()
    items = []
    errors = []
    for path in sorted(AGENT_PLATFORMS_DIR.glob("*.py")):
        name = path.stem
        module = _import_from_path(path)
        if not module:
            errors.append({"name": name, "error": "Import failed"})
            continue
        platform_dict = getattr(module, "PLATFORM", None)
        if not isinstance(platform_dict, dict):
            errors.append({"name": name, "error": "Missing PLATFORM dict"})
            continue
        items.append({"key": name, "label": platform_dict.get("label") or name})
    return {"tool": "list_agent_platforms", "ok": True, "platforms": items, "errors": errors}

_ensure_dirs()

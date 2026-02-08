import ast
import codecs
import csv
import fnmatch
import io
import json
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
import ipaddress
import socket
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from helpers import redis_client
from plugin_loader import load_plugins_from_directory
from plugin_base import ToolPlugin
from plugin_registry import reload_plugins
from plugin_settings import get_plugin_enabled


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

    if normalized == "/agent_lab":
        raw = str(AGENT_LAB_DIR)
    elif normalized.startswith("/agent_lab/"):
        suffix = normalized[len("/agent_lab/") :]
        raw = str(AGENT_LAB_DIR / suffix)
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


def read_url(
    url: str,
    *,
    max_bytes: int = 200_000,
    timeout_sec: int = 15,
) -> Dict[str, Any]:
    err = _validate_url(url)
    if err:
        return {"tool": "read_url", "ok": False, "error": err}
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Tater-AgentLab/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            content_type = resp.headers.get("Content-Type", "") or ""
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
            "url": url,
            "content_type": content_type,
            "bytes": len(raw),
            "truncated": truncated,
            "content": content,
        }
    except Exception as e:
        return {"tool": "read_url", "ok": False, "error": str(e)}


def download_file(
    url: str,
    *,
    filename: Optional[str] = None,
    subdir: Optional[str] = None,
    max_bytes: int = 25_000_000,
    timeout_sec: int = 30,
) -> Dict[str, Any]:
    _ensure_dirs()
    err = _validate_url(url)
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

    parsed = urllib.parse.urlparse(url)
    default_name = _sanitize_filename(os.path.basename(parsed.path)) or "download.bin"
    safe_name = _sanitize_filename(filename or default_name) or "download.bin"
    dest = target_dir / safe_name

    import hashlib

    hasher = hashlib.sha256()
    size = 0
    content_type = ""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Tater-AgentLab/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            content_type = resp.headers.get("Content-Type", "") or ""
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
    return {
        "tool": "download_file",
        "ok": True,
        "url": url,
        "path": str(dest),
        "bytes": size,
        "sha256": hasher.hexdigest(),
        "content_type": content_type,
    }


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
    dest_path = _resolve_safe_path(destination or default_dest, allowed)
    if not dest_path:
        return {"tool": "extract_archive", "ok": False, "error": "Destination path not allowed."}
    if dest_path.exists() and not dest_path.is_dir():
        return {"tool": "extract_archive", "ok": False, "error": "Destination exists and is not a directory."}
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
    return True, ""


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
        ast.parse(source, filename=str(path))
    except Exception as e:
        report = {
            "tool": "validate_plugin",
            "ok": False,
            "error": f"Syntax error: {e}",
            "path": str(path),
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
            lowered = explicit_wait_prompt.lower()
            has_instruction = any(word in lowered for word in ("write", "generate", "tell", "say", "respond"))
            has_output_constraint = any(
                phrase in lowered for phrase in ("only output", "output only", "only return", "return only")
            )
            if not (has_instruction and has_output_constraint):
                warnings.append(
                    "waiting_prompt_template should instruct the LLM (e.g., 'Write ...') "
                    "and constrain output (e.g., 'Only output that message.')."
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
        report["plugin_name"] = getattr(plugin, "name", name)
        report["version"] = getattr(plugin, "version", "")
        report["platforms"] = getattr(plugin, "platforms", []) or []
    _store_validation("plugin", name, report)
    return report


def create_plugin(
    name: str,
    code: Optional[str] = None,
    *,
    code_b64: Optional[str] = None,
    code_lines: Optional[List[str]] = None,
) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        return {"tool": "create_plugin", "ok": False, "error": "Invalid plugin name."}
    path = _exp_plugin_path(name)
    try:
        if code_b64:
            try:
                import base64
                payload = base64.b64decode(code_b64.encode("utf-8")).decode("utf-8")
            except Exception as e:
                return {"tool": "create_plugin", "ok": False, "error": f"Invalid code_b64: {e}"}
        elif isinstance(code_lines, list):
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
    code_b64: Optional[str] = None,
    code_lines: Optional[List[str]] = None,
) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        return {"tool": "create_platform", "ok": False, "error": "Invalid platform name."}
    path = _exp_platform_path(name)
    try:
        if code_b64:
            try:
                import base64
                payload = base64.b64decode(code_b64.encode("utf-8")).decode("utf-8")
            except Exception as e:
                return {"tool": "create_platform", "ok": False, "error": f"Invalid code_b64: {e}"}
        elif isinstance(code_lines, list):
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

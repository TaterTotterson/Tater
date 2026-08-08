from __future__ import annotations

import argparse
import hashlib
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


_MEMORY_ADDRESS = 0x02000000
_CHUNK_SIZE = 32 * 1024 * 1024
_NAND_PAGE_SIZE = 2048
_PARTITION_LAYOUT: Tuple[Tuple[str, int, int], ...] = (
    ("boot", 0x02C00000, 15 * 1024 * 1024),
    ("recovery", 0x01C00000, 16 * 1024 * 1024),
    ("system", 0x03B00000, 280 * 1024 * 1024),
)


class FlashError(RuntimeError):
    pass


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _stream_command(command: List[str], *, cwd: Path) -> None:
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    output: List[str] = []
    try:
        assert process.stdout is not None
        for line in iter(process.stdout.readline, ""):
            print(line.rstrip(), flush=True)
            output.append(line)
        returncode = process.wait()
    except Exception:
        process.kill()
        process.wait()
        raise
    text = "".join(output).lower()
    if returncode != 0 or "[ko]" in text or "can not find device" in text:
        raise FlashError(f"Command failed ({returncode}): {' '.join(command[:2])}")


def _captured_command(
    command: List[str],
    *,
    cwd: Path,
    timeout: float = 120.0,
    allow_disconnect: bool = False,
) -> str:
    try:
        result = subprocess.run(
            command,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        if allow_disconnect:
            return ""
        raise FlashError(f"Command timed out: {' '.join(command[:2])}")
    output = "\n".join(part.strip() for part in (result.stdout, result.stderr) if part and part.strip())
    if output:
        print(output, flush=True)
    lowered = output.lower()
    failed = (
        result.returncode != 0
        or "[ko]" in lowered
        or "can not find device" in lowered
        or bool(re.search(r"(?:^|\])err(?:or)?\b", lowered, flags=re.MULTILINE))
    )
    if failed and not allow_disconnect:
        raise FlashError(f"Amlogic command failed ({result.returncode}): {' '.join(command[1:3])}")
    return output


def _parse_partition_files(image_cfg: Path) -> Dict[str, Path]:
    partitions: Dict[str, Path] = {}
    pattern = re.compile(
        r'^file="(?P<file>[^"]+)".*main_type="PARTITION".*sub_type="(?P<name>[^"]+)"',
        re.IGNORECASE,
    )
    for line in image_cfg.read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.search(line.strip())
        if match:
            partitions[match.group("name")] = image_cfg.parent / match.group("file")
    return partitions


def _unpack_and_validate(packer: Path, image: Path, output_dir: Path, *, cwd: Path) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=False)
    output = _captured_command([str(packer), "-d", str(image), str(output_dir)], cwd=cwd)
    if "image unpack ok" not in output.lower():
        raise FlashError("The Amlogic factory image could not be unpacked.")
    image_cfg = output_dir / "image.cfg"
    if not image_cfg.is_file():
        raise FlashError("The factory image has no image.cfg manifest.")
    partitions = _parse_partition_files(image_cfg)
    required = {"_aml_dtb", "bootloader", "boot", "recovery", "system"}
    missing = sorted(name for name in required if not partitions.get(name, Path()).is_file())
    if missing:
        raise FlashError("The factory image is missing S420 partitions: " + ", ".join(missing))
    dtb_data = partitions["_aml_dtb"].read_bytes()
    if b"axg_s420_v03trspk" not in dtb_data:
        raise FlashError("The selected factory image is not for the ThirdReality S420.")
    for name, _, capacity in _PARTITION_LAYOUT:
        size = partitions[name].stat().st_size
        if size <= 0 or size > capacity:
            raise FlashError(f"The {name} payload does not fit the S420 NAND layout ({size} bytes).")
    return partitions


def _chunks(path: Path) -> Iterable[Tuple[int, bytes]]:
    offset = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(_CHUNK_SIZE)
            if not chunk:
                break
            yield offset, chunk
            offset += len(chunk)


def _write_and_verify_partition(
    update: Path,
    tool_root: Path,
    partition: str,
    source: Path,
    nand_offset: int,
    scratch: Path,
    completed_before: int,
    grand_total: int,
) -> None:
    print(f"Erasing S420 {partition} partition...", flush=True)
    _captured_command([str(update), "bulkcmd", f"nand erase.part {partition}"], cwd=tool_root)
    total = source.stat().st_size
    completed = 0
    for index, (relative_offset, data) in enumerate(_chunks(source)):
        padded_size = (len(data) + _NAND_PAGE_SIZE - 1) & ~(_NAND_PAGE_SIZE - 1)
        padded = data + (b"\xff" * (padded_size - len(data)))
        chunk_file = scratch / f"{partition}-{index:02d}.write"
        readback_file = scratch / f"{partition}-{index:02d}.readback"
        chunk_file.write_bytes(padded)
        physical_offset = nand_offset + relative_offset
        _captured_command(
            [str(update), "mwrite", str(chunk_file), "mem", hex(_MEMORY_ADDRESS), "normal"],
            cwd=tool_root,
        )
        _captured_command([str(update), "bulkcmd", "nand device 1"], cwd=tool_root)
        _captured_command(
            [
                str(update),
                "bulkcmd",
                f"nand write {hex(_MEMORY_ADDRESS)} {hex(physical_offset)} {hex(padded_size)}",
            ],
            cwd=tool_root,
        )
        _captured_command(
            [
                str(update),
                "bulkcmd",
                f"nand read {hex(_MEMORY_ADDRESS)} {hex(physical_offset)} {hex(padded_size)}",
            ],
            cwd=tool_root,
        )
        _captured_command(
            [
                str(update),
                "mread",
                "mem",
                hex(_MEMORY_ADDRESS),
                "normal",
                hex(padded_size),
                str(readback_file),
            ],
            cwd=tool_root,
        )
        readback = readback_file.read_bytes()[: len(data)]
        if len(readback) != len(data) or _sha256(readback) != _sha256(data):
            raise FlashError(f"S420 {partition} read-back verification failed at {hex(physical_offset)}.")
        completed += len(data)
        percent = 20.0 + (75.0 * (completed_before + completed) / max(1, grand_total))
        print(f"{partition}: verified {completed}/{total} bytes ({percent:.1f}%)", flush=True)


def flash(tool_root: Path, image: Path) -> None:
    tool_root = tool_root.resolve()
    image = image.resolve()
    flash_tool = tool_root / "flash-tool"
    bash = Path(shutil.which("bash") or "/bin/bash")
    if sys.platform == "darwin":
        system_dir = "macos"
    else:
        system_dir = "linux-x86" if platform.machine().lower() in {"x86_64", "amd64"} else "linux-arm"
    update = tool_root / "tools" / system_dir / "update"
    packer = tool_root / "tools" / system_dir / "aml_image_v2_packer"
    for required in (bash, flash_tool, update, packer, image):
        if not required.is_file():
            raise FlashError(f"Required S420 flash file is missing: {required}")

    with tempfile.TemporaryDirectory(prefix="tater-s420-raw-") as temp_name:
        scratch = Path(temp_name)
        print("Validating the Tater S420 factory image...", flush=True)
        partitions = _unpack_and_validate(packer, image, scratch / "image", cwd=tool_root)
        print("Factory image validated (5.0%).", flush=True)
        print("Initializing S420 DDR, U-Boot, NAND layout, and device tree...", flush=True)
        _stream_command(
            [
                str(bash),
                str(flash_tool),
                f"--img={image}",
                "--parts=bootloader",
                "--wipe",
                "--soc=axg",
                "--reset=n",
                "--debug",
            ],
            cwd=tool_root,
        )
        print("S420 bootloader and NAND layout initialized (20.0%).", flush=True)
        payload_total = sum(partitions[name].stat().st_size for name, _, _ in _PARTITION_LAYOUT)
        payload_completed = 0
        for name, nand_offset, _ in _PARTITION_LAYOUT:
            _write_and_verify_partition(
                update,
                tool_root,
                name,
                partitions[name],
                nand_offset,
                scratch,
                payload_completed,
                payload_total,
            )
            payload_completed += partitions[name].stat().st_size
        print("All S420 NAND payloads passed byte-for-byte read-back verification (98.0%).", flush=True)
        _captured_command(
            [str(update), "bulkcmd", "reset"],
            cwd=tool_root,
            timeout=8.0,
            allow_disconnect=True,
        )
        print("S420 factory flash complete (100.0%).", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Tater ThirdReality S420 raw-NAND factory flasher")
    parser.add_argument("--tool-root", required=True, type=Path)
    parser.add_argument("--image", required=True, type=Path)
    args = parser.parse_args()
    try:
        flash(args.tool_root, args.image)
    except Exception as exc:
        print(f"S420 flash error: {str(exc) or exc.__class__.__name__}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

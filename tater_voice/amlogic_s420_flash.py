from __future__ import annotations

import argparse
import contextlib
import os
import platform
import re
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Tuple


_PARTITION_LAYOUT: Tuple[Tuple[str, int], ...] = (
    ("boot", 15 * 1024 * 1024),
    ("recovery", 16 * 1024 * 1024),
    ("system", 280 * 1024 * 1024),
)
_UART_POST_WRITE_DRAIN_SECONDS = 3.0
_POST_BURN_PASSIVE_DELAY_SECONDS = 8.0
_POST_BURN_PROBE_SECONDS = 8.0


class FlashError(RuntimeError):
    pass


def _stream_command(command: List[str], *, cwd: Path, timeout: float = 90.0) -> None:
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    output: List[str] = []
    reader_error: List[BaseException] = []

    def read_output() -> None:
        try:
            assert process.stdout is not None
            for line in iter(process.stdout.readline, ""):
                print(line.rstrip(), flush=True)
                output.append(line)
        except BaseException as exc:  # pragma: no cover - defensive pipe cleanup
            reader_error.append(exc)

    reader = threading.Thread(target=read_output, name="s420-flash-output", daemon=True)
    reader.start()
    timed_out = False
    try:
        returncode = process.wait(timeout=max(1.0, float(timeout)))
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            returncode = process.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            returncode = process.wait()
    finally:
        reader.join(timeout=2.0)
        if process.stdout is not None:
            process.stdout.close()

    if timed_out:
        raise FlashError(f"Command timed out ({timeout:.0f}s): {' '.join(command[:2])}")
    if reader_error:
        raise FlashError(f"Could not read flash-tool output: {reader_error[0]}")
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
    for name, capacity in _PARTITION_LAYOUT:
        size = partitions[name].stat().st_size
        if size <= 0 or size > capacity:
            raise FlashError(f"The {name} payload does not fit the S420 NAND layout ({size} bytes).")
    return partitions


def _capture_uboot_output(
    update: Path,
    tool_root: Path,
    command: str,
    port: str,
) -> str:
    import termios

    fd = os.open(port, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
    try:
        attrs = termios.tcgetattr(fd)
        attrs[0] = 0
        attrs[1] = 0
        attrs[2] = termios.CS8 | termios.CREAD | termios.CLOCAL
        attrs[3] = 0
        attrs[4] = termios.B115200
        attrs[5] = termios.B115200
        attrs[6][termios.VMIN] = 0
        attrs[6][termios.VTIME] = 1
        termios.tcsetattr(fd, termios.TCSANOW, attrs)
        termios.tcflush(fd, termios.TCIFLUSH)
        _captured_command([str(update), "bulkcmd", command], cwd=tool_root)
        output = bytearray()
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            ready, _, _ = select.select([fd], [], [], min(0.2, max(0.0, deadline - time.monotonic())))
            if not ready:
                continue
            try:
                chunk = os.read(fd, 65536)
            except BlockingIOError:
                continue
            if chunk:
                output.extend(chunk)
                if b"[info]success" in output or b"[info]failed" in output:
                    break
        text = output.decode("utf-8", errors="replace").replace("\x00", "")
        if "[info]success" not in text:
            raise FlashError(f"The S420 console did not return the result of `{command}`.")
        return text
    finally:
        os.close(fd)


def _write_partition(
    update: Path,
    tool_root: Path,
    partition: str,
    source: Path,
    completed_before: int,
    grand_total: int,
    debug_port: str = "",
    *,
    progress_start: float = 20.0,
    progress_span: float = 75.0,
) -> None:
    """Write one logical partition and confirm U-Boot's real burn result."""
    import termios

    port = str(debug_port or "").strip()
    if not port or not Path(port).exists():
        raise FlashError("The verified S420 debug-console port is no longer available.")
    source_size = source.stat().st_size
    print(f"Writing S420 {partition} partition with the Amlogic store writer...", flush=True)

    fd = os.open(port, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
    process: subprocess.Popen[str] | None = None
    host_output: List[str] = []
    uart_output = bytearray()
    reader_error: List[BaseException] = []
    try:
        attrs = termios.tcgetattr(fd)
        attrs[0] = 0
        attrs[1] = 0
        attrs[2] = termios.CS8 | termios.CREAD | termios.CLOCAL
        attrs[3] = 0
        attrs[4] = termios.B115200
        attrs[5] = termios.B115200
        attrs[6][termios.VMIN] = 0
        attrs[6][termios.VTIME] = 1
        termios.tcsetattr(fd, termios.TCSANOW, attrs)
        termios.tcflush(fd, termios.TCIFLUSH)

        process = subprocess.Popen(
            [str(update), "partition", partition, str(source), "normal"],
            cwd=str(tool_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )

        def read_host_output() -> None:
            try:
                assert process is not None and process.stdout is not None
                for line in iter(process.stdout.readline, ""):
                    cleaned = line.rstrip()
                    print(cleaned, flush=True)
                    host_output.append(line)
                    progress_match = re.search(r"\[\s*(\d{1,3})%/", cleaned)
                    if progress_match:
                        local_percent = min(100.0, float(progress_match.group(1)))
                        transferred = source_size * local_percent / 100.0
                        overall = progress_start + (
                            progress_span * (completed_before + transferred) / max(1, grand_total)
                        )
                        print(
                            f"{partition}: vendor transfer in progress ({overall:.1f}%).",
                            flush=True,
                        )
            except BaseException as exc:  # pragma: no cover - defensive pipe cleanup
                reader_error.append(exc)

        reader = threading.Thread(target=read_host_output, name=f"s420-{partition}-writer", daemon=True)
        reader.start()
        deadline = time.monotonic() + 600.0
        while process.poll() is None:
            if time.monotonic() >= deadline:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGTERM)
                raise FlashError(f"The S420 {partition} partition write timed out.")
            ready, _, _ = select.select([fd], [], [], 0.2)
            if ready:
                with contextlib.suppress(BlockingIOError):
                    chunk = os.read(fd, 65536)
                    if chunk:
                        uart_output.extend(chunk)
                        if len(uart_output) > 512 * 1024:
                            del uart_output[: len(uart_output) - 512 * 1024]

        quiet_deadline = time.monotonic() + _UART_POST_WRITE_DRAIN_SECONDS
        while time.monotonic() < quiet_deadline:
            ready, _, _ = select.select([fd], [], [], 0.2)
            if not ready:
                continue
            with contextlib.suppress(BlockingIOError):
                chunk = os.read(fd, 65536)
                if chunk:
                    uart_output.extend(chunk)
                    quiet_deadline = time.monotonic() + 0.4
        reader.join(timeout=2.0)
        if process.stdout is not None:
            process.stdout.close()
        if reader_error:
            raise FlashError(f"Could not read the {partition} writer output: {reader_error[0]}")

        host_text = "".join(host_output)
        uart_text = uart_output.decode("utf-8", errors="replace").replace("\x00", "")
        failed = (
            process.returncode != 0
            or "[ko]" in host_text.lower()
            or "[info]failed" in uart_text.lower()
            or "[msg]burn complete" not in uart_text.lower()
            or "[info]success" not in uart_text.lower()
        )
        if failed:
            tail = uart_text[-1200:].strip()
            detail = f" Last console output: {tail}" if tail else ""
            raise FlashError(f"The S420 did not confirm the {partition} partition burn.{detail}")
    finally:
        if process is not None and process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
        os.close(fd)

    percent = progress_start + (
        progress_span * (completed_before + source_size) / max(1, grand_total)
    )
    print(f"{partition}: vendor NAND write confirmed ({percent:.1f}%).", flush=True)


def _wait_for_burn_mode(update: Path, tool_root: Path, *, timeout: float) -> bool:
    """Return true when the S420 has fallen back into Amlogic USB burn mode."""
    deadline = time.monotonic() + max(0.0, float(timeout))
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                [str(update), "identify"],
                cwd=str(tool_root),
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
            output = "\n".join(part for part in (result.stdout, result.stderr) if part)
            if result.returncode == 0 and "firmware version" in output.lower():
                return True
        except subprocess.TimeoutExpired:
            pass
        time.sleep(0.5)
    return False


def _restore_payloads_after_first_reset(
    update: Path,
    tool_root: Path,
    partitions: Dict[str, Path],
    debug_port: str,
) -> bool:
    """Restore payloads that the S420's first burn reset invalidates."""
    # A valid image reaches the kernel quickly. Stay completely off UART while
    # U-Boot's autoboot countdown is active, then look only for Amlogic USB.
    time.sleep(_POST_BURN_PASSIVE_DELAY_SECONDS)
    if not _wait_for_burn_mode(update, tool_root, timeout=_POST_BURN_PROBE_SECONDS):
        return False

    print(
        "S420 returned to burn mode after first finalization; restoring its firmware payloads (98.2%).",
        flush=True,
    )
    # This ROM/U-Boot handoff drops the first four command bytes after reset.
    # The vendor helper consumes them with a disposable bulk command before
    # sending another `download store ...` transfer.
    _captured_command(
        [str(update), "bulkcmd", "echo 12345"],
        cwd=tool_root,
        timeout=5.0,
        allow_disconnect=True,
    )
    # The first finalization on this board has been observed to leave both the
    # boot partition erased and the system UBI partition completely empty. Do
    # the durable payload pass only after U-Boot has returned to burn mode.
    # Keep boot last so it is the final NAND payload written before reboot.
    restore_order = ("recovery", "system", "boot")
    restore_total = sum(partitions[name].stat().st_size for name in restore_order)
    restored = 0
    for name in restore_order:
        _write_partition(
            update,
            tool_root,
            name,
            partitions[name],
            restored,
            restore_total,
            debug_port,
            progress_start=98.2,
            progress_span=0.5,
        )
        restored += partitions[name].stat().st_size
    # The first burn reset already normalized the vendor environment. Keep the
    # normal-boot value for this durable second payload pass.
    _capture_uboot_output(update, tool_root, "setenv upgrade_step 2", debug_port)
    _capture_uboot_output(update, tool_root, "save", debug_port)
    _captured_command(
        [str(update), "bulkcmd", "burn_complete 1"],
        cwd=tool_root,
        timeout=8.0,
        allow_disconnect=True,
    )
    print("S420 firmware payload restore complete; waiting for verified Tater boot (98.8%).", flush=True)
    return True


def flash(tool_root: Path, image: Path, debug_port: str) -> None:
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
        initialization_command = [
            str(bash),
            str(flash_tool),
            f"--img={image}",
            "--parts=bootloader",
            "--wipe",
            "--soc=axg",
            "--reset=n",
            "--debug",
        ]
        for attempt in range(2):
            try:
                _stream_command(initialization_command, cwd=tool_root)
                break
            except FlashError as exc:
                if attempt or "timed out" not in str(exc).lower():
                    raise
                # The legacy helper can wait forever for the intentional USB
                # disconnect after erase_bootloader/reset. The board is then
                # already back in ROM mode, so a clean retry is safe.
                print("Amlogic reset helper stalled; retrying S420 initialization...", flush=True)
        print("S420 bootloader and NAND layout initialized (20.0%).", flush=True)
        payload_total = sum(partitions[name].stat().st_size for name, _ in _PARTITION_LAYOUT)
        payload_completed = 0
        for name, _ in _PARTITION_LAYOUT:
            _write_partition(
                update,
                tool_root,
                name,
                partitions[name],
                payload_completed,
                payload_total,
                debug_port,
            )
            payload_completed += partitions[name].stat().st_size
        print("All S420 NAND payloads were confirmed by the vendor store writer (98.0%).", flush=True)
        # Preserve the vendor burn state machine: burn_complete must see
        # upgrade_step=1. During the reset, this board's U-Boot converts it to
        # the persistent normal-boot value (2). Setting 2 before burn_complete
        # makes the first burn session invalidate the freshly written boot
        # partition.
        _capture_uboot_output(update, tool_root, "setenv upgrade_step 1", debug_port)
        _capture_uboot_output(update, tool_root, "save", debug_port)
        _captured_command(
            [str(update), "bulkcmd", "burn_complete 1"],
            cwd=tool_root,
            timeout=8.0,
            allow_disconnect=True,
        )
        _restore_payloads_after_first_reset(
            update,
            tool_root,
            partitions,
            debug_port,
        )
        print("S420 factory write complete; waiting for verified Tater boot (98.0%).", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Tater ThirdReality S420 factory flasher")
    parser.add_argument("--tool-root", required=True, type=Path)
    parser.add_argument("--image", required=True, type=Path)
    parser.add_argument("--debug-port", required=True)
    args = parser.parse_args()
    try:
        flash(args.tool_root, args.image, args.debug_port)
    except Exception as exc:
        print(f"S420 flash error: {str(exc) or exc.__class__.__name__}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

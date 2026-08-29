#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]
SETUP_SOURCE = ROOT / "setup_tater.sh"
SETUP_LIBRARY = SETUP_SOURCE.read_text(encoding="utf-8").rsplit('\nmain "$@"', 1)[0]


def run_setup_functions(body: str, *, environ: dict[str, str]) -> subprocess.CompletedProcess[str]:
    script = f"{SETUP_LIBRARY}\n{textwrap.dedent(body)}\n"
    env = os.environ.copy()
    env.update(environ)
    return subprocess.run(
        ["sh", "-s"],
        cwd=ROOT,
        env=env,
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )


class SetupTaterVenvTests(unittest.TestCase):
    def test_requirement_builds_receive_cmake_four_compatibility_floor(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fake_python = Path(temp_dir) / "python"
            fake_python.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' \"${CMAKE_POLICY_VERSION_MINIMUM:-}\" \"$*\" > \"${TEST_LOG}\"\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o755)
            requirements = Path(temp_dir) / "requirements.txt"
            requirements.write_text("example\n", encoding="utf-8")
            log_path = Path(temp_dir) / "pip.log"

            completed = run_setup_functions(
                r"""
                unset CMAKE_POLICY_VERSION_MINIMUM
                pip_install_requirements "${TEST_PYTHON}" "${TEST_REQUIREMENTS}"
                """,
                environ={
                    "TEST_LOG": str(log_path),
                    "TEST_PYTHON": str(fake_python),
                    "TEST_REQUIREMENTS": str(requirements),
                },
            )
            output = log_path.read_text(encoding="utf-8") if log_path.exists() else ""

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertEqual(output.splitlines()[0], "3.5")
        self.assertIn(f"-m pip install -r {requirements}", output)

    def test_all_requirement_files_use_the_compatibility_helper(self) -> None:
        self.assertEqual(SETUP_LIBRARY.count('-m pip install -r'), 1)

    def test_missing_ensurepip_is_repaired_for_every_linux_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            completed = run_setup_functions(
                r"""
                python_version() { printf '%s' '3.14'; }
                install_linux_python_venv_support() {
                  : > "${TEST_ROOT}/installed"
                }
                create_python_venv() {
                  profile="$1"
                  marker="${TEST_ROOT}/attempted-${profile}"
                  if [ ! -f "${marker}" ]; then
                    : > "${marker}"
                    mkdir -p "${VENV_DIR}/bin"
                    : > "${VENV_DIR}/pyvenv.cfg"
                    printf '%s\n' 'ensurepip is not available' >&2
                    return 1
                  fi
                  mkdir -p "${VENV_DIR}/bin"
                  : > "${VENV_DIR}/pyvenv.cfg"
                  printf '%s\n' '#!/bin/sh' 'exit 0' > "${VENV_DIR}/bin/python"
                  chmod +x "${VENV_DIR}/bin/python"
                }

                for profile in cpu edge nvidia rocm jetson thor; do
                  VENV_DIR="${TEST_ROOT}/venv-${profile}"
                  RUNTIME_DIR="${TEST_ROOT}/runtime-${profile}"
                  PROFILE_FILE="${RUNTIME_DIR}/setup_profile"
                  ensure_venv "${profile}" /fake/python
                  test -f "${TEST_ROOT}/installed"
                  rm -f "${TEST_ROOT}/installed"
                  test -x "${VENV_DIR}/bin/python"
                done
                """,
                environ={"TEST_ROOT": temp_dir},
            )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)

    def test_incomplete_venv_is_removed_before_profile_mismatch_check(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            venv_dir = Path(temp_dir) / "venv"
            (venv_dir / "bin").mkdir(parents=True)
            (venv_dir / "pyvenv.cfg").write_text("partial\n", encoding="utf-8")
            python = venv_dir / "bin" / "python"
            python.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
            python.chmod(0o755)

            completed = run_setup_functions(
                r"""
                confirm() { return 1; }
                create_python_venv() {
                  mkdir -p "${VENV_DIR}/bin"
                  : > "${VENV_DIR}/pyvenv.cfg"
                  printf '%s\n' '#!/bin/sh' 'exit 0' > "${VENV_DIR}/bin/python"
                  chmod +x "${VENV_DIR}/bin/python"
                }
                VENV_DIR="${TEST_ROOT}/venv"
                RUNTIME_DIR="${TEST_ROOT}/runtime"
                PROFILE_FILE="${RUNTIME_DIR}/setup_profile"
                ensure_venv rocm /fake/python
                """,
                environ={"TEST_ROOT": temp_dir},
            )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("Removing incomplete", completed.stdout)
        self.assertNotIn("prepared for", completed.stdout)

    def test_debian_repair_uses_interpreter_specific_venv_package(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fake_bin = Path(temp_dir) / "bin"
            fake_bin.mkdir()
            apt_get = fake_bin / "apt-get"
            apt_get.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            apt_get.chmod(0o755)
            log_path = Path(temp_dir) / "commands.log"

            completed = run_setup_functions(
                r"""
                uname() { printf '%s\n' 'Linux'; }
                python_version() { printf '%s' '3.14'; }
                run_privileged() { printf '%s\n' "$*" >> "${TEST_LOG}"; }
                install_linux_python_venv_support /fake/python
                """,
                environ={
                    "PATH": f"{fake_bin}:/usr/bin:/bin",
                    "TEST_LOG": str(log_path),
                },
            )

            commands = log_path.read_text(encoding="utf-8") if log_path.exists() else ""

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("apt-get update", commands)
        self.assertIn("apt-get install -y python3.14-venv", commands)

    def test_unrelated_venv_failure_does_not_install_system_packages(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            completed = run_setup_functions(
                r"""
                create_python_venv() {
                  printf '%s\n' 'Permission denied' >&2
                  return 1
                }
                install_linux_python_venv_support() {
                  : > "${TEST_ROOT}/unexpected-install"
                }
                VENV_DIR="${TEST_ROOT}/venv"
                RUNTIME_DIR="${TEST_ROOT}/runtime"
                PROFILE_FILE="${RUNTIME_DIR}/setup_profile"
                ensure_venv rocm /fake/python
                """,
                environ={"TEST_ROOT": temp_dir},
            )
            unexpected_install = Path(temp_dir, "unexpected-install").exists()

        self.assertNotEqual(completed.returncode, 0)
        self.assertFalse(unexpected_install)
        self.assertIn("Failed to create", completed.stderr)


if __name__ == "__main__":
    unittest.main()

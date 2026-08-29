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
    def test_supported_python_versions_match_ai_dependency_wheels(self) -> None:
        completed = run_setup_functions(
            r"""
            python_version_supported 3.11
            python_version_supported 3.12
            python_version_supported 3.13
            ! python_version_supported 3.10
            ! python_version_supported 3.14
            """,
            environ={},
        )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)

    def test_unsupported_system_python_selects_managed_python(self) -> None:
        completed = run_setup_functions(
            r"""
            python_version() {
              case "$1" in
                /system/python) printf '%s' '3.14' ;;
                /managed/python) printf '%s' '3.11' ;;
              esac
            }
            install_managed_python() {
              test "$1" = /system/python
              MANAGED_PYTHON_BIN=/managed/python
            }
            unset PYTHON
            select_supported_python /system/python
            test "${SUPPORTED_PYTHON_BIN}" = /managed/python
            """,
            environ={},
        )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("not supported", completed.stdout)

    def test_explicit_unsupported_python_fails_with_supported_range(self) -> None:
        completed = run_setup_functions(
            r"""
            python_version() { printf '%s' '3.14'; }
            select_supported_python /explicit/python
            """,
            environ={"PYTHON": "/explicit/python"},
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("Python 3.11, 3.12, or 3.13", completed.stderr)

    def test_managed_python_assets_are_pinned_for_linux_x86_64(self) -> None:
        completed = run_setup_functions(
            r"""
            uname() {
              case "$1" in
                -m) printf '%s' "${TEST_ARCH}" ;;
                *) printf '%s' Linux ;;
              esac
            }
            managed_python_asset
            printf '%s\n%s\n' "${MANAGED_PYTHON_URL}" "${MANAGED_PYTHON_SHA256}"
            """,
            environ={"TEST_ARCH": "x86_64"},
        )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("cpython-3.11.16%2B20260825-x86_64-unknown-linux-gnu", completed.stdout)
        self.assertIn("25844eb97cdc72cdc78addaad0969ce3b2133a4de54bfcfa4d57f8a6d095eaab", completed.stdout)

    def test_ryzen_ai_profile_can_select_managed_python_312(self) -> None:
        completed = run_setup_functions(
            r"""
            python_version() { printf '%s' '3.14'; }
            install_managed_python() {
              test "$1" = /system/python
              test "$2" = 3.12
              MANAGED_PYTHON_BIN=/managed/python3.12
            }
            unset PYTHON
            select_supported_python /system/python 3.12
            test "${SUPPORTED_PYTHON_BIN}" = /managed/python3.12

            configure_managed_python 3.12
            uname() {
              case "$1" in
                -m) printf '%s' x86_64 ;;
                *) printf '%s' Linux ;;
              esac
            }
            managed_python_asset
            printf '%s\n%s\n' "${MANAGED_PYTHON_URL}" "${MANAGED_PYTHON_SHA256}"
            """,
            environ={},
        )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("cpython-3.12.14%2B20260825-x86_64-unknown-linux-gnu", completed.stdout)
        self.assertIn("cbdd2f0cf02f941bc5c81e546f377275e322733abffe805ac29d2b7e8a58f7e3", completed.stdout)

    def test_missing_rocm_sdk_selects_vulkan_llama_backend(self) -> None:
        completed = run_setup_functions(
            r"""
            find_rocm_sdk_root() { return 0; }
            ensure_vulkan_build_dependencies() { :; }
            unset TATER_LLAMA_CPP_ROCM_BACKEND
            prepare_amd_llama_cpp_backend
            test "${TATER_LLAMA_CPP_ROCM_BACKEND_SELECTED}" = vulkan
            test "$(llama_cpp_native_cmake_args rocm)" = '-DGGML_VULKAN=on -DGGML_HIP=off'
            """,
            environ={},
        )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("Vulkan GPU acceleration", completed.stdout)

    def test_amd_ryzen_ai_stack_is_pinned_to_rocm_10(self) -> None:
        self.assertIn('AMD_RYZEN_AI_ROCM_VERSION="10.0.0"', SETUP_LIBRARY)
        self.assertIn('AMD_RYZEN_AI_TORCH_VERSION="2.13.0+rocm10.0.0"', SETUP_LIBRARY)
        self.assertIn('AMD_RYZEN_AI_TORCHVISION_VERSION="0.28.0+rocm10.0.0"', SETUP_LIBRARY)
        self.assertIn('AMD_RYZEN_AI_TORCHAUDIO_VERSION="2.11.0.2+rocm10.0.0"', SETUP_LIBRARY)
        self.assertIn("https://stable.repo.amd.com/rocm/whl-next/", SETUP_LIBRARY)

    def test_rocm_10_uses_architecture_specific_ryzen_ai_packages(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fake_python = Path(temp_dir) / "python"
            fake_python.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' \"$*\" > \"${TEST_LOG}\"\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o755)
            log_path = Path(temp_dir) / "pip.log"

            completed = run_setup_functions(
                r"""
                install_amd_ryzen_ai_pytorch "${TEST_PYTHON}"
                """,
                environ={
                    "TATER_ROCM_GFX_TARGET": "gfx1150",
                    "TEST_LOG": str(log_path),
                    "TEST_PYTHON": str(fake_python),
                },
            )
            command = log_path.read_text(encoding="utf-8") if log_path.exists() else ""

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("torch[device-gfx1150]==2.13.0+rocm10.0.0", command)
        self.assertIn("torchvision[device-gfx1150]==0.28.0+rocm10.0.0", command)
        self.assertIn("torchaudio==2.11.0.2+rocm10.0.0", command)

    def test_strix_halo_auto_selects_gfx1151(self) -> None:
        completed = run_setup_functions(
            r"""
            amd_rocm_gfx_target_from_tools() { :; }
            is_strix_halo_host() { return 0; }
            test "$(amd_ryzen_ai_gfx_target)" = gfx1151
            test "$(validated_rocm_gfx_target device-gfx1150)" = gfx1150
            """,
            environ={},
        )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)

    def test_rocm_upgrade_request_does_not_reuse_old_healthy_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            venv_dir = Path(temp_dir) / "venv"
            runtime_dir = Path(temp_dir) / "runtime"
            (venv_dir / "bin").mkdir(parents=True)
            runtime_dir.mkdir(parents=True)
            python = venv_dir / "bin" / "python"
            python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            python.chmod(0o755)
            (runtime_dir / "setup_profile").write_text("rocm\n", encoding="utf-8")

            completed = run_setup_functions(
                r"""
                rocm_torch_ready() { return 0; }
                VENV_DIR="${TEST_ROOT}/venv"
                RUNTIME_DIR="${TEST_ROOT}/runtime"
                PROFILE_FILE="${RUNTIME_DIR}/setup_profile"
                ! existing_rocm_environment_ready
                """,
                environ={
                    "TATER_SETUP_UPGRADE_ROCM": "1",
                    "TEST_ROOT": temp_dir,
                },
            )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)

    def test_missing_amd_gpu_groups_are_added_before_requesting_reboot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "privileged.log"
            completed = run_setup_functions(
                r"""
                id() {
                  case "$1" in
                    -Gn) printf '%s' 'phooey sudo' ;;
                    -un) printf '%s' 'phooey' ;;
                    *) command id "$@" ;;
                  esac
                }
                run_privileged() { printf '%s\n' "$*" > "${TEST_LOG}"; }
                ensure_amd_gpu_group_membership
                """,
                environ={
                    "TATER_SETUP_USER": "phooey",
                    "TEST_LOG": str(log_path),
                },
            )
            command = log_path.read_text(encoding="utf-8") if log_path.exists() else ""

        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(command.strip(), "usermod -a -G render,video phooey")
        self.assertIn("Reboot this machine", completed.stderr)

    def test_existing_amd_gpu_groups_only_require_a_fresh_login(self) -> None:
        completed = run_setup_functions(
            r"""
            id() {
              case "$1" in
                -Gn) printf '%s' 'phooey sudo video render' ;;
                -un) printf '%s' 'phooey' ;;
                *) command id "$@" ;;
              esac
            }
            run_privileged() { return 99; }
            ensure_amd_gpu_group_membership
            """,
            environ={"TATER_SETUP_USER": "phooey"},
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("login session does not have GPU access", completed.stderr)

    def test_existing_python_314_venv_is_rebuilt_automatically(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            venv_dir = Path(temp_dir) / "venv"
            (venv_dir / "bin").mkdir(parents=True)
            old_python = venv_dir / "bin" / "python"
            old_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            old_python.chmod(0o755)

            completed = run_setup_functions(
                r"""
                python_version() {
                  case "$1" in
                    *venv/bin/python) printf '%s' '3.14' ;;
                    *) printf '%s' '3.11' ;;
                  esac
                }
                create_python_venv() {
                  mkdir -p "${VENV_DIR}/bin"
                  printf '%s\n' '#!/bin/sh' 'exit 0' > "${VENV_DIR}/bin/python"
                  chmod +x "${VENV_DIR}/bin/python"
                }
                VENV_DIR="${TEST_ROOT}/venv"
                RUNTIME_DIR="${TEST_ROOT}/runtime"
                PROFILE_FILE="${RUNTIME_DIR}/setup_profile"
                ensure_venv rocm /managed/python
                """,
                environ={"TEST_ROOT": temp_dir},
            )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("unsupported Python 3.14", completed.stdout)

    def test_ryzen_ai_venv_is_rebuilt_with_required_python_minor(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            venv_dir = Path(temp_dir) / "venv"
            (venv_dir / "bin").mkdir(parents=True)
            old_python = venv_dir / "bin" / "python"
            old_python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            old_python.chmod(0o755)

            completed = run_setup_functions(
                r"""
                python_version() {
                  case "$1" in
                    *venv/bin/python) printf '%s' '3.11' ;;
                    *) printf '%s' '3.12' ;;
                  esac
                }
                create_python_venv() {
                  mkdir -p "${VENV_DIR}/bin"
                  printf '%s\n' '#!/bin/sh' 'exit 0' > "${VENV_DIR}/bin/python"
                  chmod +x "${VENV_DIR}/bin/python"
                }
                VENV_DIR="${TEST_ROOT}/venv"
                RUNTIME_DIR="${TEST_ROOT}/runtime"
                PROFILE_FILE="${RUNTIME_DIR}/setup_profile"
                ensure_venv rocm /managed/python3.12 3.12
                """,
                environ={"TEST_ROOT": temp_dir},
            )

        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertIn("Python 3.12 required by rocm", completed.stdout)

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

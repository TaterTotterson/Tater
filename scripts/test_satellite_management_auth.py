"""Keep WebUI login separate from the existing voice API authentication contract."""

from __future__ import annotations

import ast
import copy
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional
import unittest
from unittest import mock

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
import httpx

from tater_voice import native_satellite
from scripts import test_native_audio_scene_route as audio_scene_tests

ROOT = Path(__file__).resolve().parents[1]
APP_SOURCE = ROOT / "tater/web/application.py"
ROUTES_SOURCE = ROOT / "tater_voice/voice_pipeline/routes.py"
PIPELINE_SOURCE = ROOT / "tater_voice/voice_pipeline/__init__.py"
APP_TREE = ast.parse(APP_SOURCE.read_text())
ROUTES_TREE = ast.parse(ROUTES_SOURCE.read_text())
PIPELINE_TREE = ast.parse(PIPELINE_SOURCE.read_text())
COOKIE = "tater_webui_session"


def load_functions(tree, names, scope, filename):
    nodes = [
        copy.deepcopy(node)
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names
    ]
    for node in nodes:
        node.decorator_list = []
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    module = ast.fix_missing_locations(ast.Module(body=[future, *nodes], type_ignores=[]))
    exec(compile(module, str(filename), "exec"), scope)
    return scope


def auth_scope(settings):
    return load_functions(
        PIPELINE_TREE,
        {"_text", "_lower", "_as_bool", "_require_api_auth", "_require_configured_api_auth"},
        {
            "HTTPException": HTTPException,
            "_voice_settings": lambda: settings,
        },
        PIPELINE_SOURCE,
    )


def middleware_scope():
    return load_functions(
        APP_TREE,
        {"_webui_auth_middleware"},
        {
            "Request": Request,
            "JSONResponse": JSONResponse,
            "_is_spud_link_path": lambda path: path.startswith("/api/spudlink/"),
            "_is_spud_link_external_api_path": lambda path: False,
            "_webui_password_is_set": mock.Mock(return_value=True),
            "_webui_session_is_valid": mock.Mock(
                side_effect=lambda value: value == "valid-session"
            ),
            "WEBUI_AUTH_COOKIE_NAME": COOKIE,
            "RedisError": RuntimeError,
        },
        APP_SOURCE,
    )


class SatelliteAccessCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = {"API_AUTH_ENABLED": False, "API_AUTH_KEY": "existing-key"}
        self.guards = auth_scope(self.settings)
        self.middleware = middleware_scope()
        self.app = FastAPI()
        self.app.middleware("http")(self.middleware["_webui_auth_middleware"])
        self.vp = SimpleNamespace(
            _require_api_auth=self.guards["_require_api_auth"],
            _require_configured_api_auth=self.guards["_require_configured_api_auth"],
            _text=self.guards["_text"],
            __version__="test",
            _voice_config_snapshot=lambda: {},
            _voice_ui_setting_fields=lambda: {},
        )
        scope = load_functions(
            ROUTES_TREE,
            {
                "voice_config",
                "native_satellite_status",
                "native_satellite_command",
                "native_satellite_settings_save",
            },
            {
                "__package__": "tater_voice.voice_pipeline",
                "__name__": __name__,
                "_vp": lambda: self.vp,
                "HTTPException": HTTPException,
                "Header": Header,
                "Any": Any,
                "Dict": Dict,
                "Optional": Optional,
            },
            ROUTES_SOURCE,
        )
        self.status = mock.AsyncMock(return_value={"ok": True})
        self.command = mock.AsyncMock(return_value={"ok": True})
        self.save = mock.AsyncMock(return_value={"ok": True})
        for name, replacement in (
            ("status", self.status),
            ("send_command", self.command),
            ("save_live_settings", self.save),
        ):
            patcher = mock.patch.object(native_satellite, name, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.app.add_api_route(
            "/api/tater/satellite/v1/status", scope["native_satellite_status"], methods=["GET"]
        )
        self.app.add_api_route(
            "/api/tater/satellite/v1/command", scope["native_satellite_command"], methods=["POST"]
        )
        self.app.add_api_route(
            "/api/tater/satellite/v1/settings",
            scope["native_satellite_settings_save"],
            methods=["POST"],
        )
        self.app.add_api_route("/tater-ha/v1/voice/config", scope["voice_config"], methods=["GET"])

        @self.app.post("/api/settings/voice/runtime/action")
        async def webui_action():
            return {"ok": True}

        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://tater.test"
        )
        self.addAsyncCleanup(self.client.aclose)

    async def test_webui_password_does_not_override_disabled_voice_api_authentication(self):
        for path in ("/api/tater/satellite/v1/status", "/tater-ha/v1/voice/config"):
            self.assertEqual((await self.client.get(path)).status_code, 200)
        response = await self.client.post(
            "/api/tater/satellite/v1/command", json={"selector": "native:test", "type": "state"}
        )
        self.assertEqual(response.status_code, 200)
        self.command.assert_awaited_once()
        self.middleware["_webui_password_is_set"].assert_not_called()

    async def test_enabled_voice_api_authentication_rejects_missing_and_wrong_keys(self):
        self.settings["API_AUTH_ENABLED"] = True
        for path in ("/api/tater/satellite/v1/status", "/tater-ha/v1/voice/config"):
            self.assertEqual((await self.client.get(path)).status_code, 401)
            self.assertEqual(
                (await self.client.get(path, headers={"X-Tater-Token": "wrong"})).status_code, 401
            )
        self.assertEqual(
            (await self.client.post("/api/tater/satellite/v1/command", json={})).status_code, 401
        )
        self.status.assert_not_awaited()
        self.command.assert_not_awaited()

    async def test_existing_api_key_keeps_working_without_a_webui_cookie(self):
        self.settings["API_AUTH_ENABLED"] = True
        for path in ("/api/tater/satellite/v1/status", "/tater-ha/v1/voice/config"):
            self.assertEqual(
                (
                    await self.client.get(path, headers={"X-Tater-Token": "existing-key"})
                ).status_code,
                200,
            )
        response = await self.client.post(
            "/api/tater/satellite/v1/command", json={}, headers={"X-Tater-Token": "existing-key"}
        )
        self.assertEqual(response.status_code, 200)

    async def test_browser_login_does_not_bypass_enabled_api_key_authentication(self):
        self.settings["API_AUTH_ENABLED"] = True
        self.client.cookies.set(COOKIE, "valid-session")
        self.assertEqual((await self.client.get("/api/tater/satellite/v1/status")).status_code, 401)
        self.status.assert_not_awaited()

    async def test_existing_strict_settings_write_restriction_is_preserved(self):
        response = await self.client.post("/api/tater/satellite/v1/settings", json={})
        self.assertEqual(response.status_code, 403)
        self.save.assert_not_awaited()
        self.settings["API_AUTH_ENABLED"] = True
        self.assertEqual(
            (await self.client.post("/api/tater/satellite/v1/settings", json={})).status_code, 401
        )
        response = await self.client.post(
            "/api/tater/satellite/v1/settings", json={}, headers={"X-Tater-Token": "existing-key"}
        )
        self.assertEqual(response.status_code, 200)
        self.save.assert_awaited_once()

    async def test_webui_settings_still_require_browser_login_independently(self):
        response = await self.client.post(
            "/api/settings/voice/runtime/action", headers={"X-Tater-Token": "existing-key"}
        )
        self.assertEqual(response.status_code, 401)
        self.client.cookies.set(COOKIE, "valid-session")
        self.assertEqual(
            (await self.client.post("/api/settings/voice/runtime/action")).status_code, 200
        )

    def test_every_existing_native_and_legacy_control_handler_retains_its_api_guard(self):
        public = {
            "native_satellite_ws",
            "native_satellite_firmware_artifact",
            "native_satellite_wake_package_manifest",
            "native_satellite_wake_package_model",
            "native_satellite_wake_package_bundle",
            "native_satellite_oww_metadata",
            "native_satellite_oww_model",
            "native_satellite_nww_model",
            "wake_package_boundary_model",
            "native_satellite_display_photo",
            "linked_trainer_wake_word_save",
            "linked_trainer_claim",
            "linked_trainer_unlink",
            "native_tts_stream",
            "native_media_stream",
        }
        checked = 0
        for node in ROUTES_TREE.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name in public:
                continue
            paths = [
                d.args[0].value
                for d in node.decorator_list
                if isinstance(d, ast.Call)
                and d.args
                and isinstance(d.args[0], ast.Constant)
                and isinstance(d.args[0].value, str)
            ]
            if not any(
                path.startswith(("/api/tater/satellite/", "/tater-ha/v1/voice/")) for path in paths
            ):
                continue
            checked += 1
            guarded = any(
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr in {"_require_api_auth", "_require_configured_api_auth"}
                for call in ast.walk(node)
            )
            self.assertTrue(guarded, node.name)
        self.assertGreaterEqual(checked, 25)


class InternalAudioHTTPCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_playback_routes_work_with_password_enabled_in_both_api_modes(self):
        fixture = audio_scene_tests.NativeAudioSceneRouteTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        # Isolate unrelated speech defaults; this test must not read live Redis
        # or import model/runtime settings while an HTTP request is executing.
        fixture.routes._native_ducking_payload = lambda values: dict(values or {})
        settings = {"API_AUTH_ENABLED": False, "API_AUTH_KEY": "existing-key"}
        guards = auth_scope(settings)
        fixture.vp._require_api_auth = guards["_require_api_auth"]
        middleware = middleware_scope()
        app = FastAPI()
        app.middleware("http")(middleware["_webui_auth_middleware"])

        @app.post("/api/tater/satellite/v1/play")
        async def play(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)):
            return await fixture.routes.native_satellite_play(payload, x_tater_token)

        @app.post("/api/tater/satellite/v1/play-group")
        async def play_group(payload: Dict[str, Any], x_tater_token: Optional[str] = Header(None)):
            return await fixture.routes.native_satellite_play_group(payload, x_tater_token)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            for enabled in (False, True):
                settings["API_AUTH_ENABLED"] = enabled
                headers = {"X-Tater-Token": "existing-key"} if enabled else {}
                payload = fixture._payload()
                self.assertEqual(
                    (
                        await client.post(
                            "/api/tater/satellite/v1/play", json=payload, headers=headers
                        )
                    ).status_code,
                    200,
                )
                payload["selectors"] = ["native:kitchen", "native:office"]
                self.assertEqual(
                    (
                        await client.post(
                            "/api/tater/satellite/v1/play-group", json=payload, headers=headers
                        )
                    ).status_code,
                    200,
                )
        middleware["_webui_password_is_set"].assert_not_called()
        self.assertTrue(fixture.commands or fixture.single_overlay_calls or fixture.sendspin_calls)


if __name__ == "__main__":
    unittest.main()

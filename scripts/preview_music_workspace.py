#!/usr/bin/env python3
"""Serve the built Music UI with the real Core and isolated sample data.

Run after `npm run build --prefix frontend`, then run
`node scripts/test_music_workspace_browser.mjs http://127.0.0.1:4179`.
Tater_Shop is expected beside Tater, or pass --shop-path. No live Redis,
provider, satellite, or user settings are used. Restart to reset sample state.
"""

import argparse
from contextlib import ExitStack
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

REPO = Path(__file__).resolve().parents[1]
INDEX = """<!doctype html><html lang="en" data-theme="tater">
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Music workspace — isolated sample</title><link rel="stylesheet" href="/ui/tater-ui.css">
<style>
*,*::before,*::after{box-sizing:border-box}
html,body{margin:0;background:var(--bg);color:var(--text);font:13px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
#app{max-width:1100px;margin:0 auto;padding:25px 28px}
button,input,select{font:inherit}h1{font-size:26px;font-weight:500}
.top{border-bottom:1px solid var(--line);padding:14px 28px;display:flex;gap:18px}
.top b{color:var(--accent);font:italic 500 24px Georgia,serif}.top span{margin-left:auto;color:var(--muted)}
</style><div class="top"><b>Tater</b><span>Isolated UI test · Sample library</span></div>
<div id="app"></div><script type="module">
import {mountMusicCore} from '/ui/tater-ui.js';
const payload=await(await fetch('/tab')).json();
window.musicController=mountMusicCore(document.getElementById('app'),{
 initialPayload:payload,coreKey:'music_core',tabEndpoint:'/tab',actionEndpoint:'/action',eventsEndpoint:'/events'
});
</script></html>"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shop-path", type=Path, default=REPO.parent / "Tater_Shop")
    parser.add_argument("--port", type=int, default=4179)
    args = parser.parse_args()
    if not (args.shop_path / "tests/test_music_core.py").is_file():
        parser.error("Pass --shop-path pointing to the Tater_Shop checkout.")
    if not (REPO / "tateros_static/ui/tater-ui.js").is_file():
        parser.error("Build the frontend first.")

    with tempfile.TemporaryDirectory(
        prefix="tater-music-preview-"
    ) as temporary, ExitStack() as patches:
        root = Path(temporary)
        os.environ.update(
            TATER_RUNTIME_DIR=str(root / "runtime"),
            TATER_AGENT_ROOT=str(root / "agent"),
            TATER_REDIS_USE_UNIX_SOCKET="false",
            TATER_REDIS_AOF_AUTO_COMPACT="0",
        )
        # Shop's existing test helper imports the Core with an in-memory Redis.
        sys.path.insert(0, str(args.shop_path / "tests"))
        from test_music_core import MusicCoreTests

        MusicCoreTests.setUpClass()
        fixture = MusicCoreTests()
        fixture.setUp()
        core, redis = fixture.core, fixture.redis
        tracks = fixture.tracks + [
            {
                **fixture.tracks[0],
                "id": "track:three",
                "title": "Is This Love",
                "track_number": 2,
                "duration_seconds": 228,
            },
            {
                **fixture.tracks[1],
                "id": "track:four",
                "title": "So What",
                "track_number": 1,
                "duration_seconds": 545,
            },
        ]
        for track in tracks:
            duration = int(track["duration_seconds"])
            track["duration_display"] = f"{duration // 60}:{duration % 60:02}"
            track["artwork_path"] = "artwork/" + track["id"].replace(":", "-") + ".jpg"
        targets = [
            {
                "value": "voice_core:stereo:office",
                "label": "Tater Stereo: Office Stereo (Office • ready)",
                "pair_members": [
                    {
                        "selector": "native:left",
                        "label": "Left ThirdReality",
                        "channel": "left",
                    },
                    {
                        "selector": "native:right",
                        "label": "Right ThirdReality",
                        "channel": "right",
                    },
                ],
            },
            {
                "value": "voice_core:native:checkers",
                "label": "Tater Satellite: Checkers (Office) • online",
            },
            {
                "value": "voice_core:stereo:master",
                "label": "Tater Stereo: Stereo Master (Master bedroom • ready)",
                "pair_members": [
                    {
                        "selector": "native:masterleft",
                        "label": "Biscuit Left",
                        "channel": "left",
                    },
                    {
                        "selector": "native:masterright",
                        "label": "Biscuit Right",
                        "channel": "right",
                    },
                ],
            },
        ]
        redis.set(
            core.CATALOG_KEY,
            json.dumps(
                {
                    "tracks": tracks,
                    "artists": ["Bob Marley & The Wailers", "Miles Davis"],
                    "albums": ["Exodus", "Kind of Blue"],
                    "genres": ["Reggae", "Roots Reggae", "Jazz"],
                    "synced_at": time.time(),
                }
            ),
        )
        redis.hset(
            core.SETTINGS_KEY,
            mapping={
                "default_targets": json.dumps([targets[0]["value"]]),
                "default_volume_percent": "52",
            },
        )
        redis.set(
            core.PLAYER_KEY,
            json.dumps(
                {
                    "status": "playing",
                    "current": tracks[0],
                    "queue": tracks,
                    "index": 0,
                    "targets": [targets[0]["value"]],
                    "volume_percent": 52,
                    "position_offset_seconds": 0,
                    "started_at": time.time() - 84,
                    "duration_seconds": 180,
                    "continuous_radio": True,
                }
            ),
        )
        redis.set(
            core.HISTORY_KEY,
            json.dumps(
                [
                    {
                        "track_id": row["id"],
                        "provider": "tater_tube",
                        "played_at": time.time(),
                    }
                    for row in tracks[:2]
                ]
            ),
        )
        for name, replacement in {
            "_target_options": patch.object(
                core, "_target_options", return_value=targets
            ),
            "_reconcile_native_playback": patch.object(
                core,
                "_reconcile_native_playback",
                side_effect=lambda player, *_: player,
            ),
            "_play_track": patch.object(
                core, "_play_track", return_value={"ok": True, "sent_count": 1}
            ),
            "_stop_target": patch.object(core, "_stop_target", return_value=[]),
            "_set_target_volume": patch.object(
                core,
                "_set_target_volume",
                return_value={"sent_count": 1, "warnings": []},
            ),
            "_require_native_seek_support": patch.object(
                core, "_require_native_seek_support"
            ),
            "_validate_catalog_provider_targets": patch.object(
                core, "_validate_catalog_provider_targets"
            ),
            "_publish_tater_tube_activity": patch.object(
                core, "_publish_tater_tube_activity"
            ),
        }.items():
            patches.enter_context(replacement)
        (root / "ui").symlink_to(REPO / "tateros_static/ui", target_is_directory=True)
        (root / "index.html").write_text(INDEX)

        class Handler(SimpleHTTPRequestHandler):
            def __init__(self, *arguments, **keywords):
                super().__init__(*arguments, directory=str(root), **keywords)

            def log_message(self, *_):
                pass

            def send_body(self, body, kind, status=200):
                self.send_response(status)
                self.send_header("Content-Type", kind)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def send_json(self, data, status=200):
                self.send_body(json.dumps(data).encode(), "application/json", status)

            def do_GET(self):
                if self.path == "/tab":
                    return self.send_json(core.get_htmlui_tab_data(redis_client=redis))
                if self.path.startswith("/api/cores/music_core/webhook/artwork"):
                    track_id = parse_qs(urlparse(self.path).query).get(
                        "track_id", [""]
                    )[0]
                    warm = track_id in {"track:one", "track:three"}
                    color, title = (
                        ("#b87443", "EXODUS") if warm else ("#335e60", "KIND OF BLUE")
                    )
                    art = f'<svg xmlns="http://www.w3.org/2000/svg" width="400" height="400"><rect width="400" height="400" fill="{color}"/><circle cx="320" cy="65" r="140" fill="#e7bc7c" opacity=".5"/><circle cx="55" cy="345" r="220" fill="none" stroke="#fff" opacity=".2"/><text x="32" y="70" fill="#fff" font-family="Georgia" font-size="24">{title}</text></svg>'
                    return self.send_body(art.encode(), "image/svg+xml")
                if self.path == "/events":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    try:
                        for _ in range(120):
                            event = json.dumps(
                                core.get_htmlui_tab_data(redis_client=redis)
                            )
                            self.wfile.write(
                                f"event: core-tab\ndata: {event}\n\n".encode()
                            )
                            self.wfile.flush()
                            time.sleep(1)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return
                return super().do_GET()

            def do_POST(self):
                if self.path != "/action":
                    return self.send_error(404)
                try:
                    data = json.loads(
                        self.rfile.read(int(self.headers.get("Content-Length", "0")))
                    )
                    result = core.handle_htmlui_tab_action(
                        action=data["action"],
                        payload=data.get("payload", {}),
                        redis_client=redis,
                    )
                    self.send_json(result)
                except Exception as error:
                    self.send_json({"detail": str(error)}, 400)

        with ThreadingHTTPServer(("127.0.0.1", args.port), Handler) as server:
            print(f"Isolated Music workspace: http://127.0.0.1:{args.port}", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()

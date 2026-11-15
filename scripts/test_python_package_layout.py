"""Behavioral contracts for the package migration and legacy extension API."""
from __future__ import annotations

import importlib.util
import json
import os
import plistlib
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class PythonPackageLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if any(importlib.util.find_spec(name) is None for name in ('fastapi', 'redis', 'openai')):
            raise unittest.SkipTest('Application runtime dependencies are required.')
        executable = shutil.which('redis-server')
        if not executable:
            raise unittest.SkipTest('A redis-server executable is required for isolated runtime checks.')
        cls.temp = tempfile.TemporaryDirectory(prefix='tater-layout-', dir='/tmp')
        cls.directory = Path(cls.temp.name)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        cls.server = subprocess.Popen(
            [executable, '--bind', '127.0.0.1', '--port', str(port), '--save', '',
             '--appendonly', 'no', '--dir', str(cls.directory)],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        )
        import redis
        client = redis.Redis(host='127.0.0.1', port=port, socket_connect_timeout=.2)
        for _ in range(50):
            try:
                if client.ping():
                    break
            except redis.RedisError:
                time.sleep(.05)
        else:
            cls.server.terminate()
            cls.server.wait(timeout=5)
            cls.temp.cleanup()
            raise RuntimeError('Isolated Redis did not start.')
        client.close()
        config = cls.directory / 'redis.json'
        config.write_text(json.dumps({'mode': 'external', 'host': '127.0.0.1', 'port': port}))
        cls.environment = dict(
            os.environ, PYTHONPATH=str(ROOT), TATER_REDIS_MODE='external',
            TATER_REDIS_CONFIG_PATH=str(config), TATER_RUNTIME_DIR=str(cls.directory / 'runtime'),
            TATER_AGENT_ROOT=str(cls.directory / 'agent'),
        )

    @classmethod
    def tearDownClass(cls):
        cls.server.terminate()
        cls.server.wait(timeout=5)
        cls.temp.cleanup()

    def python(self, *arguments, input=None, cwd=None):
        try:
            result = subprocess.run(
                [sys.executable, *arguments], input=input, text=True, capture_output=True,
                env=self.environment, cwd=cwd or ROOT, timeout=45,
            )
        except subprocess.TimeoutExpired as error:
            output = error.stderr or error.stdout or b''
            self.fail('Child process timed out: ' + (output.decode(errors='replace') if isinstance(output, bytes) else output))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def test_imports_share_state_in_both_orders(self):
        for canonical_first in (False, True):
            with self.subTest(canonical_first=canonical_first):
                self.python('-c', f'''
import importlib
from tater._compat import LEGACY_MODULES
for legacy, canonical in LEGACY_MODULES.items():
    first, second = (canonical, legacy) if {canonical_first!r} else (legacy, canonical)
    a, b = importlib.import_module(first), importlib.import_module(second)
    assert a is b, (first, second)
    marker = object()
    a._layout_test_marker = marker
    assert b._layout_test_marker is marker
    del b._layout_test_marker
import tateros_app
from tater.web import schemas
for name, value in vars(schemas).items():
    if isinstance(value, type) and value.__module__ == schemas.__name__:
        assert getattr(tateros_app, name) is value, name
''')

    def test_worker_launchers_work_outside_the_source_directory(self):
        for filename in ('face_id_worker.py', 'managed_tts_worker.py'):
            with self.subTest(filename=filename):
                result = self.python(str(ROOT / filename), '--help', cwd=self.directory)
                self.assertIn('usage:', result.stdout)
        result = self.python(
            str(ROOT / 'helpers.py'), '--tater-llama-cpp-engine-worker',
            input='{"id":"layout-check","op":"shutdown"}\n', cwd=self.directory,
        )
        response = json.loads(next(line for line in result.stdout.splitlines() if 'layout-check' in line))
        self.assertTrue(response['ok'])
        self.assertTrue(response['result']['shutdown'])

    def test_http_and_asset_paths_use_the_application_root(self):
        self.python('-c', '''
import faulthandler
faulthandler.dump_traceback_later(12)
import asyncio
from pathlib import Path
import httpx
import tateros_app as legacy
from tater.web import application
from tater_paths import APP_ROOT
from tater.extensions.surfaces import SurfaceRuntimeManager
from tater.identity.runtime import _worker_path
assert legacy is application
assert application.STATIC_DIR == APP_ROOT / 'tateros_static'
assert application.DEFAULT_TATER_AVATAR_PATH == APP_ROOT / 'images' / 'tater.png'
assert application.STATIC_DIR.is_dir()
assert _worker_path() == APP_ROOT / 'face_id_worker.py'
manager = SurfaceRuntimeManager(kind='core', package_name='cores', env_name='LAYOUT_TEST_CORE_DIR', default_subdir='cores')
assert manager.surface_dir == (APP_ROOT / 'cores').resolve()
async def check():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application.app), base_url='http://test') as client:
        response = await client.get('/api/auth/status')
        assert response.status_code == 200, response.text
        response = await client.get('/')
        assert response.status_code == 200, response.text
        assert 'text/html' in response.headers['content-type']
with asyncio.Runner() as runner:
    runner.run(check())
faulthandler.cancel_dump_traceback_later()
''', cwd=self.directory)

    def test_new_top_level_package_does_not_start_services(self):
        self.python('-c', '''
import sys
import tater
assert 'helpers' not in sys.modules
assert 'redis_runtime' not in sys.modules
assert 'tateros_app' not in sys.modules
''')

    def test_packaged_source_keeps_bundle_and_worker_paths(self):
        contents = self.directory / 'Tater.app' / 'Contents'
        snapshot = contents / 'Resources' / 'TaterSource'
        snapshot.mkdir(parents=True)
        shutil.copytree(ROOT / 'tater', snapshot / 'tater', ignore=shutil.ignore_patterns('__pycache__'))
        for path in ROOT.glob('*.py'):
            shutil.copy2(path, snapshot / path.name)
        with (contents / 'Info.plist').open('wb') as handle:
            plistlib.dump({'CFBundleShortVersionString': '123.4'}, handle)
        environment = dict(self.environment, PYTHONPATH=str(snapshot))
        environment.pop('TATER_APP_VERSION', None)
        environment.pop('TATER_INFO_PLIST', None)
        result = subprocess.run([sys.executable, '-c', '''
from pathlib import Path
import helpers
import tater.llm.runtime
from tater_paths import APP_ROOT
from tater_version import resolve_tater_version
from tater.identity.runtime import _worker_path
assert helpers is tater.llm.runtime
assert APP_ROOT == Path.cwd()
assert resolve_tater_version() == '123.4'
assert _worker_path() == APP_ROOT / 'face_id_worker.py'
assert _worker_path().is_file()
assert (APP_ROOT / 'managed_tts_worker.py').is_file()
'''], cwd=snapshot, env=environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()

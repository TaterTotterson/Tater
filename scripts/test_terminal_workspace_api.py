"""Exercise the real Terminal routes without starting Tater's model/hardware services."""
from __future__ import annotations
import ast
import asyncio
from contextlib import asynccontextmanager
import json
import logging
from pathlib import Path
import time
from typing import Any, Dict, List, Optional
import unittest
from unittest import mock

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
import httpx
from pydantic import BaseModel, Field
from scripts import test_spudex_runtime as fixtures
from spudex import runner, settings, workspace, terminal
from tater.web import schemas


def route_app(redis, llm_factory):
    app = FastAPI()
    scope = dict(globals(), app=app, redis_client=redis, get_llm_client_from_env=llm_factory,
                 logger=logging.getLogger(__name__), _spudex_platform_options=lambda *_: [])
    scope.update({name: value for name, value in vars(schemas).items() if name.startswith('Spudex') and name.endswith('Request')})
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'tater/web/application.py').read_text())
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name.startswith('Spudex') and node.name.endswith('Request'):
            nodes.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
            isinstance(d, ast.Call) and d.args and isinstance(d.args[0], ast.Constant)
            and str(d.args[0].value).startswith('/api/spudex') for d in node.decorator_list
        ):
            nodes.append(node)
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), 'terminal_routes', 'exec'), scope)
    return app


class TerminalWorkspaceApiTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.SpudexRuntimeTests.setUp
    tearDown = fixtures.SpudexRuntimeTests.tearDown
    asyncTearDown = fixtures.SpudexRuntimeTests.asyncTearDown

    def client(self, *, model=None):
        redis = fixtures._FakeRedis({'enabled': True, 'full_access': True, 'default_cwd': str(self.workspace), 'llm_model': 'fixture-model', 'allowed_platforms': ['webui', 'saved-platform'], 'max_task_steps': 2})
        redis.set = lambda key, value: setattr(redis, 'values', json.loads(value))
        self.redis = redis
        @asynccontextmanager
        async def factory(**kwargs):
            yield model or fixtures._SequenceLlm(json.dumps({'type':'reply','message':'Answer','outcome':'answer'}))
        app = route_app(redis, factory)
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://fixture')

    async def test_draft_and_followup_keep_selected_project_goal_and_mode(self):
        project = workspace.add_project(str(self.workspace), 'Selected project')
        async with self.client() as client:
            response = await client.post('/api/spudex/chat/session', json={'project_id': project['id'], 'mode': 'ask'})
            self.assertEqual(response.status_code,200)
            sid = response.json()['session']['id']
            response = await client.post('/api/spudex/chat', json={'session_id':sid,'message':'Explain the project','project_id':project['id'],'mode':'ask'})
            self.assertEqual(response.status_code,200)
            await asyncio.gather(*list(runner._ACTIVE_TASKS.values()))
            saved = runner.get_spudex_session(sid)
            self.assertEqual(saved['goal'],'Explain the project');self.assertEqual(saved['mode'],'ask')
            self.assertEqual(Path(saved['cwd']),self.workspace.resolve())
            self.assertEqual(saved['status'],'succeeded')

    async def test_invalid_requests_are_rejected_without_starting_tasks(self):
        async with self.client() as client:
            for payload in ({'mode':'invalid'}, {'project_id':'missing'}):
                response=await client.post('/api/spudex/chat/session',json=payload)
                self.assertEqual(response.status_code,400)
            response=await client.post('/api/spudex/chat',json={'session_id':'missing','message':'Hello'})
            self.assertEqual(response.status_code,400)
            response=await client.get('/api/spudex/sessions/missing/events')
            self.assertEqual(response.status_code,404)
        self.assertFalse(runner._ACTIVE_TASKS)

    async def test_duplicate_task_start_is_a_conflict(self):
        class SlowModel:
            async def chat(self,**kwargs):
                await asyncio.sleep(30)
        async with self.client(model=SlowModel()) as client:
            first=await client.post('/api/spudex/chat',json={'message':'Explain','mode':'ask'})
            sid=first.json()['session']['id']
            response=await client.post('/api/spudex/chat',json={'message':'Duplicate','session_id':sid})
            self.assertEqual(response.status_code,409)
            await client.post(f'/api/spudex/sessions/{sid}/stop')
        self.assertEqual(runner.get_spudex_session(sid)['status'],'stopped')

    async def test_settings_save_retains_hidden_fields_and_saved_platforms(self):
        async with self.client() as client:
            response=await client.post('/api/spudex/settings',json={'values':{'command_timeout_sec':61}})
            saved=response.json()['settings']
            self.assertEqual(saved['command_timeout_sec'],61)
            self.assertEqual(saved['llm_model'],'fixture-model')
            self.assertEqual(saved['allowed_platforms'],['webui','saved-platform'])
            self.assertEqual(saved['default_cwd'],str(self.workspace))
            self.assertTrue(saved['full_access'])

    async def test_restricted_shell_and_oversized_input_are_rejected(self):
        async with self.client() as client:
            await client.post('/api/spudex/settings',json={'values':{'full_access':False}})
            response=await client.post('/api/spudex/terminal',json={})
            self.assertEqual(response.status_code,403)
            response=await client.post('/api/spudex/terminal/missing/input',json={'data':'x'*16385})
            self.assertEqual(response.status_code,422)
        self.assertFalse(terminal._TERMINALS)

if __name__=='__main__':unittest.main()

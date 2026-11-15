from __future__ import annotations
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest import mock
import unittest

from scripts import test_spudex_runtime as fixtures
_SequenceLlm = fixtures._SequenceLlm
from spudex import runner, chat_loop, workspace, terminal
from spudex.events import session_events


class TerminalWorkspaceTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.SpudexRuntimeTests.setUp
    tearDown = fixtures.SpudexRuntimeTests.tearDown
    asyncTearDown = fixtures.SpudexRuntimeTests.asyncTearDown
    _redis = staticmethod(fixtures.SpudexRuntimeTests._redis)
    _session = fixtures.SpudexRuntimeTests._session

    def test_recent_context_keeps_latest_followup_after_long_history(self):
        sid = self._session()
        for i in range(500):
            runner.append_session_log(sid, stream='stdout', text=f'Old command {i}')
        runner.append_session_log(sid, stream='user', text='Use my newest correction, not the first plan.')
        context = chat_loop._recent_session_context(sid)
        self.assertEqual(context[-1]['text'], 'Use my newest correction, not the first plan.')
        self.assertNotIn('Old command 0', str(context))

    def test_log_tail_pagination_and_forward_cursor(self):
        sid = self._session()
        for i in range(25): runner.append_session_log(sid, stream='user', text=str(i))
        page = runner.read_spudex_logs(sid, tail=True, limit=10)
        self.assertEqual([row['seq'] for row in page['entries']], list(range(16,26)))
        self.assertTrue(page['has_more'])
        older = runner.read_spudex_logs(sid, tail=True, before_seq=page['first_seq'], limit=20)
        self.assertEqual([row['seq'] for row in older['entries']], list(range(1,16)))
        self.assertFalse(older['has_more'])
        self.assertEqual(runner.read_spudex_logs(sid, after_seq=24)['entries'][0]['seq'],25)

    def test_projects_tasks_and_legacy_folders_remain_attached(self):
        one = workspace.add_project(str(self.workspace), 'First project')
        other = self.root / 'other'; other.mkdir()
        two = workspace.add_project(str(other), 'Other project')
        first, _ = workspace.prepare_task(message='Fix the component',project_id=one['id'],mode='plan')
        runner.update_spudex_session(first['id'],status='incomplete',memory_summary='Keep the user draft.',plan=[{'step':'Inspect','status':'completed'},{'step':'Edit','status':'failed'}])
        followup, message = workspace.prepare_task(message='Use the latest correction',session_id=first['id'],project_id=one['id'])
        self.assertEqual(followup['goal'],'Fix the component')
        self.assertIsNone(followup.get('verification'))
        resumed, message = workspace.prepare_task(message='',session_id=first['id'],resume=True,default_cwd=str(other))
        self.assertEqual(message,'Use the latest correction')
        self.assertEqual(Path(resumed['cwd']),self.workspace.resolve())
        self.assertEqual(resumed['mode'],'plan')
        self.assertEqual(resumed['memory_summary'],'Keep the user draft.')
        with self.assertRaisesRegex(ValueError,'stays attached'):
            workspace.prepare_task(message='Move it',session_id=first['id'],project_id=two['id'])
        legacy = runner.create_spudex_session(label='Legacy',cwd=str(other),source='spudex_chat')
        self.assertEqual(Path(workspace.session_project(legacy)['path']),other.resolve())
        self.assertIn(one['id'],[p['id'] for p in workspace.list_projects()])

    async def test_modes_block_project_writes_commands_verification_and_network(self):
        for mode in ('ask','plan'):
            sid = self._session(mode)
            model = _SequenceLlm(
                json.dumps({'type':'write_file','path':'forbidden.txt','content':'no'}),
                json.dumps({'type':'command','argv':['touch','forbidden.txt']}),
                json.dumps({'type':'verify','argv':['touch','forbidden.txt']}),
                json.dumps({'type':'search','query':'do not contact the network'}),
                json.dumps({'type':'reply','outcome':'answer','message':'No files were changed.','plan':[{'step':'Implement later','status':'completed'}]}),
            )
            with mock.patch.object(chat_loop,'run_argv_in_session',new_callable=mock.AsyncMock) as command, mock.patch.object(chat_loop,'write_spudex_file_in_session') as write, mock.patch.object(chat_loop,'research_web',new_callable=mock.AsyncMock) as network:
                result = await chat_loop.run_spudex_chat_turn(session_id=sid,message='Describe a fix',platform='webui',llm_client=model,redis_client=self._redis(max_task_steps=5),mode=mode)
            self.assertTrue(result['ok']);command.assert_not_awaited();write.assert_not_called();network.assert_not_awaited()
            self.assertFalse((self.workspace/'forbidden.txt').exists())
            if mode=='plan': self.assertEqual(runner.get_spudex_session(sid)['plan'][0]['status'],'pending')

    async def test_read_only_inspection_and_malformed_reply_repair_are_enforced(self):
        (self.workspace/'existing.txt').write_text('actual file contents')
        sid = self._session()
        model = _SequenceLlm('bad JSON', json.dumps({'type':'write_file','path':'no.txt','content':'forbidden'}), json.dumps({'type':'read_file','path':'existing.txt'}), json.dumps({'type':'reply','outcome':'answer','message':'Inspected the file.'}))
        result = await chat_loop.run_spudex_chat_turn(session_id=sid,message='Inspect this file',platform='webui',llm_client=model,redis_client=self._redis(max_task_steps=3),mode='ask')
        self.assertTrue(result['ok']);self.assertFalse((self.workspace/'no.txt').exists())
        self.assertIn('actual file contents',model.calls[-1]['messages'][1]['content'])

    async def test_build_edits_and_verifies_in_the_saved_project(self):
        sid = self._session()
        check=[sys.executable,'-c',"from pathlib import Path; assert Path('result.txt').read_text()=='potato'"]
        model = _SequenceLlm(json.dumps({'type':'write_file','path':'result.txt','content':'potato'}),json.dumps({'type':'verify','argv':check}),json.dumps({'type':'reply','outcome':'completed','message':'Built and verified.'}))
        result = await chat_loop.run_spudex_chat_turn(session_id=sid,message='Write and test a file',platform='webui',llm_client=model,redis_client=self._redis(max_task_steps=3),mode='build')
        self.assertTrue(result['ok']);self.assertEqual((self.workspace/'result.txt').read_text(),'potato')
        self.assertEqual(runner.get_spudex_session(sid)['verification']['status'],'passed')

    def test_inspection_cannot_escape_through_parent_or_symlink(self):
        outside=self.host_root/'outside.txt';outside.write_text('outside')
        (self.workspace/'link').symlink_to(outside)
        project=workspace.project_row(self.workspace)
        self.assertFalse(workspace.inspect_project(project,'read_file',path='../outside.txt')['ok'])
        self.assertFalse(workspace.inspect_project(project,'read_file',path='link')['ok'])
        self.assertNotIn('link',workspace.inspect_project(project,'list_files')['results'])
        (self.workspace/'large.txt').write_text('x'*60000)
        result=workspace.inspect_project(project,'read_file',path='large.txt')
        self.assertTrue(result['truncated']);self.assertEqual(len(result['content']),48000)

    def test_project_changes_include_shell_edits_untracked_and_unusual_paths(self):
        def git(*args):return subprocess.run(['git','-c','core.hooksPath=/dev/null','-C',str(self.workspace),*args],capture_output=True,text=True,check=True)
        git('init');(self.workspace/'original.txt').write_text('before\n');git('add','original.txt');git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','fixture')
        (self.workspace/'original.txt').write_text('after\n');(self.workspace/'new file\nname.txt').write_text('new\n')
        result=workspace.project_changes(workspace.project_row(self.workspace))
        self.assertTrue(result['ok']);self.assertEqual({row['name'] for row in result['files']},{'original.txt','new file\nname.txt'})
        self.assertIn('+after',next(row['diff'] for row in result['files'] if row['name']=='original.txt'))
        (self.workspace/'large.txt').write_bytes(b'x'*600000)
        for i in range(65):(self.workspace/f'extra-{i}.txt').write_text('new')
        bounded=workspace.project_changes(workspace.project_row(self.workspace))
        self.assertEqual(len(bounded['files']),60);self.assertTrue(bounded['truncated'])
        self.assertEqual(bounded['total_count'],68)


    async def test_active_task_cannot_be_started_twice(self):
        session,_=workspace.prepare_task(message='Original task')
        active=asyncio.create_task(asyncio.sleep(30));runner.register_spudex_task(session['id'],active)
        with self.assertRaisesRegex(RuntimeError,'already running'):workspace.prepare_task(message='Duplicate',session_id=session['id'])
        active.cancel();await asyncio.gather(active,return_exceptions=True)

    async def test_live_events_replay_from_last_event_id_without_duplicate_logs(self):
        sid=self._session();runner.append_session_log(sid,stream='user',text='one');runner.append_session_log(sid,stream='assistant',text='two')
        class Request:
            headers={'last-event-id':'1'}
            async def is_disconnected(self):return False
        stream=session_events(Request(),sid)
        first=await anext(stream);data=json.loads(first.split('data: ',1)[1]);self.assertEqual([row['seq'] for row in data['entries']],[2])
        runner.append_session_log(sid,stream='system',text='three')
        second=await anext(stream);data=json.loads(second.split('data: ',1)[1]);self.assertEqual([row['seq'] for row in data['entries']],[3])
        await stream.aclose()

    async def test_live_events_emit_runtime_completion_without_new_logs(self):
        sid=self._session()
        active=asyncio.create_task(asyncio.sleep(30));runner.register_spudex_task(sid,active)
        class Request:
            headers={}
            async def is_disconnected(self):return False
        stream=session_events(Request(),sid)
        first=json.loads((await anext(stream)).split('data: ',1)[1])
        self.assertTrue(first['session']['active'])
        active.cancel();await asyncio.gather(active,return_exceptions=True)
        second=json.loads((await asyncio.wait_for(anext(stream),1)).split('data: ',1)[1])
        self.assertFalse(second['session']['active']);self.assertFalse(second['entries'])
        await stream.aclose()

    @unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_spawn'),'POSIX terminals')
    async def test_interactive_setup_failure_cleans_up_spawned_shell(self):
        with mock.patch.dict(os.environ,{'PATH':'/usr/bin:/bin','HOME':str(self.host_root),'SHELL':'/bin/sh'},clear=True), mock.patch.object(terminal,'resize_terminal',side_effect=OSError('fixture setup failure')):
            with self.assertRaisesRegex(OSError,'fixture setup failure'):
                await terminal.start_terminal(redis_client=self._redis(full_access=True))
        self.assertFalse(terminal._TERMINALS);self.assertFalse(runner._ACTIVE_PROCESSES)
        self.assertEqual(runner.list_spudex_sessions()[0]['status'],'failed')

    async def test_interactive_shell_restricted_access_is_rejected(self):
        with self.assertRaises(PermissionError):await terminal.start_terminal(redis_client=self._redis(full_access=False))
        self.assertFalse(terminal._TERMINALS)

    @unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_spawn'),'POSIX terminals')
    async def test_interactive_shell_input_resize_interrupt_and_shutdown(self):
        redis=self._redis(full_access=True)
        project=workspace.add_project(str(self.workspace))
        with mock.patch.dict(os.environ,{'PATH':'/usr/bin:/bin','HOME':str(self.host_root),'SHELL':'/bin/sh','PS1':'fixture$ '},clear=True):
            result=await terminal.start_terminal(project_id=project['id'],redis_client=redis)
        sid=result['session']['id']
        async def until(needle):
            for _ in range(120):
                output=''.join(row['text'] for row in runner.read_spudex_logs(sid,tail=True,limit=1000)['entries'])
                if needle in output:return output
                await asyncio.sleep(.025)
            self.fail(f'PTY did not produce {needle!r}')
        await terminal.write_terminal(sid,"printf '\\120\\124\\131\\137\\117\\113\\n'\r",redis_client=redis)
        await until('PTY_OK')
        await terminal.write_terminal(sid,'read answer; printf "answer=%s\\n" "$answer"\r',redis_client=redis)
        await asyncio.sleep(.08)
        await terminal.write_terminal(sid,'potato\r',redis_client=redis)
        await until('answer=potato')
        await terminal.write_terminal(sid,'stty size\r',cols=120,rows=40,redis_client=redis)
        await until('40 120')
        await terminal.write_terminal(sid,'sleep 30\r',redis_client=redis);await asyncio.sleep(.1)
        await terminal.write_terminal(sid,'\x03',redis_client=redis)
        await terminal.write_terminal(sid,"printf '\\122\\105\\103\\117\\126\\105\\122\\105\\104\\n'\r",redis_client=redis)
        await until('RECOVERED')
        await runner.shutdown_spudex_runtime()
        self.assertNotIn(sid,terminal._TERMINALS);self.assertNotIn(sid,runner._ACTIVE_PROCESSES)

if __name__=='__main__':unittest.main()

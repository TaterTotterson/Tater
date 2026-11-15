import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import vm from 'node:vm';
import {computed, effectScope, ref, watch} from '../frontend/node_modules/vue/index.mjs';
import ts from '../frontend/node_modules/typescript/lib/typescript.js';

// Execute the actual component setup/queue with real Vue reactivity and a fake
// firmware API. These tests never contact or flash a satellite.
const component = await readFile(new URL('../frontend/src/settings/components/voice/VoiceFirmware.vue', import.meta.url), 'utf8');
const setup = component.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)[1];
const compiled = ts.transpileModule(setup + `
globalThis.firmwareTest = {updateAll, flashOta, stopSession, batchRows, busy, session,
  progress, progressLabel, sessionRunning, sessionEntries, error};`, {
  compilerOptions: {module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022},
}).outputText;

function harness({start, poll, wait} = {}) {
  const calls = [];
  const notifications = [];
  let refreshes = 0;
  const scope = effectScope();
  const context = vm.createContext({
    exports: {}, Error,
    defineProps: () => ({payload: {firmware: {}}, actionEndpoint: '/fake/firmware'}),
    defineEmits: () => (event, ...args) => event === 'notify' ? notifications.push(args) : refreshes++,
    require: (name) => {
      if (name === 'vue') return {computed, ref, watch, onBeforeUnmount() {}};
      if (name === './runtime') return {
        optionValue: (row) => String(row?.value || ''),
        optionLabel: (row) => String(row?.label || ''),
        voiceAction: async (_endpoint, action, payload) => {
          calls.push({action, payload});
          if (action === 'voice_firmware_flash_start') return start(payload);
          if (action === 'voice_firmware_flash_poll') return poll(payload);
          if (action === 'voice_firmware_flash_stop') return {session_id: payload.session_id, phase: 'stopped'};
          throw new Error(`Unexpected action ${action}`);
        },
      };
      throw new Error(`Unexpected import ${name}`);
    },
    window: {
      setTimeout: (callback) => { queueMicrotask(() => wait ? wait(callback) : callback()); return 1; },
      clearTimeout() {},
    },
  });
  scope.run(() => vm.runInContext(compiled, context));
  return {api: context.firmwareTest, calls, notifications, refreshes: () => refreshes, close: () => scope.stop()};
}

const satellites = (count) => Array.from({length: count}, (_, index) => ({
  selector: `native:sat${index + 1}`, template_key: 'satellite1', title: `Sat ${index + 1}`, connected: true,
}));
const statuses = (h) => Array.from(h.api.batchRows.value, (row) => row.status);
const starts = (h) => h.calls.filter((call) => call.action === 'voice_firmware_flash_start');

test('five-device queue skips an offline third satellite and updates the remaining four', async () => {
  const h = harness({
    start: ({selector}) => selector.endsWith('sat3')
      ? {phase: 'skipped', reason: 'offline', message: 'Satellite is offline. Skipped; update it when it reconnects.'}
      : {session_id: selector, phase: 'writing', progress_percent: 25},
    poll: ({session_id}) => ({session_id, phase: 'completed', progress_percent: 100}),
  });
  try {
    await h.api.updateAll(satellites(5));
    assert.equal(starts(h).length, 5);
    assert.ok(starts(h).every((call) => call.payload.skip_offline === true));
    assert.deepEqual(statuses(h), ['completed', 'completed', 'skipped', 'completed', 'completed']);
    assert.equal(h.calls.filter((call) => call.action === 'voice_firmware_flash_poll').length, 4);
    assert.match(h.api.session.value.message, /Updated 4 satellites\. Skipped 1 offline satellite\./);
    assert.equal(h.api.session.value.phase, 'completed');
    assert.equal(h.api.progress.value, 100);
    assert.equal(h.api.busy.value, '');
    assert.equal(h.refreshes(), 1);
  } finally { h.close(); }
});

test('start errors and device-reported failures do not prevent later updates', async () => {
  const h = harness({
    start: ({selector}) => {
      if (selector.endsWith('sat1')) throw new Error('Manifest unavailable');
      return {session_id: selector, phase: 'writing'};
    },
    poll: ({session_id}) => session_id.endsWith('sat2')
      ? {session_id, phase: 'failed', error: 'OTA checksum failed'}
      : {session_id, phase: 'completed'},
  });
  try {
    await h.api.updateAll(satellites(3));
    assert.equal(starts(h).length, 3);
    assert.deepEqual(statuses(h), ['failed', 'failed', 'completed']);
    assert.equal(h.api.batchRows.value[0].message, 'Manifest unavailable');
    assert.equal(h.api.batchRows.value[1].message, 'OTA checksum failed');
    assert.match(h.api.session.value.message, /Updated 1 satellite\. 2 updates failed/);
    assert.equal(h.api.session.value.phase, 'failed');
    assert.equal(h.api.progress.value, 100);
    assert.equal(h.notifications.at(-1)[1], 'error');
  } finally { h.close(); }
});

test('aggregate progress counts skipped/failed work and resets the next device progress', async () => {
  let h;
  h = harness({
    start: ({selector}) => {
      if (selector.endsWith('sat2')) {
        assert.equal(h.api.progress.value, 25);
        assert.equal(h.api.sessionRunning.value, true);
        return {phase: 'skipped', reason: 'offline'};
      }
      if (selector.endsWith('sat3')) {
        assert.equal(h.api.progress.value, 50);
        throw new Error('Device failed');
      }
      return {session_id: selector, phase: 'writing', progress_percent: 50};
    },
    poll: ({session_id}) => {
      assert.equal(h.api.progress.value, session_id.endsWith('sat1') ? 12.5 : 87.5);
      return {session_id, phase: 'completed', progress_percent: 100};
    },
  });
  try {
    await h.api.updateAll(satellites(4));
    assert.equal(h.api.progress.value, 100);
    assert.equal(h.api.progressLabel.value, '4 of 4 processed');
    assert.match(h.api.session.value.message, /Updated 2 satellites\. Skipped 1 offline satellite\. 1 update failed/);
  } finally { h.close(); }
});

test('all-offline batch reports zero updates without polling or claiming success for devices', async () => {
  const h = harness({start: () => ({phase: 'skipped', reason: 'offline', message: 'Offline; skipped.'})});
  try {
    await h.api.updateAll(satellites(2));
    assert.deepEqual(statuses(h), ['skipped', 'skipped']);
    assert.match(h.api.session.value.message, /Updated 0 satellites\. Skipped 2 offline satellites\./);
    assert.equal(h.api.progress.value, 100);
    assert.equal(h.notifications.at(-1)[1], 'warning');
    assert.ok(h.calls.every((call) => call.action === 'voice_firmware_flash_start'));
  } finally { h.close(); }
});

test('stale offline UI snapshot does not skip a satellite that is online at dispatch', async () => {
  const h = harness({start: () => ({phase: 'completed', progress_percent: 100})});
  try {
    await h.api.updateAll([{...satellites(1)[0], connected: false}]);
    assert.equal(starts(h).length, 1);
    assert.deepEqual(statuses(h), ['completed']);
  } finally { h.close(); }
});

test('stopping the queue stops the current session and does not start another device', async () => {
  let h;
  h = harness({
    start: ({selector}) => ({session_id: selector, phase: 'writing'}),
    wait: async (callback) => { await h.api.stopSession(); callback(); },
  });
  try {
    await h.api.updateAll(satellites(3));
    assert.equal(starts(h).length, 1);
    assert.equal(h.calls.filter((call) => call.action === 'voice_firmware_flash_stop').length, 1);
    assert.deepEqual(statuses(h), ['stopped', 'stopped', 'stopped']);
    assert.equal(h.api.session.value.phase, 'stopped');
    assert.equal(h.api.busy.value, '');
  } finally { h.close(); }
});

test('stopping during a pending start also stops the returned session and keeps queue locked', async () => {
  let finishStart;
  const h = harness({start: () => new Promise((resolve) => { finishStart = resolve; })});
  try {
    const queue = h.api.updateAll(satellites(2));
    await h.api.stopSession();
    await h.api.updateAll(satellites(2));
    assert.equal(starts(h).length, 1);
    finishStart({session_id: 'late-start', phase: 'writing'});
    await queue;
    assert.equal(h.calls.filter((call) => call.action === 'voice_firmware_flash_stop').length, 1);
    assert.deepEqual(statuses(h), ['stopped', 'stopped']);
  } finally { h.close(); }
});

test('single-device offline install still reports its error instead of using batch skip mode', async () => {
  const h = harness({start: () => { throw new Error('Satellite is offline'); }});
  try {
    await h.api.flashOta(satellites(1)[0]);
    assert.equal(starts(h)[0].payload.skip_offline, undefined);
    assert.equal(h.api.session.value.phase, 'failed');
    assert.equal(h.api.error.value, 'Satellite is offline');
  } finally { h.close(); }
});

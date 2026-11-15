import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import vm from 'node:vm';
import ts from '../frontend/node_modules/typescript/lib/typescript.js';
const source = await readFile(new URL('../frontend/src/shared/popupMotion.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
const { normalizePopupEffect, popupEffects, popupMotionFrames, animatePopup, cancelPopupMotion } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`);
const legacy = { flame: 'scale', dust: 'fade', glitch: 'tilt', portal: 'fold', melt: 'slide' };
function fixture({ effect = 'scale', reduced = false, toast = false } = {}) {
  const listeners = new Set();
  const media = { matches: reduced, addEventListener: (_, fn) => listeners.add(fn), removeEventListener: (_, fn) => listeners.delete(fn) };
  globalThis.window = { matchMedia: () => media };
  globalThis.document = { body: { dataset: { popupEffect: effect } } };
  const animations = [];
  const animate = (frames, options) => {
    let resolve, reject;
    const row = { frames, options, finished: new Promise((ok, no) => { resolve = ok; reject = no; }), finish: () => resolve(), cancel: () => { row.cancelled = true; reject(new Error('cancelled')); } };
    animations.push(row); return row;
  };
  const panel = { animate };
  const root = { dataset: {}, animate, querySelector: () => toast ? null : panel, inert: false };
  return { root, animations, listeners, reduce() { media.matches = true; [...listeners].forEach(fn => fn()); } };
}
const settle = () => new Promise(resolve => setImmediate(resolve));

test('all existing preferences migrate and unknown values choose the default', () => {
  for (const [old, current] of Object.entries(legacy)) assert.equal(normalizePopupEffect(` ${old.toUpperCase()} `), current);
  for (const effect of popupEffects) assert.equal(normalizePopupEffect(effect.id), effect.id);
  for (const value of [undefined, '', 'unknown', 'constructor', '__proto__']) assert.equal(normalizePopupEffect(value), 'scale');
});

test('bootstrap and Vue normalize exactly the same saved values', async () => {
  const bootstrap = await readFile(new URL('../tateros_static/app.js', import.meta.url), 'utf8');
  const statements = ts.createSourceFile('app.js', bootstrap, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS).statements;
  const code = statements.filter(node => (ts.isFunctionDeclaration(node) && node.name?.text === 'normalizePopupEffectStyle') || (ts.isVariableStatement(node) && node.declarationList.declarations.some(row => ['POPUP_EFFECT_STYLE_CHOICES', 'POPUP_EFFECT_STYLE_ALIASES'].includes(row.name.getText())))).map(node => node.getText()).join('\n');
  const normalize = vm.runInNewContext(`${code}\nnormalizePopupEffectStyle`);
  for (const value of [...Object.keys(legacy), ...popupEffects.map(row => row.id), ' BAD ', undefined, 'constructor', '__proto__']) assert.equal(normalize(value), normalizePopupEffect(value));
});

test('each motion has distinct frames for both surfaces and exit ends invisible', () => {
  for (const surface of ['dialog', 'toast']) {
    const motions = popupEffects.filter(row => row.id !== 'disabled').map(row => {
      const frames = popupMotionFrames(row.id, 'leave', surface);
      assert.equal(frames[0].opacity, 1); assert.equal(frames.at(-1).opacity, 0);
      assert.equal(Object.hasOwn(frames.at(-1), 'filter'), false);
      return JSON.stringify(frames);
    });
    assert.equal(new Set(motions).size, 5);
  }
});

test('fade remains a visible dissolve without introducing movement', () => {
  for (const surface of ['dialog', 'toast']) {
    const { root, animations } = fixture({ toast: surface === 'toast' });
    for (const phase of ['enter', 'leave']) {
      animatePopup(root, phase, () => {}, phase === 'enter' ? 'fade' : undefined, surface);
      const panel = animations.at(surface === 'toast' ? -1 : -2);
      assert.ok(panel.options.duration >= 200, 'the dissolve must last long enough to perceive');
      assert.equal(panel.options.easing, 'linear', 'opacity must not jump almost immediately to its final value');
      assert.ok(panel.frames.every(frame => frame.transform === 'none'));
      cancelPopupMotion(root);
    }
  }
});

test('tilt reveals the rotated card and keeps it visible through the exit rotation', () => {
  for (const surface of ['dialog', 'toast']) {
    const { root, animations } = fixture({ toast: surface === 'toast' });
    for (const phase of ['enter', 'leave']) {
      animatePopup(root, phase, () => {}, phase === 'enter' ? 'tilt' : undefined, surface);
      const panel = animations.at(surface === 'toast' ? -1 : -2);
      const visibleTilt = panel.frames.find(frame => frame.opacity === 1 && /rotateX\(-?[1-9]/.test(frame.transform));
      assert.ok(visibleTilt, 'the tipped card must be fully visible before it straightens or disappears');
      if (surface === 'dialog') {
        const backdrop = animations.at(-1);
        const hold = backdrop.frames.find(frame => frame.opacity === 1 && frame.offset > 0 && frame.offset < 1);
        assert.ok(hold, 'backdrop fading must leave time to see the rotation');
        if (phase === 'enter') assert.ok(hold.offset < 0.5);
        else assert.ok(hold.offset >= visibleTilt.offset);
      }
      cancelPopupMotion(root);
    }
  }
});

test('Vue is released only after both panel and backdrop finish', async () => {
  const { root, animations, listeners } = fixture(); let calls = 0;
  animatePopup(root, 'enter', () => calls++);
  assert.equal(animations.length, 2); assert.equal(root.dataset.popupMotion, 'scale');
  animations[0].finish(); await settle(); assert.equal(calls, 0);
  animations[1].finish(); await settle(); assert.equal(calls, 1); assert.equal(listeners.size, 0);
  assert.ok(animations.every(row => row.cancelled));
});

test('preview entry and exit retain their own effect without changing global preference', async () => {
  const { root, animations } = fixture({ effect: 'fade' }); let calls = 0;
  animatePopup(root, 'enter', () => calls++, 'fold');
  animations.forEach(row => row.finish()); await settle();
  animatePopup(root, 'leave', () => calls++);
  assert.equal(root.dataset.popupMotion, 'fold'); assert.equal(document.body.dataset.popupEffect, 'fade'); assert.equal(root.inert, true);
  assert.equal(animations[2].frames.at(-1).transform, 'scaleY(0.04) scaleX(0.94)');
  animations.slice(2).forEach(row => row.finish()); await settle(); assert.equal(calls, 2);
});

test('Off and reduced motion complete immediately without requesting animations', () => {
  for (const options of [{ effect: 'disabled' }, { reduced: true }]) {
    const { root, animations } = fixture(options); let calls = 0;
    animatePopup(root, 'enter', () => calls++); animatePopup(root, 'leave', () => calls++);
    assert.equal(calls, 2); assert.equal(animations.length, 0);
  }
});

test('enabling reduced motion finishes running animation and removes listeners', async () => {
  const { root, reduce, listeners } = fixture(); let calls = 0;
  animatePopup(root, 'enter', () => calls++); reduce(); await settle();
  assert.equal(calls, 1); assert.equal(listeners.size, 0);
});

test('rapid reopen cancels stale callbacks, restores interaction, and unmount cancels safely', async () => {
  const { root, animations, listeners } = fixture(); let stale = 0, current = 0;
  animatePopup(root, 'enter', () => stale++);
  animatePopup(root, 'leave', () => stale++);
  animatePopup(root, 'enter', () => current++);
  assert.equal(root.inert, false);
  animations.slice(-2).forEach(row => row.finish()); await settle();
  assert.equal(stale, 0); assert.equal(current, 1); assert.equal(listeners.size, 0);
  animatePopup(root, 'leave', () => stale++); cancelPopupMotion(root); await settle();
  assert.equal(stale, 0); assert.equal(listeners.size, 0);
});

test('toasts animate their actual surface and finish without a fixed removal timeout', async () => {
  const { root, animations } = fixture({ toast: true }); let calls = 0;
  animatePopup(root, 'enter', () => calls++, 'slide', 'toast');
  assert.equal(animations.length, 1); assert.match(animations[0].frames[0].transform, /translateY\(20px\)/);
  animations[0].finish(); await settle(); assert.equal(calls, 1);
});

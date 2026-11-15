import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";
import ts from "../frontend/node_modules/typescript/lib/typescript.js";

const appSource = await readFile(new URL("../tateros_static/app.js", import.meta.url), "utf8");
const shellSource = (await readFile(new URL("../frontend/src/shell/AppShell.vue", import.meta.url), "utf8")).match(/<script[^>]*>([\s\S]*?)<\/script>/)[1];
function declarations(source, names) {
  const parsed = ts.createSourceFile("startup.ts", source, ts.ScriptTarget.Latest, true);
  const selected = parsed.statements.filter(node =>
    ts.isFunctionDeclaration(node) ? names.has(node.name?.text)
      : ts.isVariableStatement(node) && node.declarationList.declarations.every(item => names.has(item.name.getText(parsed))),
  ).map(node => node.getText(parsed)).join("\n");
  return ts.transpileModule(selected, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText;
}
const initCode = declarations(appSource, new Set(["init"]));
function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

test("auth and bundle start together; stalled Redis health cannot delay the shell", async () => {
  const events = [], auth = deferred(), bundle = deferred();
  const context = vm.createContext({
    state: { auth: { passwordSet: true, authenticated: true } },
    normalizeSettingsTab: value => value,
    ensureRedisSetup: () => { events.push("redis"); return new Promise(() => {}); },
    fetchWebuiAuthStatus: () => { events.push("auth"); return auth.promise; },
    loadCoreVueModule: () => { events.push("bundle"); return bundle.promise; },
    mountVueAppShell: async () => { events.push("mount"); return true; },
    finishAuthenticatedBootstrap: async () => { events.push("health"); },
  });
  vm.runInContext(initCode, context);
  const pending = context.init();
  assert.deepEqual(events, ["redis", "auth", "bundle"]);
  bundle.resolve();
  await Promise.resolve();
  assert.ok(!events.includes("mount"), "The shell must wait for auth status");
  auth.resolve();
  await pending;
  assert.deepEqual(events, ["redis", "auth", "bundle", "mount", "health"]);
});

test("a locked session never starts authenticated bootstrap", async () => {
  let healthCalls = 0;
  const context = vm.createContext({
    state: { auth: { passwordSet: true, authenticated: false } },
    normalizeSettingsTab: value => value, ensureRedisSetup: async () => {},
    fetchWebuiAuthStatus: async () => {}, loadCoreVueModule: async () => {},
    mountVueAppShell: async () => true, finishAuthenticatedBootstrap: async () => { healthCalls += 1; },
  });
  vm.runInContext(initCode, context);
  await context.init();
  assert.equal(healthCalls, 0);
});

test("native readiness is signalled after the mounted UI gets a paint opportunity", () => {
  const frames = [], dataset = {};
  const context = vm.createContext({ window: { requestAnimationFrame: callback => frames.push(callback) }, document: { documentElement: { dataset } } });
  vm.runInContext(declarations(appSource, new Set(["markTaterUiReady"])), context);
  context.markTaterUiReady();
  assert.equal(dataset.taterUiReady, undefined);
  frames.shift()();
  assert.equal(dataset.taterUiReady, undefined);
  frames.shift()();
  assert.equal(dataset.taterUiReady, "true");
});

const pollCode = declarations(shellSource, new Set([
  "startupModelOpen", "startupModelWarmup", "startupModelPollError", "startupModelTimer", "startupModelCompleteTimer",
  "startupModelDismissed", "startupModelObserved", "startupModelRequest", "startupModelDisposed",
  "pollStartupModelWarmup", "scheduleStartupModelPoll", "clearStartupModelTimers", "closeStartupModelProgress",
]));
function pollingContext(getJson) {
  const timers = new Map();
  let sequence = 0;
  const context = vm.createContext({
    AbortController, getJson, ref: value => ({ value }),
    props: { options: { modelWarmupEndpoint: "/progress" } }, authState: { required: false },
    window: { setTimeout: (callback, delay) => { const id = ++sequence; timers.set(id, { callback: () => { timers.delete(id); return callback(); }, delay }); return id; }, clearTimeout: id => timers.delete(id) },
  });
  vm.runInContext(pollCode + "\nfunction progressState() { return { open: startupModelOpen.value, observed: startupModelObserved, error: startupModelPollError.value }; }", context);
  return { context, timers, run: code => vm.runInContext(code, context) };
}
const snapshot = { reason: "startup", running: true, startup_bootstrap_pending: true };

test("the first unavailable progress request retries and opens the loader on recovery", async () => {
  let calls = 0;
  const { context, timers } = pollingContext(async () => { if (++calls === 1) throw new Error("Starting"); return snapshot; });
  await context.pollStartupModelWarmup();
  assert.equal(context.progressState().observed, false);
  const retry = [...timers.values()].find(timer => timer.delay === 1600);
  assert.ok(retry);
  await retry.callback();
  assert.equal(context.progressState().open, true);
  assert.equal(context.progressState().error, "");
  context.closeStartupModelProgress();
  await [...timers.values()].find(timer => timer.delay === 650).callback();
  assert.equal(context.progressState().open, false, "Keep the user's Hide choice during retries");
});

test("a hung progress request is aborted and retried; unmount stops retrying", async () => {
  let lastSignal;
  const { context, timers, run } = pollingContext((_endpoint, { signal }) => new Promise((_resolve, reject) => {
    lastSignal = signal;
    signal.addEventListener("abort", () => reject(new Error("Aborted")), { once: true });
  }));
  const pending = context.pollStartupModelWarmup();
  [...timers.values()].find(timer => timer.delay === 4000).callback();
  await pending;
  assert.equal(lastSignal.aborted, true);
  assert.ok([...timers.values()].some(timer => timer.delay === 1600));
  const retrying = [...timers.values()].find(timer => timer.delay === 1600).callback();
  run("startupModelDisposed = true; clearStartupModelTimers();");
  await retrying;
  assert.equal(lastSignal.aborted, true);
  assert.equal(timers.size, 0);
});

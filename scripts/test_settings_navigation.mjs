import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import ts from "../frontend/node_modules/typescript/lib/typescript.js";

const source = await readFile(new URL("../frontend/src/settings/navigation.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
const { settingsCategories, settingsPages, settingsPageForLegacy, searchSettings } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}`);

test("every original settings panel remains reachable", () => {
  const panels = ["general", "people", "models", "hydra", "esphome", "redis", "spudhub", "misc", "advanced", "system", "logs"];
  assert.deepEqual([...new Set(settingsPages.map(page => page.panel))].sort(), panels.sort());
  for (const panel of panels) assert.equal(settingsPageForLegacy(panel).panel, panel);
});

test("every model and satellite screen has exactly one canonical destination", () => {
  for (const [panel, children] of Object.entries({
    models: ["huggingface", "routing", "listening", "speech", "wake", "vision", "audio-understanding", "speakerid", "emotionid", "faceid"],
    esphome: ["satellites", "firmware", "stereo", "airplay", "presence", "stats", "platform"],
  })) {
    assert.deepEqual(settingsPages.filter(page => page.panel === panel).map(page => page.child).sort(), children.sort());
    for (const child of children) {
      assert.equal(settingsPageForLegacy(panel, child).child, child);
      assert.equal(settingsPages.filter(page => page.panel === panel && page.child === child).length, 1);
    }
  }
});

test("groups, section navigation, and page IDs have no orphaned routes", () => {
  assert.equal(settingsCategories.length, 7);
  assert.equal(new Set(settingsPages.map(page => page.id)).size, settingsPages.length);
  for (const page of settingsPages) {
    assert.ok(settingsCategories.some(category => category.id === page.category));
    const peers = settingsPages.filter(candidate => candidate.category === page.category && candidate.section === page.section);
    assert.ok(peers.every(candidate => candidate.sectionLabel === page.sectionLabel));
  }
  for (const category of settingsCategories) assert.ok(settingsPages.some(page => page.category === category.id));
  assert.equal(settingsPages.find(page => page.id === "login").category, "access");
});

test("legacy selection normalizes input and retains sensible fallbacks", () => {
  assert.equal(settingsPageForLegacy(" MODELS ", " WAKE ").id, "wake");
  assert.equal(settingsPageForLegacy("models").child, "huggingface");
  assert.equal(settingsPageForLegacy("models", "removed-tab").child, "huggingface");
  assert.equal(settingsPageForLegacy("esphome").child, "satellites");
  assert.equal(settingsPageForLegacy("general").id, "general");
  assert.equal(settingsPageForLegacy("general", "login").id, "login");
  assert.equal(settingsPageForLegacy("unknown").id, "general");
});

test("plain-language and technical searches find settings in their new locations", () => {
  for (const [query, id] of [["wake word", "wake"], ["password", "login"], ["Emotion ID", "emotionid"], ["Redis encryption", "redis"], ["AirPlay", "airplay"], ["presence bluetooth", "presence"], ["Discord reactions", "misc"], ["Hydra ledger", "hydra"], ["GGUF", "models"], ["display brightness", "esphome"], ["admin plugin", "advanced"]]) {
    assert.ok(searchSettings(query).some(page => page.id === id), `${query} should find ${id}`);
  }
  assert.deepEqual(searchSettings("   "), []);
  assert.deepEqual(searchSettings("no-setting-has-this-name"), []);
});

test("the new routes cover all child tabs actually declared by the existing forms", async () => {
  for (const [file, panel] of [["ModelsSettings.vue", "models"], ["VoiceSettings.vue", "esphome"]]) {
    const form = await readFile(new URL(`../frontend/src/settings/components/${file}`, import.meta.url), "utf8");
    const tabs = form.match(/const tabs = \[([\s\S]*?)\] as const;/)[1];
    const ids = [...tabs.matchAll(/id: "([^"]+)"/g)].map(match => match[1]);
    assert.deepEqual(settingsPages.filter(page => page.panel === panel).map(page => page.child).sort(), ids.sort());
  }
});

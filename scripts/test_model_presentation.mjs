import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import ts from "../frontend/node_modules/typescript/lib/typescript.js";

const source = await readFile(new URL("../frontend/src/settings/components/models/modelPresentation.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
const { modelPresentation, splitListeningSections } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}`);
const workspaceSource = await readFile(new URL("../frontend/src/settings/components/voice/satelliteWorkspace.ts", import.meta.url), "utf8");
const workspaceCompiled = ts.transpileModule(workspaceSource, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
const { satelliteSettingsChange, satelliteSettingsTab, satelliteWorkspaceTabs } = await import(`data:text/javascript;base64,${Buffer.from(workspaceCompiled).toString("base64")}`);

test("satellite preset selection updates every band and boost while manual edits preserve the curve", () => {
  const warm = [3, 3, -1, -3, -2, -1, -2, -3];
  const fields = [{ key: "eq_preset", presets: { warm, flat: Array(8).fill(0) } }];
  const draft = { eq_bands: Array(8).fill(2), eq_loudness: true };
  Object.assign(draft, satelliteSettingsChange(fields, "eq_preset", "warm"));
  assert.deepEqual(draft, { eq_preset: "warm", eq_bands: warm, eq_loudness: false });
  draft.eq_bands[0] = 4;
  assert.equal(warm[0], 3, "a manual edit must not mutate the preset definition");
  assert.deepEqual(satelliteSettingsChange(fields, "eq_bands", draft.eq_bands), { eq_preset: "manual", eq_bands: draft.eq_bands });
  assert.deepEqual(satelliteSettingsChange(fields, "eq_loudness", true), { eq_preset: "manual", eq_loudness: true });
  assert.deepEqual(satelliteSettingsChange(fields, "eq_preset", "manual"), { eq_preset: "manual" });
  assert.deepEqual(satelliteSettingsChange(fields, "volume_percent", 20), { volume_percent: 20 });
});

test("friendly model names preserve the exact identifier and quantization", () => {
  const id = "Example/Chat-GGUF::Chat-8B-Q4_K_M.gguf";
  const result = modelPresentation(id, id);
  assert.equal(result.name, "Chat 8B");
  assert.equal(result.identifier, id);
  assert.equal(result.original, id);
  assert.match(result.detail, /Q4_K_M/);
  assert.match(result.detail, /Example\/Chat-GGUF/);
});

test("legacy models keep their not-installed warning visible", () => {
  const id = "Example/Previous::Previous-Q8_0.gguf";
  const result = modelPresentation(`Current: ${id} (not installed)`, id);
  assert.equal(result.identifier, id);
  assert.match(result.detail, /Not installed/);
});

test("short backend names and empty optional choices retain their labels", () => {
  assert.equal(modelPresentation("Voxtral Realtime", "voxtral_realtime_llama_cpp").name, "Voxtral Realtime");
  assert.equal(modelPresentation("Embedded heads / no sidecar", "").name, "Embedded heads / no sidecar");
  assert.equal(modelPresentation("Choose a model", "").identifier, "");
});

test("grouping listening controls retains unknown future fields exactly once", () => {
  const source = [{ label: "Voice", fields: [{ key: "VOICE_VAD_BACKEND" }, { key: "FUTURE_BACKEND_OPTION" }] }, { label: "Hardware", fields: [{ key: "ACCELERATOR" }] }];
  const split = splitListeningSections(source);
  const all = [...split.basic, ...split.advanced].flatMap(section => section.fields);
  assert.deepEqual(all.map(field => field.key).sort(), ["VOICE_VAD_BACKEND", "FUTURE_BACKEND_OPTION", "ACCELERATOR"].sort());
  assert.equal(split.basic[0].fields[0], source[0].fields[0]);
  assert.equal(source[0].fields.length, 2);
});

test("satellite workspaces retain all groups, including future controls and dependencies", () => {
  const groups = [
    { key: "dnd_section", label: "Do Not Disturb", fields: [{ key: "dnd_enabled" }, { key: "dnd_start", show_when: { source_key: "dnd_enabled", equals: true } }] },
    { key: "output_section", label: "Speaker output", fields: [{ key: "audio_output_mode" }] },
    { key: "led_section", label: "LED Settings", fields: [{ key: "led_preview", type: "led_preview" }] },
    { key: "screen_section", label: "Display & Night", fields: [{ key: "screen_brightness" }] },
    { key: "display_theme_section", label: "Appearance & Photos", fields: [{ key: "display_theme" }] },
    { key: "ssh_section", label: "Remote Maintenance", fields: [{ key: "ssh_password", type: "password" }] },
    { key: "logging_section", label: "Logging", fields: [{ key: "logging_level" }] },
    { key: "future_section", label: "Future satellite features", fields: [{ key: "unknown_future_control" }] },
  ];
  assert.deepEqual(groups.map(satelliteSettingsTab), ["room", "room", "voice", "display", "display", "info", "info", "voice"]);
  const displayed = satelliteWorkspaceTabs.flatMap(tab => groups.filter(group => satelliteSettingsTab(group) === tab.id));
  assert.equal(displayed.length, groups.length);
  for (const group of groups) assert.equal(displayed.filter(candidate => candidate === group).length, 1);
  assert.equal(displayed.flatMap(group => group.fields).length, groups.flatMap(group => group.fields).length);
  assert.equal(groups[0].fields[1].show_when.source_key, "dnd_enabled");
});

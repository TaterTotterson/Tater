import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import ts from "../frontend/node_modules/typescript/lib/typescript.js";

const source = await readFile(new URL("../frontend/src/shared/catalogSearch.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
const { catalogMatches } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}`);

test("an empty catalog search keeps every installed item visible", () => {
  assert.equal(catalogMatches("  ", "Broadcast", "Send announcements"), true);
  assert.equal(catalogMatches("", null, undefined), true);
});

test("search combines words across names, descriptions, IDs, and capability tags", () => {
  assert.equal(catalogMatches("OFFICE lights", "Philips Hue", "Office", ["lights", "scenes"]), true);
  assert.equal(catalogMatches("voice broad", "broadcast", "Voice announcements"), true);
  assert.equal(catalogMatches("office camera", "Office", ["lights"]), false);
});

test("display names match with or without accents and surrounding whitespace", () => {
  assert.equal(catalogMatches("  cafe  ", "Café speakers"), true);
  assert.equal(catalogMatches("café", "Cafe speakers"), true);
});

/** Display names only: model IDs and native option values are never rewritten. */
export function modelPresentation(label: string, value = "") {
  const original = String(label || value || "").trim();
  const identifier = String(value || "");
  const parts = identifier.split("::");
  const filename = (parts.length > 1 ? parts.slice(1).join("::") : identifier).split(/[\\/]/).pop() || "";
  const quant = filename.match(/(?:^|[-_.])(UD[-_])?(?:IQ|Q)\d[\w-]*(?=\.gguf$|$)/i)?.[0].replace(/^[-_.]/, "").replace(/\.gguf$/i, "") || "";
  const needsShortName = original.includes("::") || /\.gguf/i.test(original) || original.length > 95 || original === identifier && original.includes("/");
  const shortName = needsShortName && filename
    ? filename.replace(/\.gguf$/i, "").replace(/[-_.](?:UD[-_])?(?:IQ|Q)\d[\w-]*$/i, "").replace(/[-_.]+/g, " ").trim()
    : original;
  const name = needsShortName ? shortName.charAt(0).toUpperCase() + shortName.slice(1) : shortName;
  return { name: name || "Choose a model", quant, detail: [quant, parts.length > 1 ? parts[0] : "", /not installed/i.test(original) ? "Not installed" : ""].filter(Boolean).join(" · "), original, identifier };
}

/** Keep everyday detection visible and retain every other dynamic control. */
export function splitListeningSections(sections: Record<string, unknown>[]) {
  const basic: Record<string, unknown>[] = [], advanced: Record<string, unknown>[] = [];
  for (const section of sections) {
    const fields = Array.isArray(section.fields) ? section.fields as Record<string, unknown>[] : [];
    const visible = fields.filter(field => String(field.key || field.id || "") === "VOICE_VAD_BACKEND");
    const remaining = fields.filter(field => String(field.key || field.id || "") !== "VOICE_VAD_BACKEND");
    if (visible.length) basic.push({ ...section, fields: visible });
    if (remaining.length || !fields.length) advanced.push({ ...section, fields: remaining });
  }
  return { basic, advanced };
}

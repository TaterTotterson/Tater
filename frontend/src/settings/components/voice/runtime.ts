import { postJson } from "../../../shared/api";
import type { JsonRow } from "../../types";

export function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value ?? {})) as T;
}

export function itemForms(payload: JsonRow, group?: string): JsonRow[] {
  const ui = payload.ui && typeof payload.ui === "object" ? payload.ui as JsonRow : {};
  const forms = Array.isArray(ui.item_forms) ? ui.item_forms as JsonRow[] : [];
  return group ? forms.filter((form) => String(form.group || "") === group) : forms;
}

export function fieldsToValues(fields: JsonRow[] = []): JsonRow {
  const values: JsonRow = {};
  fields.forEach((field) => {
    const key = String(field.key || field.id || "").trim();
    const type = String(field.type || "").trim().toLowerCase();
    if (!key || field.disabled || field.read_only || field.readonly || ["table", "readonly", "section", "led_preview"].includes(type)) return;
    values[key] = field.value ?? field.default ?? (String(field.type || "") === "checkbox" ? false : "");
  });
  return values;
}

export function sectionsToValues(sections: JsonRow[] = []): JsonRow {
  const values: JsonRow = {};
  sections.forEach((section) => Object.assign(values, fieldsToValues(Array.isArray(section.fields) ? section.fields : [])));
  return values;
}

export function formSections(form: JsonRow | null | undefined): JsonRow[] {
  return form && Array.isArray(form.sections) ? form.sections as JsonRow[] : [];
}

export function formFields(form: JsonRow | null | undefined): JsonRow[] {
  return form && Array.isArray(form.fields) ? form.fields as JsonRow[] : [];
}

export async function voiceAction(endpoint: string, action: string, payload: JsonRow = {}): Promise<JsonRow> {
  return postJson<JsonRow>(endpoint, { action, payload });
}

export function optionValue(option: unknown): string {
  if (option && typeof option === "object") {
    const row = option as JsonRow;
    return String(row.value ?? row.id ?? row.key ?? "");
  }
  return String(option ?? "");
}

export function optionLabel(option: unknown): string {
  if (option && typeof option === "object") {
    const row = option as JsonRow;
    return String(row.label ?? row.name ?? row.value ?? row.id ?? "");
  }
  return String(option ?? "");
}

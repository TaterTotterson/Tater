import type {JsonRow} from './types';

export const collectionLayouts = new Set(['tasks','rules','events','timeline','memories','people','feeds','nodes','devices','accounts','sources','picks']);
export const overviewLayouts = new Set(['weather','forecast','trends','network','agenda','insights','media','behavior']);

/** Presentation is optional. Unknown tabs and community cores keep their original renderer. */
export function workspaceScreen(tab: JsonRow | null, metadata: JsonRow): JsonRow {
  const screen = metadata.screens?.[String(tab?.key || '')] || metadata.screens?.['*'];
  return screen && typeof screen === 'object' ? {...screen} : {};
}
export function allFields(item: JsonRow): JsonRow[] {
  return [...(item.fields || []), ...(item.sections || []).flatMap((section: JsonRow) => section.fields || []), ...(item.popup_fields || [])];
}
export function fieldText(field: JsonRow): string {
  const value = field.value ?? field.default;
  if (value === undefined || value === null) return '';
  const options = (field.options || []).flatMap((option: JsonRow) => option?.options || [option]);
  const label = (entry: unknown) => {
    const match = options.find((option: any) => String(option?.value ?? option?.id ?? option) === String(entry));
    return String(match?.label ?? match?.title ?? entry);
  };
  if (Array.isArray(value)) return value.map(label).join(', ');
  if (typeof value === 'boolean') return value ? 'Enabled' : 'Disabled';
  return typeof value === 'object' ? '' : label(value);
}
export function isPrivateField(field: JsonRow): boolean {
  return ['hidden','password','file'].includes(String(field.type)) || /(?:password|secret|token|api_key|credential|(^|_)pin($|_)|raw_.*json|backup_json)/i.test(String(field.key));
}
/** Summaries honor the same conditional visibility as the original field editor. */
export function fieldVisible(field: JsonRow, values: JsonRow): boolean {
  const conditions = Array.isArray(field.show_when_all) ? field.show_when_all : field.show_when ? [field.show_when] : [];
  const normalized = (value: unknown) => typeof value === 'boolean' ? String(value) : String(value ?? '').trim();
  return conditions.filter((condition: unknown) => condition && typeof condition === 'object').every((condition: JsonRow) => {
    const key = String(condition.source_key ?? condition.key ?? '').trim();
    const allowed = [...(Array.isArray(condition.any_of) ? condition.any_of : []), ...(Array.isArray(condition.values) ? condition.values : []),
      ...['equals','eq','value'].flatMap(name => condition[name] !== undefined ? [condition[name]] : [])].map(normalized);
    return !key || !allowed.length || allowed.includes(normalized(values[key]));
  });
}
export function previewFacts(item: JsonRow, metadata: JsonRow, currentValues?: JsonRow): JsonRow[] {
  const summary = (item.summary_rows || []).filter((row: JsonRow) => row.label && row.value !== undefined && row.value !== '');
  const keys: string[] = metadata.preview_fields || [];
  const values = currentValues || Object.fromEntries(allFields(item).map(field => [field.key,field.value ?? field.default]));
  const fields = allFields(item).map<JsonRow>(field => ({...field,value:values[field.key] ?? field.value ?? field.default})).filter(field => keys.includes(String(field.key)) && !isPrivateField(field) && fieldVisible(field,values));
  const facts = fields.map(field => ({label: field.label || field.key, value: fieldText(field)})).filter(row => row.value !== '');
  const seen = new Set(summary.map((row: JsonRow) => String(row.label).toLowerCase()));
  return [...summary, ...facts.filter(row => !seen.has(String(row.label).toLowerCase()))];
}
export function readoutFields(item: JsonRow): JsonRow[] {
  return allFields(item).filter(field => !isPrivateField(field) && (
    field.read_only || field.readonly || ['readonly','read_only','table','bars','bar_chart','image','video'].includes(String(field.type))
  ));
}
export function itemCanEdit(item: JsonRow): boolean {
  return allFields(item).some(field => !field.read_only && !field.readonly && !['hidden','readonly','read_only','table','bars','bar_chart','image','video','heading','section_heading'].includes(String(field.type)));
}
export function metric(stats: JsonRow[], labels: string[]): string {
  const row = stats.find(row => labels.some(label => String(row.label).toLowerCase() === label.toLowerCase()));
  return row?.value === undefined || row.value === null ? '' : String(row.value);
}

/** All table columns/rows are retained, including unrecognized provider columns. */
export function tableRows(field: JsonRow): JsonRow[] {
  const columns = (field.columns || []).map((column: any, index: number) => typeof column === 'string' ? {key:column,label:column} : {...column,key:String(column.key ?? column.id ?? column.field ?? `col_${index}`)});
  return (field.rows || []).map((row: any) => Object.fromEntries(columns.map((column: JsonRow, index: number) => [column.label || column.key, Array.isArray(row) ? row[index] : row[column.key]])));
}

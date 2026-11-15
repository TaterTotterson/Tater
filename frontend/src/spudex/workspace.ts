import type { JsonRow } from './types';

export type TaskMode = 'ask' | 'plan' | 'build';
export function taskMode(value: unknown): TaskMode { return value === 'ask' || value === 'plan' ? value : 'build'; }
export function sessionProjectId(session: JsonRow, projects: JsonRow[]): string {
  if (session.project?.id) return String(session.project.id);
  const cwd = String(session.cwd || '').replace(/\/$/, '');
  const matches = projects.filter(project => cwd === project.path || cwd.startsWith(`${project.path}/`)).sort((a,b) => String(b.path).length - String(a.path).length);
  return String(matches[0]?.id || '');
}
export function mergeLogRows(current: JsonRow[], incoming: JsonRow[], reset = false): JsonRow[] {
  const rows = new Map((reset ? [] : current).map(row => [String(row.seq),row]));
  incoming.forEach(row => rows.set(String(row.seq),row));
  return [...rows.values()].sort((a,b) => Number(a.seq)-Number(b.seq));
}
export function taskFeed(rows: JsonRow[]): JsonRow[] {
  const feed: JsonRow[] = [];
  for (const row of rows) {
    if (row.stream === 'user' || row.stream === 'assistant') feed.push({kind:'message',seq:row.seq,role:row.stream,content:String(row.text || '')});
    else {
      let group = feed.at(-1);
      if (!group || group.kind !== 'activity') { group={kind:'activity',seq:row.seq,entries:[]}; feed.push(group); }
      group.entries.push(row);
    }
  }
  return feed;
}
export function statusLabel(value: unknown): string {
  const token=String(value || 'draft').toLowerCase();
  return ({succeeded:'Complete',completed:'Complete',running:'Working',queued:'Queued',draft:'New task',failed:'Failed',blocked:'Needs attention',incomplete:'Step limit reached',stopped:'Stopped',timeout:'Timed out'} as Record<string,string>)[token] || token;
}
export function isActive(session?: JsonRow | null): boolean {
  if (!session || session.active === false) return false;
  return Boolean(session.active) || ['running','queued'].includes(String(session.status));
}

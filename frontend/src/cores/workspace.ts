import defaults from './workspaces.json';
import type {JsonRow} from './types';
export function coreWorkspace(key: unknown, metadata?: unknown): JsonRow {
  const fallback=(defaults as Record<string,JsonRow>)[String(key)]||{};
  const source=metadata&&typeof metadata==='object'&&(metadata as JsonRow).schema===1?metadata as JsonRow:{};
  return {...fallback,...source,screens:{...fallback.screens,...source.screens},icon:String(source.icon||fallback.icon||'cores'),description:String(source.description||fallback.description||'A workspace for this core.')};
}
export function workspaceTabs(tabs: unknown, metadata: JsonRow): JsonRow[] {
  const rows=(Array.isArray(tabs)?tabs:[]).filter(row=>row&&String(row.key||'').trim());
  const order=Array.isArray(metadata.tab_order)?metadata.tab_order:[];
  return rows.map((row,index)=>({...row,label:metadata.tab_labels?.[row.key]||row.label||row.key,_order:order.includes(row.key)?order.indexOf(row.key):order.length+index})).sort((a,b)=>a._order-b._order);
}
export function initialWorkspaceTab(tabs: JsonRow[], metadata: JsonRow, legacy?: unknown): string {
  return [metadata.default_tab,legacy,tabs[0]?.key].find(key=>tabs.some(tab=>tab.key===key))||'';
}
export interface FieldGroup {id:string;label:string;fields:JsonRow[];description?:string}
/** Every field record is assigned once. New and unrecognized fields remain reachable. */
export function fieldGroups(fields: JsonRow[], sections: JsonRow[]=[], metadata: JsonRow={}): FieldGroup[] {
  const groups:FieldGroup[]=[];
  const recipes=Array.isArray(metadata.field_groups)?metadata.field_groups:[];
  const hasHeadings=fields.some(field=>['heading','section_heading'].includes(String(field.type)));
  const add=(id:string,label:string,field:JsonRow)=>{let group=groups.find(row=>row.id===id);if(!group){group={id,label,fields:[]};groups.push(group);}group.fields.push(field);};
  let heading='details',label='Details';
  for(const field of fields){
    if(hasHeadings){if(['heading','section_heading'].includes(String(field.type))){heading=`heading-${groups.length}`;label=String(field.label||'Options');}add(heading,label,field);continue;}
    if(!recipes.length){add('details','Details',field);continue;}
    const key=String(field.key||'').toLowerCase();let matched=-1,score=0;
    recipes.forEach((recipe:JsonRow,index:number)=>{for(const term of recipe.terms||[]){const token=String(term).toLowerCase();if(token&&key.includes(token)&&token.length>score){score=token.length;matched=index;}}});
    const recipe=recipes[matched];add(matched<0?'extra':`recipe-${matched}`,recipe?.label||'More options',field);
  }
  sections.forEach((section,index)=>{for(const field of Array.isArray(section.fields)?section.fields:[])add(`section-${index}`,String(section.label||'Options'),field);});
  if(!hasHeadings&&recipes.length)groups.sort((a,b)=>{const rank=(id:string)=>id.startsWith('recipe-')?Number(id.slice(7)):recipes.length+(id==='extra'?1:0);return rank(a.id)-rank(b.id);});
  for(const group of groups){if(group.id.startsWith('recipe-'))group.description=recipes[Number(group.id.slice(7))]?.description;}
  return groups;
}
export function itemFields(item:JsonRow):JsonRow[]{return [...(item.fields||[]),...(item.sections||[]).flatMap((s:JsonRow)=>s.fields||[]),...(item.popup_fields||[])];}
export function itemSummary(item:JsonRow):string{return String(item.subtitle||item.detail||(item.summary_rows||[]).map((r:JsonRow)=>`${r.label}: ${r.value}`).join(' · ')||'View details');}
export function itemImage(item:JsonRow):string{return String(item.hero_image_src||itemFields(item).find(f=>f.type==='image'&&(f.src||f.url))?.src||itemFields(item).find(f=>f.type==='image')?.url||itemFields(item).find(f=>f.type==='video')?.poster||'');}
export function itemEditable(item:JsonRow):boolean{return itemFields(item).some(f=>!f.read_only&&!f.readonly&&!['readonly','read_only','table','bars','bar_chart','image','video','heading','section_heading','hidden'].includes(f.type));}
export function presentationFor(tab:JsonRow|null, metadata:JsonRow):string{return String(metadata.views?.[tab?.key]|| (tab?.source==='add_form'?'form':tab?.selector?'list':'stack'));}

/** Only acknowledge values actually submitted; preserve edits made during a save. */
export function acknowledgeDraft(values:JsonRow,dirty:Set<string>,submitted:JsonRow){
  for(const key of [...dirty])if(JSON.stringify(values[key])===JSON.stringify(submitted[key]))dirty.delete(key);
}

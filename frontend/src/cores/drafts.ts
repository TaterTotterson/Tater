import {inject,provide,reactive,type InjectionKey,type Ref} from 'vue';
import type {JsonRow} from './types';
export interface CoreDraft {values:JsonRow;dirty:Set<string>}
const draftKey:InjectionKey<Map<string,CoreDraft>>=Symbol('core-workspace-drafts');
export const workspaceKey:InjectionKey<Ref<JsonRow>>=Symbol('core-workspace-presentation');
export function provideCoreDrafts(){const drafts=reactive(new Map<string,CoreDraft>());provide(draftKey,drafts);return drafts;}
export function useCoreDraft(id:string):CoreDraft{const drafts=inject(draftKey,null);if(!drafts)return reactive({values:{},dirty:new Set<string>()});let draft=drafts.get(id);if(!draft){draft=reactive({values:{},dirty:new Set<string>()});drafts.set(id,draft);}return draft;}
export function cloneFieldValue(value:unknown):any{return value&&typeof value==='object'?JSON.parse(JSON.stringify(value)):value;}
export function sameFieldValue(a:unknown,b:unknown):boolean{return JSON.stringify(a)===JSON.stringify(b);}

export {acknowledgeDraft} from './workspace';

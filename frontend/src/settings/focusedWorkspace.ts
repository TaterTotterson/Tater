import { provide, ref, shallowRef } from "vue";
import type { InjectionKey, Ref, ShallowRef } from "vue";

export interface SettingsWorkspaceHost {
  target: ShallowRef<HTMLElement | null>;
  active: Ref<string>;
  close: ShallowRef<(() => void) | null>;
}

export const settingsWorkspaceKey: InjectionKey<SettingsWorkspaceHost> = Symbol("settings-workspace");

export function provideSettingsWorkspace(): SettingsWorkspaceHost {
  const workspace = { target: shallowRef<HTMLElement | null>(null), active: ref(""), close: shallowRef<(() => void) | null>(null) };
  provide(settingsWorkspaceKey, workspace);
  return workspace;
}

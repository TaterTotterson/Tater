import type { JsonRow } from "../../types";

export const satelliteWorkspaceTabs = [
  { id: "room", label: "Room & audio" },
  { id: "voice", label: "Voice & lights" },
  { id: "display", label: "Display" },
  { id: "info", label: "Device info" },
] as const;
export type SatelliteWorkspaceTab = typeof satelliteWorkspaceTabs[number]["id"];

// Classify whole groups so descriptions, previews, and dependent fields stay
// together. Unrecognized groups remain accessible under Voice & lights.
export function satelliteSettingsTab(group: JsonRow): SatelliteWorkspaceTab {
  const key = String(group.key || "").toLowerCase();
  const label = String(group.label || "").toLowerCase();
  if (/^(ssh|logging|debug|diagnostic|maintenance)/.test(key) || /maintenance|diagnostic|logging/.test(label)) return "info";
  if (/^(screen|display|weather|climate|thermostat)/.test(key) || /display|screen|appearance|photo|weather|climate/.test(label)) return "display";
  if (/^(dnd|playback|equalizer|audio_output|output|volume)/.test(key) || /do not disturb|audio feedback|equalizer|audio output/.test(label)) return "room";
  return "voice";
}

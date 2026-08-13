export type PlayerTargetKind = "satellite" | "stereo" | "airplay" | "sonos" | "home" | "player";

export type PlayerSection<T> = {
  key: PlayerTargetKind;
  label: string;
  items: T[];
};

const sectionOrder: PlayerTargetKind[] = ["satellite", "stereo", "airplay", "sonos", "home", "player"];

const sectionLabels: Record<PlayerTargetKind, string> = {
  satellite: "Tater Native Sats",
  stereo: "Tater Stereo Pairs",
  airplay: "AirPlay Devices",
  sonos: "Sonos Players",
  home: "Home Assistant Players",
  player: "Other Players",
};

export function playerTargetKind(target: unknown): PlayerTargetKind {
  const value = String(target ?? "").toLowerCase();
  if (value.startsWith("voice_core:stereo:") || value.startsWith("stereo:")) return "stereo";
  if (value.startsWith("voice_core:") || value.startsWith("native:")) return "satellite";
  if (value.startsWith("airplay:")) return "airplay";
  if (value.startsWith("sonos:")) return "sonos";
  if (value.startsWith("ha:")) return "home";
  return "player";
}
export function groupPlayerTargets<T>(items: T[], target: (item: T) => unknown): PlayerSection<T>[] {
  const grouped = new Map<PlayerTargetKind, T[]>();
  for (const item of items) {
    const kind = playerTargetKind(target(item));
    grouped.set(kind, [...(grouped.get(kind) || []), item]);
  }
  return sectionOrder
    .filter((kind) => grouped.has(kind))
    .map((kind) => ({ key: kind, label: sectionLabels[kind], items: grouped.get(kind) || [] }));
}

function withoutCategoryPrefix(label: string): string {
  return label.replace(
    /^(?:Tater\s+(?:Satellite|Sat|Stereo)|AirPlay(?:\s+Bridge)?|Sonos|Home\s+Assistant|Saved\s+player)\s*:\s*/i,
    "",
  );
}

function labelParts(label: unknown): { name: string; detail: string; status: string } {
  let value = String(label ?? "").trim();
  let status = "";
  const statusMatch = value.match(/\s*•\s*(offline(?:\s+or\s+firmware\s+update\s+required)?|online)\s*$/i);
  if (statusMatch) {
    status = statusMatch[1].replace(/^./, (character) => character.toUpperCase());
    value = value.slice(0, statusMatch.index).trim();
  }
  value = withoutCategoryPrefix(value);
  let detail = "";
  const detailStart = value.lastIndexOf(" (");
  if (detailStart >= 0 && value.endsWith(")")) {
    detail = value.slice(detailStart + 2, -1).trim();
    value = value.slice(0, detailStart).trim();
  }
  return { name: value || String(label ?? "").trim(), detail, status };
}

export function playerFriendlyName(label: unknown, target: unknown): string {
  const parts = labelParts(label);
  return parts.name || String(target ?? "").trim() || "Unnamed player";
}

export function playerSecondaryText(label: unknown, meta: unknown, target: unknown): string {
  const kind = playerTargetKind(target);
  const parts = labelParts(label);
  if (kind === "satellite") {
    const room = parts.detail.split("•", 1)[0].trim();
    const usefulRoom = room && !/^(?:native|voice_core):/i.test(room) && room !== parts.name ? room : "";
    return [usefulRoom, parts.status].filter(Boolean).join(" · ");
  }
  if (kind === "stereo") return parts.status;
  if (kind === "airplay") return parts.status;
  return String(meta ?? "").trim();
}

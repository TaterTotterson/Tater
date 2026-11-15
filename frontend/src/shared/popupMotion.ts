export const popupEffects = [
  { id: "scale", label: "Soft zoom", description: "Gently grow into view and shrink away.", hint: "Default" },
  { id: "fade", label: "Fade", description: "A quiet dissolve, with no movement.", hint: "Subtle" },
  { id: "slide", label: "Slide", description: "Glide up into view and down to close.", hint: "Smooth" },
  { id: "tilt", label: "Tilt", description: "Tip into view with a little 3D depth.", hint: "Playful" },
  { id: "fold", label: "Fold", description: "Unfold from the top, then flatten away.", hint: "Compiz inspired" },
  { id: "disabled", label: "Off", description: "Open and close instantly, without animation.", hint: "No motion" },
] as const;

export type PopupEffect = typeof popupEffects[number]["id"];
export type PopupPhase = "enter" | "leave";
export type PopupSurface = "dialog" | "toast";

// Older preferences and API clients continue to select a supported motion.
const legacyEffects: Record<string, PopupEffect> = { flame: "scale", dust: "fade", glitch: "tilt", portal: "fold", melt: "slide" };
export function normalizePopupEffect(value: unknown): PopupEffect {
  const token = String(value || "").trim().toLowerCase();
  return popupEffects.some(effect => effect.id === token) ? token as PopupEffect : Object.prototype.hasOwnProperty.call(legacyEffects, token) ? legacyEffects[token] : "scale";
}

export function prefersReducedMotion(): boolean {
  return window.matchMedia?.("(prefers-reduced-motion: reduce)").matches === true;
}

// Only transforms and opacity are animated: no full-screen blur or particle layers.
export function popupMotionFrames(effect: PopupEffect, phase: PopupPhase, surface: PopupSurface): Keyframe[] {
  const toast = surface === "toast";
  const rest = { opacity: 1, transform: "none" };
  if (effect === "tilt") {
    const direction = phase === "enter" ? -1 : 1;
    const tipped = `perspective(900px) rotateX(${direction * 32}deg) rotateY(${direction * 10}deg) translateY(${toast ? 12 : 18}px)`;
    const flat = "perspective(900px) rotateX(0deg) rotateY(0deg) translateY(0px)";
    // Reveal the tipped card before straightening it; keep it visible while tipping away.
    return phase === "enter"
      ? [{ opacity: 0, transform: tipped }, { opacity: 1, transform: tipped, offset: 0.12 }, { opacity: 1, transform: flat }]
      : [{ opacity: 1, transform: flat }, { opacity: 1, transform: tipped, offset: 0.85 }, { opacity: 0, transform: tipped }];
  }
  let hidden: Keyframe;
  switch (effect) {
    case "fade": hidden = { opacity: 0, transform: "none" }; break;
    case "slide": hidden = { opacity: 0, transform: `translateY(${toast ? 20 : 40}px)` }; break;
    case "fold": hidden = { opacity: 0, transform: "scaleY(0.04) scaleX(0.94)" }; break;
    default: hidden = { opacity: 0, transform: `scale(${toast ? 0.88 : 0.9})` };
  }
  return phase === "enter" ? [hidden, rest] : [rest, hidden];
}

const durations: Record<PopupEffect, [number, number]> = {
  scale: [240, 180], fade: [320, 260], slide: [280, 220], tilt: [380, 300], fold: [320, 260], disabled: [0, 0],
};
type RunningMotion = { cancel: () => void };
const running = new WeakMap<HTMLElement, RunningMotion>();
export function cancelPopupMotion(element: Element) {
  running.get(element as HTMLElement)?.cancel();
}

export function animatePopup(
  element: Element, phase: PopupPhase, done: () => void,
  override?: unknown, surface: PopupSurface = "dialog",
) {
  const root = element as HTMLElement;
  cancelPopupMotion(root);
  // Capture the selection on entry so an unsaved preview also closes with its own effect.
  const effect = normalizePopupEffect(phase === "enter" ? override ?? document.body.dataset.popupEffect : root.dataset.popupMotion);
  root.dataset.popupMotion = effect;
  root.dataset.popupPhase = phase;
  root.inert = phase === "leave";
  const media = window.matchMedia?.("(prefers-reduced-motion: reduce)");
  if (effect === "disabled" || media?.matches || typeof root.animate !== "function") { done(); return; }

  const panel = surface === "toast" ? root : root.querySelector<HTMLElement>(":scope > [role='dialog'], :scope > .tv-modal, :scope > .tm-modal") || root.firstElementChild as HTMLElement | null;
  if (!panel) { done(); return; }
  const duration = durations[effect][phase === "enter" ? 0 : 1] * (surface === "toast" ? 0.85 : 1);
  const animations: Animation[] = [];
  let settled = false;
  const cleanup = () => {
    media?.removeEventListener("change", onPreferenceChange);
    animations.forEach(animation => animation.cancel());
    running.delete(root);
  };
  const finish = () => {
    if (settled) return;
    settled = true;
    cleanup();
    done();
  };
  const onPreferenceChange = () => { if (media?.matches) finish(); };
  running.set(root, { cancel() { settled = true; cleanup(); } });
  media?.addEventListener("change", onPreferenceChange);
  try {
    const easing = effect === "fade" ? "linear" : effect === "tilt" ? "cubic-bezier(.25,.46,.45,.94)" : phase === "enter" ? "cubic-bezier(.16,1,.3,1)" : "cubic-bezier(.4,0,1,1)";
    const options: KeyframeAnimationOptions = { duration, easing, fill: "both" };
    // Keyframe-local origins do not leave inline transforms on panels after completion.
    const frames = popupMotionFrames(effect, phase, surface).map(frame => ({ ...frame, transformOrigin: effect === "fold" ? "50% 0%" : "50% 50%" }));
    animations.push(panel.animate(frames, options));
    if (panel !== root) {
      // Parent opacity also dims the card. Do not hide most of Tilt's rotation behind its backdrop fade.
      const backdropFrames = effect === "tilt"
        ? phase === "enter" ? [{ opacity: 0 }, { opacity: 1, offset: 0.2 }, { opacity: 1 }] : [{ opacity: 1 }, { opacity: 1, offset: 0.85 }, { opacity: 0 }]
        : phase === "enter" ? [{ opacity: 0 }, { opacity: 1 }] : [{ opacity: 1 }, { opacity: 0 }];
      animations.push(root.animate(backdropFrames, { ...options, easing: "linear" }));
    }
    // Vue removes the popup only after BOTH the panel and backdrop have finished.
    void Promise.allSettled(animations.map(animation => animation.finished)).then(finish);
  } catch { finish(); }
}
